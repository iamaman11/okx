#!/usr/bin/env python3
"""Strict *non-installable* Host-only Actions artifact byte verifier.

The future publisher must independently verify a first-attempt Ready PR,
the accepted-main tree and the GitHub artifact digest. This verifier only
examines local bytes. No existing Native ReleaseSet or CONTROL installer
accepts this schema; never reinterpret it as a four-EXE release.
"""

import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import stat
import struct
import sys
import zipfile


FILES = ("Cargo.lock", "manifest.json", "okx-host-control.exe", "okx-host-launcher.exe")
EXECUTABLES = ("okx-host-control.exe", "okx-host-launcher.exe")
BOUNDS = {
    "Cargo.lock": 4 * 1024 * 1024,
    "manifest.json": 64 * 1024,
    "okx-host-control.exe": 128 * 1024 * 1024,
    "okx-host-launcher.exe": 128 * 1024 * 1024,
}
MAX_ARCHIVE = 300 * 1024 * 1024
SHA40 = re.compile(r"[0-9a-f]{40}\Z")
SHA64 = re.compile(r"[0-9a-f]{64}\Z")
SCHEMA = "okx.windows.host-component/v1"
POLICY = "okx.github.actions.host-ready/v1"
RUST_VERSION = "rustc 1.100.0 (0ef1bc386 2026-11-09)"


def fail(code):
    raise ValueError(code)


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def pairs_no_duplicates(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            fail("HOST_MANIFEST_DUPLICATE_FIELD")
        obj[key] = value
    return obj


def read_manifest(raw):
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs_no_duplicates,
                          parse_constant=lambda _: fail("HOST_MANIFEST_NONFINITE"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("HOST_MANIFEST_INVALID_JSON") from exc


def verified_payloads(contents, head, tree):
    if not SHA40.fullmatch(head) or not SHA40.fullmatch(tree):
        fail("HOST_SOURCE_IDENTITY_INVALID")
    if set(contents) != set(FILES):
        fail("HOST_EXACT_FOUR_FILES_REQUIRED")
    for name, payload in contents.items():
        if type(payload) is not bytes or not 0 < len(payload) <= BOUNDS[name]:
            fail("HOST_ENTRY_UNSAFE_OR_OVERSIZED")

    manifest = read_manifest(contents["manifest.json"])
    if type(manifest) is not dict or manifest.get("schema") != SCHEMA:
        fail("HOST_MANIFEST_SCHEMA_UNSUPPORTED")
    required = {
        "schema", "ci_status_policy", "repository_id", "source_head_sha", "source_tree",
        "rust_version", "cargo_lock_sha256", "artifact_scope", "release_is_complete",
        "installation_allowed", "components",
    }
    if set(manifest) != required:
        fail("HOST_MANIFEST_UNKNOWN_OR_MISSING_FIELDS")
    if (manifest["ci_status_policy"] != POLICY or
            type(manifest["repository_id"]) is not int or
            manifest["repository_id"] != 1388071566 or
            manifest["source_head_sha"] != head or manifest["source_tree"] != tree or
            type(manifest["rust_version"]) is not str or
            manifest["rust_version"] != RUST_VERSION or
            type(manifest["cargo_lock_sha256"]) is not str or
            not SHA64.fullmatch(manifest["cargo_lock_sha256"]) or
            manifest["cargo_lock_sha256"] != sha256(contents["Cargo.lock"])):
        fail("HOST_MANIFEST_PROVENANCE_INVALID")
    if (manifest["artifact_scope"] != "host_only" or
            manifest["release_is_complete"] is not False or
            manifest["installation_allowed"] is not False):
        fail("HOST_MANIFEST_FALSLY_INSTALLABLE")

    components = manifest["components"]
    if type(components) is not dict or set(components) != set(EXECUTABLES):
        fail("HOST_COMPONENT_SET_INVALID")
    for name in EXECUTABLES:
        declared = components[name]
        binary = contents[name]
        if (type(declared) is not dict or set(declared) != {"sha256", "size"} or
                type(declared["sha256"]) is not str or
                not SHA64.fullmatch(declared["sha256"]) or
                type(declared["size"]) is not int or
                not 1024 <= declared["size"] == len(binary) <= BOUNDS[name] or
                binary[:2] != b"MZ" or declared["sha256"] != sha256(binary)):
            fail("HOST_COMPONENT_BYTES_INVALID")
    return {
        "schema": "okx.windows.host-component-validation/v1",
        "component_schema": SCHEMA,
        "ci_status_policy": POLICY,
        "repository_id": 1388071566,
        "source_head_sha": head,
        "source_tree": tree,
        "cargo_lock_sha256": manifest["cargo_lock_sha256"],
        "components": components,
        "release_is_complete": False,
        "installation_allowed": False,
        "engine_or_research_bytes_in_artifact": False,
    }


def verify_directory(directory, head, tree):
    directory = Path(directory)
    if not directory.is_dir() or directory.is_symlink():
        fail("HOST_COMPONENT_DIRECTORY_INVALID")
    items = list(directory.iterdir())
    if {p.name for p in items} != set(FILES) or len(items) != len(FILES):
        fail("HOST_EXACT_FOUR_FILES_REQUIRED")
    contents = {}
    for path in items:
        if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= BOUNDS[path.name]:
            fail("HOST_ENTRY_UNSAFE_OR_OVERSIZED")
        contents[path.name] = path.read_bytes()
    return verified_payloads(contents, head, tree)


def verify_canonical_zip_layout(raw, entries):
    """Reject data hidden in ZIP prefixes, comments, gaps, aliases or overlays.

    ZIP is a container, not just a mapping of normalized names to extracted
    bytes. Require the complete archive to be exactly four contiguous local
    records, one central directory, and a zero-comment EOCD at the final byte.
    ZIP64, extra fields and noncanonical encodings are intentionally not part
    of this v1 artifact contract.
    """
    if (len(raw) < 22 or raw[:4] != b"PK\x03\x04" or
            raw[-22:-18] != b"PK\x05\x06"):
        fail("HOST_ARCHIVE_NONCANONICAL_LAYOUT")
    eocd = struct.unpack_from("<IHHHHIIH", raw, len(raw) - 22)
    _, disk, cd_disk, disk_count, total, cd_size, cd_offset, comment = eocd
    if (disk or cd_disk or disk_count != len(FILES) or total != len(FILES) or
            comment or cd_offset + cd_size != len(raw) - 22):
        fail("HOST_ARCHIVE_NONCANONICAL_LAYOUT")

    cursor = 0
    for entry in sorted(entries, key=lambda item: item.header_offset):
        name = entry.filename.encode("ascii")
        if (entry.orig_filename != entry.filename or
                entry.header_offset != cursor or cursor + 30 > cd_offset):
            fail("HOST_ARCHIVE_NONCANONICAL_LAYOUT")
        local = struct.unpack_from("<IHHHHHIIIHH", raw, cursor)
        (signature, _version, flags, method, _time, _date, crc, compressed,
         expanded, name_length, extra_length) = local
        name_start = cursor + 30
        data_start = name_start + name_length + extra_length
        if (signature != 0x04034B50 or extra_length or
                raw[name_start:name_start + name_length] != name or
                flags != entry.flag_bits or method != entry.compress_type or
                flags & ~0x0808 or data_start + entry.compress_size > cd_offset):
            fail("HOST_ARCHIVE_NONCANONICAL_LAYOUT")
        if flags & 0x0008:
            # The streaming writer may put CRC and sizes in a descriptor.
            if (crc not in (0, entry.CRC) or
                    compressed not in (0, entry.compress_size) or
                    expanded not in (0, entry.file_size)):
                fail("HOST_ARCHIVE_NONCANONICAL_LAYOUT")
        elif (crc != entry.CRC or compressed != entry.compress_size or
              expanded != entry.file_size):
            fail("HOST_ARCHIVE_NONCANONICAL_LAYOUT")
        cursor = data_start + entry.compress_size
        if flags & 0x0008:
            if raw[cursor:cursor + 4] == b"PK\x07\x08":
                cursor += 4
            if cursor + 12 > cd_offset or struct.unpack_from("<III", raw, cursor) != (
                    entry.CRC, entry.compress_size, entry.file_size):
                fail("HOST_ARCHIVE_NONCANONICAL_LAYOUT")
            cursor += 12
    if cursor != cd_offset:
        fail("HOST_ARCHIVE_NONCANONICAL_LAYOUT")

    for entry in entries:
        if cursor + 46 > len(raw) - 22 or raw[cursor:cursor + 4] != b"PK\x01\x02":
            fail("HOST_ARCHIVE_NONCANONICAL_LAYOUT")
        name_len, extra_len, comment_len = struct.unpack_from("<HHH", raw, cursor + 28)
        origin_offset = struct.unpack_from("<I", raw, cursor + 42)[0]
        if (extra_len or comment_len or origin_offset != entry.header_offset or
                raw[cursor + 46:cursor + 46 + name_len] != entry.filename.encode("ascii")):
            fail("HOST_ARCHIVE_NONCANONICAL_LAYOUT")
        cursor += 46 + name_len
    if cursor != len(raw) - 22:
        fail("HOST_ARCHIVE_NONCANONICAL_LAYOUT")


def verify_bytes(archive_bytes, digest, head, tree):
    if not SHA64.fullmatch(digest) or sha256(archive_bytes) != digest:
        fail("HOST_ARCHIVE_DIGEST_MISMATCH")
    if not 0 < len(archive_bytes) <= MAX_ARCHIVE:
        fail("HOST_ARCHIVE_OVERSIZED")
    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes), "r") as archive:
            entries = archive.infolist()
            if sorted(entry.filename for entry in entries) != list(FILES):
                fail("HOST_ARCHIVE_EXACT_FOUR_ENTRIES_REQUIRED")
            verify_canonical_zip_layout(archive_bytes, entries)
            for entry in entries:
                mode = stat.S_IFMT(entry.external_attr >> 16)
                if (entry.is_dir() or entry.external_attr & 0x10 or
                        mode not in (0, stat.S_IFREG) or
                        entry.flag_bits & 1 or
                        entry.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED) or
                        not 0 < entry.file_size <= BOUNDS[entry.filename] or
                        entry.compress_size > MAX_ARCHIVE):
                    fail("HOST_ARCHIVE_ENTRY_UNSAFE_OR_OVERSIZED")
            contents = {entry.filename: archive.read(entry) for entry in entries}
    except (zipfile.BadZipFile, RuntimeError) as exc:
        raise ValueError("HOST_ARCHIVE_INVALID") from exc
    result = verified_payloads(contents, head, tree)
    result["archive_sha256"] = digest
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", help="Host-only directory or its immutable GitHub Actions ZIP")
    parser.add_argument("expected_head")
    parser.add_argument("expected_tree")
    parser.add_argument("--archive-sha256", help="required SHA-256 for a ZIP")
    args = parser.parse_args()
    path = Path(args.source)
    if path.is_dir() and not args.archive_sha256:
        result = verify_directory(path, args.expected_head, args.expected_tree)
    else:
        if not path.is_file() or path.stat().st_size > MAX_ARCHIVE or not args.archive_sha256:
            fail("HOST_ARCHIVE_MISSING_DIGEST_OR_OVERSIZED")
        result = verify_bytes(path.read_bytes(), args.archive_sha256,
                              args.expected_head, args.expected_tree)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError) as error:
        print(f"HOST_COMPONENT_PROOF_REJECTED:{error}", file=sys.stderr)
        sys.exit(1)
