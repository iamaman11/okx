#!/usr/bin/env python3
"""Positive and mutation-negative Host-only artifact contract tests."""

import copy
import hashlib
import io
import json
from pathlib import Path
import stat
import struct
import tempfile
import zipfile

from verify_host_component_release import (
    EXECUTABLES, FILES, POLICY, RUST_VERSION, SCHEMA, sha256, verify_bytes,
    verify_directory,
)


HEAD = "a" * 40
TREE = "b" * 40
BINARY = b"MZ" + b"\0" * 2046
LOCK = b"# pinned Cargo.lock fixture\n"


def bundle():
    return {
        "Cargo.lock": LOCK,
        "okx-host-control.exe": BINARY + b"control",
        "okx-host-launcher.exe": BINARY + b"launcher",
    }


def manifest(files):
    return {
        "schema": SCHEMA,
        "ci_status_policy": POLICY,
        "repository_id": 1388071566,
        "source_head_sha": HEAD,
        "source_tree": TREE,
        "rust_version": RUST_VERSION,
        "cargo_lock_sha256": sha256(files["Cargo.lock"]),
        "artifact_scope": "host_only",
        "release_is_complete": False,
        "installation_allowed": False,
        "components": {
            exe: {"sha256": sha256(files[exe]), "size": len(files[exe])}
            for exe in EXECUTABLES
        },
    }


def archive(files, *, symlink=None, dos_directory=None):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, payload in files.items():
            info = zipfile.ZipInfo(name)
            info.compress_type = zipfile.ZIP_DEFLATED
            if name == symlink:
                info.create_system = 3
                info.external_attr = (stat.S_IFLNK | 0o777) << 16
            else:
                info.create_system = 3
                info.external_attr = (stat.S_IFREG | 0o644) << 16
            if name == dos_directory:
                info.external_attr |= 0x10
            z.writestr(info, payload)
    return buf.getvalue()


def inject_hidden_deflate_suffix(raw, hidden):
    """Hide bytes after DEFLATE EOS without making a ZIP gap or overlay."""
    result = bytearray(raw)
    with zipfile.ZipFile(io.BytesIO(result), "r") as z:
        entries = z.infolist()
    target = entries[-1]
    assert target.filename == "manifest.json"
    assert target.compress_type == zipfile.ZIP_DEFLATED
    cd_offset = struct.unpack_from("<I", result, len(result) - 6)[0]
    result[cd_offset:cd_offset] = hidden
    struct.pack_into("<I", result, target.header_offset + 18,
                     target.compress_size + len(hidden))
    cursor = cd_offset + len(hidden)
    for entry in entries:
        assert result[cursor:cursor + 4] == b"PK\x01\x02"
        if entry.filename == target.filename:
            struct.pack_into("<I", result, cursor + 20,
                             target.compress_size + len(hidden))
        name_len, extra_len, comment_len = struct.unpack_from("<HHH", result, cursor + 28)
        cursor += 46 + name_len + extra_len + comment_len
    struct.pack_into("<I", result, len(result) - 6, cd_offset + len(hidden))
    return bytes(result)


def encoded(m, files=None):
    contents = bundle() if files is None else copy.deepcopy(files)
    contents["manifest.json"] = json.dumps(m, separators=(",", ":")).encode()
    return contents


def rejects(files, error, *, digest=None, head=HEAD, tree=TREE,
            symlink=None, dos_directory=None):
    zip_bytes = archive(files, symlink=symlink, dos_directory=dos_directory)
    try:
        verify_bytes(zip_bytes, digest or sha256(zip_bytes), head, tree)
    except ValueError as exc:
        assert str(exc) == error, (str(exc), error)
    else:
        raise AssertionError(f"Host artifact wrongly accepted: {error}")


def run():
    files = bundle()
    good_manifest = manifest(files)
    contents = encoded(good_manifest, files)
    zip_bytes = archive(contents)
    verified = verify_bytes(zip_bytes, sha256(zip_bytes), HEAD, TREE)
    assert verified["installation_allowed"] is False
    assert verified["release_is_complete"] is False
    assert verified["engine_or_research_bytes_in_artifact"] is False
    assert set(verified["components"]) == set(EXECUTABLES)

    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        for name, payload in contents.items():
            (root / name).write_bytes(payload)
        assert verify_directory(root, HEAD, TREE)["installation_allowed"] is False
        (root / "okx-agent.exe").write_bytes(BINARY)
        try:
            verify_directory(root, HEAD, TREE)
        except ValueError as exc:
            assert str(exc) == "HOST_EXACT_FOUR_FILES_REQUIRED"
        else:
            raise AssertionError("Engine byte accepted in Host-only component directory")

    for mutated, code in [
        ({**good_manifest, "release_is_complete": True}, "HOST_MANIFEST_FALSLY_INSTALLABLE"),
        ({**good_manifest, "installation_allowed": True}, "HOST_MANIFEST_FALSLY_INSTALLABLE"),
        ({**good_manifest, "artifact_scope": "native_full"}, "HOST_MANIFEST_FALSLY_INSTALLABLE"),
        ({**good_manifest, "schema": "okx.windows.native/v1"}, "HOST_MANIFEST_SCHEMA_UNSUPPORTED"),
        ({**good_manifest, "ci_status_policy": "okx.github.actions.host-ready/v2"}, "HOST_MANIFEST_PROVENANCE_INVALID"),
        ({**good_manifest, "repository_id": 99}, "HOST_MANIFEST_PROVENANCE_INVALID"),
        ({**good_manifest, "source_head_sha": TREE}, "HOST_MANIFEST_PROVENANCE_INVALID"),
        ({**good_manifest, "source_tree": HEAD}, "HOST_MANIFEST_PROVENANCE_INVALID"),
        ({**good_manifest, "rust_version": "rustc 1.99.0"}, "HOST_MANIFEST_PROVENANCE_INVALID"),
        ({**good_manifest, "rust_version": RUST_VERSION + " forged-extra"}, "HOST_MANIFEST_PROVENANCE_INVALID"),
        ({**good_manifest, "rust_version": "rustc 1.100.0 (fffffffff 2026-11-09)"}, "HOST_MANIFEST_PROVENANCE_INVALID"),
        ({**good_manifest, "rust_version": "rustc 1.100.0"}, "HOST_MANIFEST_PROVENANCE_INVALID"),
        ({**good_manifest, "cargo_lock_sha256": "0" * 64}, "HOST_MANIFEST_PROVENANCE_INVALID"),
        ({**good_manifest, "owner_claim": "engine"}, "HOST_MANIFEST_UNKNOWN_OR_MISSING_FIELDS"),
        ({k: v for k, v in good_manifest.items() if k != "components"}, "HOST_MANIFEST_UNKNOWN_OR_MISSING_FIELDS"),
        ({**good_manifest, "components": {}}, "HOST_COMPONENT_SET_INVALID"),
    ]:
        rejects(encoded(mutated), code)

    for name in EXECUTABLES:
        wrong_hash = copy.deepcopy(good_manifest)
        wrong_hash["components"][name]["sha256"] = "f" * 64
        rejects(encoded(wrong_hash), "HOST_COMPONENT_BYTES_INVALID")
        wrong_size = copy.deepcopy(good_manifest)
        wrong_size["components"][name]["size"] = len(files[name]) + 1
        rejects(encoded(wrong_size), "HOST_COMPONENT_BYTES_INVALID")
        wrong_fields = copy.deepcopy(good_manifest)
        wrong_fields["components"][name]["source_tree"] = TREE
        rejects(encoded(wrong_fields), "HOST_COMPONENT_BYTES_INVALID")

    rejects({k: v for k, v in contents.items() if k != "okx-host-launcher.exe"},
            "HOST_ARCHIVE_EXACT_FOUR_ENTRIES_REQUIRED")
    rejects({**contents, "okx-agent.exe": BINARY}, "HOST_ARCHIVE_EXACT_FOUR_ENTRIES_REQUIRED")
    rejects({**contents, "../escaped": b"bad"}, "HOST_ARCHIVE_EXACT_FOUR_ENTRIES_REQUIRED")
    rejects(contents, "HOST_ARCHIVE_ENTRY_UNSAFE_OR_OVERSIZED", symlink="okx-host-control.exe")
    rejects(contents, "HOST_ARCHIVE_ENTRY_UNSAFE_OR_OVERSIZED", dos_directory="Cargo.lock")
    rejects({**contents, "okx-host-launcher.exe": b"MZshort"}, "HOST_COMPONENT_BYTES_INVALID")
    rejects(contents, "HOST_SOURCE_IDENTITY_INVALID", head="bad")
    rejects(contents, "HOST_SOURCE_IDENTITY_INVALID", tree="bad")

    try:
        verify_bytes(zip_bytes, "0" * 64, HEAD, TREE)
    except ValueError as exc:
        assert str(exc) == "HOST_ARCHIVE_DIGEST_MISMATCH"
    else:
        raise AssertionError("Wrong Actions archive digest accepted")

    # A ZIP parser's extracted mapping is not a proof of its raw container.
    # SFX prefixes, trailing bytes, ZIP comments and NUL-normalized aliases
    # can conceal undeclared executable content unless the physical ZIP is
    # parsed from byte zero through the exact final EOCD.
    for disguised in [
        b"MZ_UNDECLARED_ENGINE" + zip_bytes,
        zip_bytes + b"MZ_UNDECLARED_ENGINE",
    ]:
        try:
            verify_bytes(disguised, sha256(disguised), HEAD, TREE)
        except ValueError as exc:
            assert str(exc) == "HOST_ARCHIVE_NONCANONICAL_LAYOUT", str(exc)
        else:
            raise AssertionError("ZIP prefix/trailer disguised undeclared Engine bytes")

    hidden = inject_hidden_deflate_suffix(zip_bytes, b"MZ_UNDECLARED_ENGINE")
    # The standard ZIP reader ignores bytes after DEFLATE EOS even when those
    # bytes are counted in both local and central compressed sizes.
    with zipfile.ZipFile(io.BytesIO(hidden), "r") as z:
        assert z.read("manifest.json") == contents["manifest.json"]
    try:
        verify_bytes(hidden, sha256(hidden), HEAD, TREE)
    except ValueError as exc:
        assert str(exc) == "HOST_ARCHIVE_NONCANONICAL_LAYOUT", str(exc)
    else:
        raise AssertionError("Undeclared payload inside a compressed member was accepted")

    comment_zip = io.BytesIO()
    with zipfile.ZipFile(comment_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for name, payload in contents.items():
            z.writestr(name, payload)
        z.comment = b"MZ_UNDECLARED_ENGINE"
    disguised = comment_zip.getvalue()
    try:
        verify_bytes(disguised, sha256(disguised), HEAD, TREE)
    except ValueError as exc:
        assert str(exc) == "HOST_ARCHIVE_NONCANONICAL_LAYOUT", str(exc)
    else:
        raise AssertionError("ZIP comment disguised undeclared Engine bytes")

    nul_contents = {**contents}
    nul_contents["okx-host-control.exeX"] = nul_contents.pop("okx-host-control.exe")
    disguised = archive(nul_contents).replace(b"okx-host-control.exeX",
                                              b"okx-host-control.exe\x00")
    # Python's ZipInfo strips the trailing NUL while keeping orig_filename:
    # the extracted member mapping looks valid but the raw name is not.
    try:
        verify_bytes(disguised, sha256(disguised), HEAD, TREE)
    except ValueError as exc:
        assert str(exc) == "HOST_ARCHIVE_NONCANONICAL_LAYOUT", str(exc)
    else:
        raise AssertionError("NUL-normalized ZIP name silently accepted")

    duplicate = b'{"schema":"' + SCHEMA.encode() + b'","schema":"' + SCHEMA.encode() + b'"}'
    rejects({**contents, "manifest.json": duplicate}, "HOST_MANIFEST_DUPLICATE_FIELD")
    print("HOST_ONLY_COMPONENT_BYTE_AND_NONINSTALLABLE_NEGATIVE_CONTROLS=PASS")


if __name__ == "__main__":
    run()
