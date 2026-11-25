#!/usr/bin/env python3
"""Strict offline byte validation for the immutable Research-only Actions ZIP.

The caller must independently authenticate the canonical GitHub PR run,
artifact digest and merged source tree. This verifier never downloads bytes,
calls GitHub, installs Research, or accepts native Engine/CONTROL images.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import stat
import sys
import zipfile


FILES = ("Cargo.lock", "manifest.json", "okx-research-service.exe")
BOUNDS = {"Cargo.lock": 4 * 1024 * 1024,
          "manifest.json": 64 * 1024,
          "okx-research-service.exe": 128 * 1024 * 1024}
SHA40 = re.compile(r"[0-9a-f]{40}\Z")
SHA64 = re.compile(r"[0-9a-f]{64}\Z")


def fail(code):
    raise ValueError(code)


def pairs_no_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            fail("RESEARCH_MANIFEST_DUPLICATE_JSON_FIELD")
        result[key] = value
    return result


def parse_manifest(raw):
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs_no_duplicates,
                          parse_constant=lambda _: fail("RESEARCH_MANIFEST_NONFINITE_NUMBER"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("RESEARCH_MANIFEST_INVALID_JSON") from exc


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def verify_bytes(archive_bytes, expected_digest, head, tree):
    import io

    if not SHA64.fullmatch(expected_digest) or sha256(archive_bytes) != expected_digest:
        fail("RESEARCH_ARTIFACT_ZIP_DIGEST_MISMATCH")
    if not SHA40.fullmatch(head) or not SHA40.fullmatch(tree):
        fail("RESEARCH_SOURCE_IDENTITY_INVALID")
    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes), "r") as archive:
            entries = archive.infolist()
            if sorted(entry.filename for entry in entries) != list(FILES):
                fail("RESEARCH_ARCHIVE_EXACT_THREE_ENTRIES_REQUIRED")
            for entry in entries:
                mode = stat.S_IFMT(entry.external_attr >> 16)
                if (entry.is_dir() or mode not in (0, stat.S_IFREG) or
                        entry.flag_bits & 1 or
                        entry.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED) or
                        not 0 < entry.file_size <= BOUNDS[entry.filename] or
                        entry.compress_size > 268435456):
                    fail("RESEARCH_ARCHIVE_ENTRY_UNSAFE_OR_OVERSIZED")
            contents = {entry.filename: archive.read(entry) for entry in entries}
    except (zipfile.BadZipFile, RuntimeError) as exc:
        raise ValueError("RESEARCH_ARCHIVE_ZIP_UNVERIFIED") from exc
    manifest = parse_manifest(contents["manifest.json"])
    if type(manifest) is not dict or set(manifest) != {
        "schema", "repository_id", "source_head_sha", "source_tree",
        "rust_version", "cargo_lock_sha256", "worker"
    }:
        fail("RESEARCH_MANIFEST_UNKNOWN_OR_MISSING_FIELDS")
    worker = manifest["worker"]
    if type(worker) is not dict or set(worker) != {
        "sha256", "size", "protocol_version", "storage_version"
    }:
        fail("RESEARCH_WORKER_UNKNOWN_OR_MISSING_FIELDS")
    if (manifest["schema"] != "okx.windows.research-component/v1" or
            type(manifest["repository_id"]) is not int or
            manifest["repository_id"] != 1388071566 or
            manifest["source_head_sha"] != head or manifest["source_tree"] != tree or
            type(manifest["rust_version"]) is not str or
            not re.fullmatch(r"rustc 1\.100\.0(?:\s.{1,140})?", manifest["rust_version"]) or
            type(manifest["cargo_lock_sha256"]) is not str or
            not SHA64.fullmatch(manifest["cargo_lock_sha256"]) or
            manifest["cargo_lock_sha256"] != sha256(contents["Cargo.lock"])):
        fail("RESEARCH_MANIFEST_SOURCE_OR_LOCK_MISMATCH")
    executable = contents["okx-research-service.exe"]
    if (type(worker["sha256"]) is not str or not SHA64.fullmatch(worker["sha256"]) or
            type(worker["size"]) is not int or worker["size"] != len(executable) or
            worker["size"] < 1024 or worker["size"] > BOUNDS["okx-research-service.exe"] or
            executable[:2] != b"MZ" or worker["sha256"] != sha256(executable) or
            type(worker["protocol_version"]) is not int or worker["protocol_version"] != 1 or
            type(worker["storage_version"]) is not int or worker["storage_version"] != 1):
        fail("RESEARCH_MANIFEST_WORKER_BYTES_OR_VERSION_MISMATCH")
    return {
        "schema": "okx.windows.research-release-validation/v1",
        "repository_id": 1388071566,
        "source_head_sha": head,
        "source_tree": tree,
        "archive_sha256": expected_digest,
        "worker_sha256": worker["sha256"],
        "worker_size": worker["size"],
        "cargo_lock_sha256": manifest["cargo_lock_sha256"],
        "no_native_engine_or_control_bytes": True,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("archive")
    parser.add_argument("expected_sha256")
    parser.add_argument("expected_head")
    parser.add_argument("expected_tree")
    args = parser.parse_args()
    archive = Path(args.archive)
    if not archive.is_file() or archive.stat().st_size > 268435456:
        fail("RESEARCH_ARTIFACT_ARCHIVE_MISSING_OR_OVERSIZED")
    print(json.dumps(verify_bytes(archive.read_bytes(), args.expected_sha256,
                                  args.expected_head, args.expected_tree), sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError) as error:
        print(f"RESEARCH_RELEASE_PROOF_REJECTED:{error}", file=sys.stderr)
        sys.exit(1)
