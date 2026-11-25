#!/usr/bin/env python3
"""Negative ZIP/readback controls for Research-only durable publication."""
import hashlib
import io
import json
import stat
import unittest
import warnings
import zipfile

from verify_research_component_release import verify_bytes

HEAD, TREE = "a" * 40, "b" * 40
WORKER = b"MZ" + b"\0" * 2048
LOCK = b"version = 4\n"
digest = lambda raw: hashlib.sha256(raw).hexdigest()


def manifest():
    return {
        "schema": "okx.windows.research-component/v1",
        "repository_id": 1388071566,
        "source_head_sha": HEAD, "source_tree": TREE,
        "rust_version": "rustc 1.100.0 (fakehash 2026-09-01)",
        "cargo_lock_sha256": digest(LOCK),
        "worker": {"sha256": digest(WORKER), "size": len(WORKER),
                   "protocol_version": 1, "storage_version": 1},
    }


def archive(entries):
    dest = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(dest, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
            for name, raw in entries:
                zip_file.writestr(name, raw)
    return dest.getvalue()


def entries(value=None, raw=None):
    return [("manifest.json", raw if raw is not None else json.dumps(value or manifest()).encode()),
            ("Cargo.lock", LOCK), ("okx-research-service.exe", WORKER)]


def verify(blob):
    return verify_bytes(blob, digest(blob), HEAD, TREE)


class ResearchComponentReleaseTests(unittest.TestCase):
    def test_exact_three_files(self):
        result = verify(archive(entries()))
        self.assertEqual(result["worker_sha256"], digest(WORKER))
        self.assertTrue(result["no_native_engine_or_control_bytes"])

    def test_named_v2_v3_policy_is_typed_and_not_appended_to_v1(self):
        for version in (2, 3):
            named = manifest()
            named["schema"] = f"okx.windows.research-component/v{version}"
            named["ci_status_policy"] = f"okx.github.actions.research-ready/v{version}"
            self.assertEqual(verify(archive(entries(named)))["ci_status_policy"],
                             named["ci_status_policy"])
            for value in (None, "okx.github.actions.research-ready/v1",
                          f"okx.github.actions.research-ready/v{5 - version}"):
                wrong = named.copy()
                wrong["ci_status_policy"] = value
                with self.assertRaisesRegex(ValueError, "READY_POLICY_MISMATCH"):
                    verify(archive(entries(wrong)))
            missing = named.copy()
            missing.pop("ci_status_policy")
            with self.assertRaisesRegex(ValueError, "UNKNOWN_OR_MISSING_FIELDS"):
                verify(archive(entries(missing)))
            swapped = named.copy()
            swapped["schema"] = "okx.windows.research-component/v1"
            with self.assertRaisesRegex(ValueError, "UNKNOWN_OR_MISSING_FIELDS"):
                verify(archive(entries(swapped)))
            unknown = named.copy()
            unknown["engine_component"] = "forbidden"
            with self.assertRaisesRegex(ValueError, "UNKNOWN_OR_MISSING_FIELDS"):
                verify(archive(entries(unknown)))
            raw = json.dumps(named).removesuffix("}") + ',"ci_status_policy":"forged"}'
            with self.assertRaisesRegex(ValueError, "DUPLICATE_JSON_FIELD"):
                verify(archive(entries(raw=raw.encode())))
        legacy = manifest()
        legacy["ci_status_policy"] = "okx.github.actions.research-ready/v3"
        with self.assertRaisesRegex(ValueError, "UNKNOWN_OR_MISSING_FIELDS"):
            verify(archive(entries(legacy)))

    def test_zip_digest_and_source_are_bound(self):
        blob = archive(entries())
        with self.assertRaisesRegex(ValueError, "ZIP_DIGEST_MISMATCH"):
            verify_bytes(blob, "0" * 64, HEAD, TREE)
        with self.assertRaisesRegex(ValueError, "SOURCE_OR_LOCK"):
            verify_bytes(blob, digest(blob), HEAD, "0" * 40)

    def test_extra_duplicate_missing_and_traversing_entries(self):
        for changed in (
            entries()[:-1],
            entries() + [("okx-agent.exe", b"not allowed")],
            entries() + [("okx-research-service.exe", WORKER)],
            entries() + [("../okx-research-service.exe", WORKER)],
            [("bin/manifest.json", entries()[0][1]), *entries()[1:]],
        ):
            with self.subTest(changed=[entry[0] for entry in changed]):
                with self.assertRaisesRegex(ValueError, "EXACT_THREE_ENTRIES"):
                    verify(archive(changed))

    def test_manifest_unknown_duplicate_and_foreign_fields(self):
        modified = manifest(); modified["native_base_sha"] = "0" * 40
        with self.assertRaisesRegex(ValueError, "UNKNOWN_OR_MISSING_FIELDS"):
            verify(archive(entries(modified)))
        raw = json.dumps(manifest()).removesuffix("}") + ',"schema":"different"}'
        with self.assertRaisesRegex(ValueError, "DUPLICATE_JSON_FIELD"):
            verify(archive(entries(raw=raw.encode())))

    def test_manifest_bytes_lock_versions_and_identity(self):
        for mutate, error in (
            (lambda x: x.update(repository_id=1), "SOURCE_OR_LOCK"),
            (lambda x: x.update(source_head_sha="0" * 40), "SOURCE_OR_LOCK"),
            (lambda x: x.update(cargo_lock_sha256="0" * 64), "SOURCE_OR_LOCK"),
            (lambda x: x["worker"].update(sha256="0" * 64), "WORKER_BYTES_OR_VERSION"),
            (lambda x: x["worker"].update(size=len(WORKER) - 1), "WORKER_BYTES_OR_VERSION"),
            (lambda x: x["worker"].update(protocol_version=2), "WORKER_BYTES_OR_VERSION"),
            (lambda x: x["worker"].update(storage_version=2), "WORKER_BYTES_OR_VERSION"),
            (lambda x: x["worker"].update(source_tree="0" * 40), "UNKNOWN_OR_MISSING_FIELDS"),
        ):
            changed = manifest(); mutate(changed)
            with self.subTest(changed=changed):
                with self.assertRaisesRegex(ValueError, error):
                    verify(archive(entries(changed)))
        with self.assertRaisesRegex(ValueError, "WORKER_BYTES_OR_VERSION"):
            changed = entries()
            changed[-1] = ("okx-research-service.exe", b"not a Windows EXE")
            verify(archive(changed))

    def test_symlink_entry_not_a_regular_executable(self):
        changed = entries()
        dest = io.BytesIO()
        with zipfile.ZipFile(dest, "w") as zipped:
            for name, raw in changed:
                info = zipfile.ZipInfo(name)
                if name == "okx-research-service.exe":
                    info.create_system = 3
                    info.external_attr = (stat.S_IFLNK | 0o777) << 16
                zipped.writestr(info, raw)
        with self.assertRaisesRegex(ValueError, "ENTRY_UNSAFE"):
            verify(dest.getvalue())


if __name__ == "__main__":
    unittest.main()
