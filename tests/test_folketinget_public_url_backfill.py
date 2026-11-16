from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from scripts.backfill_folketinget_public_urls import (
    MANIFEST_SCHEMA, RouteResolution, WranglerD1Store, apply_manifest,
    build_manifest, dry_run, rollback_manifest,
)

PUBLIC_URL = "https://www.ft.dk/samling/20251/lovforslag/l1/index.htm"
ORIGINAL = '{"number":"L 1","type_id":1,"legal_url":null}'


def publication(publication_id=1, external_id="42", *, source="folketinget", metadata=ORIGINAL):
    return {
        "publication_id": publication_id,
        "source_code": source,
        "external_id": external_id,
        "external_url": f"https://oda.ft.dk/api/Sag({external_id})",
        "source_metadata_json": metadata,
    }


class FakeStore:
    def __init__(self, rows):
        self.rows = {row["publication_id"]: dict(row) for row in rows}
        self.publication_writes = 0
        self.other_table_writes = 0

    def list_folketinget_publications(self):
        return [dict(row) for row in self.rows.values() if row["source_code"] == "folketinget"]

    def get_publication(self, publication_id):
        row = self.rows.get(publication_id)
        return dict(row) if row else None

    def apply_public_url(self, publication_id, external_id, external_url, original_metadata, public_url):
        row = self.rows[publication_id]
        if (row["source_code"] != "folketinget" or row["external_id"] != external_id
                or row["external_url"] != external_url or row["source_metadata_json"] != original_metadata):
            return False
        metadata = json.loads(original_metadata)
        if "public_url" in metadata:
            return False
        metadata["public_url"] = public_url
        row["source_metadata_json"] = json.dumps(metadata, separators=(",", ":"))
        self.publication_writes += 1
        return True

    def remove_public_url(self, publication_id, external_id, current_metadata, public_url):
        row = self.rows[publication_id]
        if row["source_code"] != "folketinget" or row["external_id"] != external_id or row["source_metadata_json"] != current_metadata:
            return False
        metadata = json.loads(current_metadata)
        if metadata.get("public_url") != public_url:
            return False
        del metadata["public_url"]
        row["source_metadata_json"] = json.dumps(metadata, separators=(",", ":"))
        self.publication_writes += 1
        return True


class BackfillTests(unittest.TestCase):
    def test_dry_run_writes_local_manifest_but_never_writes_database(self):
        store = FakeStore([publication()])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            manifest = dry_run(store, path, resolver=lambda _rows: {"42": RouteResolution(PUBLIC_URL, audit={"sag_id": 42})})
            self.assertTrue(path.is_file())
            self.assertEqual(json.loads(path.read_text()), manifest)
        self.assertEqual(store.publication_writes, 0)
        self.assertEqual(store.other_table_writes, 0)

    def test_valid_folketinget_record_enters_auditable_manifest(self):
        manifest = build_manifest([publication()], {"42": RouteResolution(PUBLIC_URL, audit={"sag_id": 42, "periode_code": "20251"})}, created_at="2026-01-01T00:00:00Z")
        self.assertEqual(manifest["schema"], MANIFEST_SCHEMA)
        self.assertEqual(manifest["summary"]["planned_updates"], 1)
        self.assertEqual(manifest["records"][0]["original_source_metadata_json"], ORIGINAL)
        self.assertEqual(manifest["records"][0]["public_url"], PUBLIC_URL)
        self.assertEqual(manifest["records"][0]["oda_audit"]["periode_code"], "20251")

    def test_unsupported_or_incomplete_routes_are_skipped_with_reason(self):
        rows = [publication(1, "42"), publication(2, "43")]
        manifest = build_manifest(rows, {
            "42": RouteResolution(None, "unsupported_sagstype"),
            "43": RouteResolution(None, "missing_invalid_periode"),
        })
        self.assertEqual(manifest["records"], [])
        self.assertEqual(manifest["skipped_by_reason"], {"missing_invalid_periode": 1, "unsupported_sagstype": 1})
        self.assertEqual(manifest["summary"]["remaining_without_public_url"], 2)

    def test_existing_public_url_is_counted_and_not_replanned(self):
        existing = json.dumps({"number": "L 1", "public_url": PUBLIC_URL})
        manifest = build_manifest([publication(metadata=existing)], {"42": RouteResolution(PUBLIC_URL)})
        self.assertEqual(manifest["summary"]["already_have_public_url"], 1)
        self.assertEqual(manifest["summary"]["candidates_checked"], 0)
        self.assertEqual(manifest["records"], [])

    def test_apply_changes_only_metadata_and_is_repeatable(self):
        store = FakeStore([publication()])
        manifest = build_manifest([publication()], {"42": RouteResolution(PUBLIC_URL)})
        first = apply_manifest(manifest, store)
        second = apply_manifest(manifest, store)
        updated = store.get_publication(1)
        self.assertEqual(first, {"applied": 1})
        self.assertEqual(second, {"already_applied": 1})
        metadata = json.loads(updated["source_metadata_json"])
        self.assertEqual(metadata["public_url"], PUBLIC_URL)
        self.assertEqual(metadata["number"], "L 1")
        self.assertEqual(updated["external_url"], publication()["external_url"])
        self.assertEqual(store.publication_writes, 1)

    def test_stale_metadata_guard_prevents_apply(self):
        store = FakeStore([publication()])
        manifest = build_manifest([publication()], {"42": RouteResolution(PUBLIC_URL)})
        store.rows[1]["source_metadata_json"] = '{"number":"changed"}'
        self.assertEqual(apply_manifest(manifest, store), {"stale_metadata": 1})
        self.assertEqual(store.publication_writes, 0)

    def test_rollback_removes_only_matching_backfill_url_and_preserves_later_metadata(self):
        store = FakeStore([publication()])
        manifest = build_manifest([publication()], {"42": RouteResolution(PUBLIC_URL)})
        apply_manifest(manifest, store)
        metadata = json.loads(store.rows[1]["source_metadata_json"])
        metadata["operator_note"] = "preserve me"
        store.rows[1]["source_metadata_json"] = json.dumps(metadata)
        self.assertEqual(rollback_manifest(manifest, store), {"rolled_back": 1})
        restored = json.loads(store.rows[1]["source_metadata_json"])
        self.assertNotIn("public_url", restored)
        self.assertEqual(restored["operator_note"], "preserve me")
        self.assertEqual(restored["number"], "L 1")

    def test_rollback_does_not_remove_a_changed_url(self):
        store = FakeStore([publication()])
        manifest = build_manifest([publication()], {"42": RouteResolution(PUBLIC_URL)})
        apply_manifest(manifest, store)
        changed = "https://www.ft.dk/samling/20251/lovforslag/l2/index.htm"
        metadata = json.loads(store.rows[1]["source_metadata_json"])
        metadata["public_url"] = changed
        store.rows[1]["source_metadata_json"] = json.dumps(metadata)
        self.assertEqual(rollback_manifest(manifest, store), {"changed_url": 1})
        self.assertEqual(json.loads(store.rows[1]["source_metadata_json"])["public_url"], changed)

    def test_horingsportalen_publications_are_ignored(self):
        rows = [publication(source="hoeringsportalen")]
        manifest = build_manifest(rows, {"42": RouteResolution(PUBLIC_URL)})
        self.assertEqual(manifest["summary"]["total_folketinget_publications"], 0)
        self.assertEqual(manifest["records"], [])

    def test_no_event_notification_checkpoint_or_run_writes(self):
        store = FakeStore([publication()])
        manifest = build_manifest([publication()], {"42": RouteResolution(PUBLIC_URL)})
        apply_manifest(manifest, store)
        self.assertEqual(store.other_table_writes, 0)
        self.assertEqual(set(store.rows[1]), {"publication_id", "source_code", "external_id", "external_url", "source_metadata_json"})

    def test_wrangler_apply_and_rollback_sql_update_only_publication_metadata(self):
        statements = []

        def runner(args, **_kwargs):
            statements.append(args[-1])
            return SimpleNamespace(returncode=0, stdout=json.dumps([{"results": [], "meta": {"changes": 1}}]), stderr="")

        store = WranglerD1Store(runner=runner)
        self.assertTrue(store.apply_public_url(1, "42", "https://oda.ft.dk/api/Sag(42)", ORIGINAL, PUBLIC_URL))
        current = json.dumps({"number": "L 1", "public_url": PUBLIC_URL})
        self.assertTrue(store.remove_public_url(1, "42", current, PUBLIC_URL))
        self.assertEqual(len(statements), 2)
        for statement in statements:
            self.assertTrue(statement.startswith("UPDATE publications SET source_metadata_json="))
            self.assertNotRegex(statement, r"(?i)UPDATE\s+(publication_events|notifications|source_states|ingestion_runs)")
        self.assertIn("json_set(source_metadata_json", statements[0])
        self.assertIn("json_remove(source_metadata_json", statements[1])

    def test_invalid_source_or_manifest_url_is_rejected_before_write(self):
        rows = [publication()]
        manifest = build_manifest(rows, {"42": RouteResolution("https://oda.ft.dk/api/Sag(42)")})
        self.assertEqual(manifest["records"], [])
        unsafe = build_manifest(rows, {"42": RouteResolution(PUBLIC_URL)})
        unsafe["records"][0]["public_url"] = "https://evil.example/"
        store = FakeStore(rows)
        with self.assertRaises(ValueError):
            apply_manifest(unsafe, store)
        self.assertEqual(store.publication_writes, 0)


if __name__ == "__main__":
    unittest.main()
