from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.backfill_folketinget_public_urls import (
    MANIFEST_SCHEMA, MANIFEST_VERSION, RouteResolution, WranglerD1Store, apply_manifest,
    build_manifest, dry_run, resolve_oda_routes, rollback_manifest, validate_manifest,
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

    def test_skipped_records_keep_publication_sag_id_reason_and_metadata(self):
        rows = [publication(1, "42"), publication(2, "43")]
        manifest = build_manifest(rows, {
            "42": RouteResolution(None, "unsupported_sagstype", {"sag_id": 42, "sagstype": "§ 20-spørgsmål", "number": "S 4"}),
            "43": RouteResolution(None, "missing_periodeid", {"sag_id": 43, "periode_id": None}),
        })
        self.assertEqual(manifest["records"], [])
        self.assertEqual(manifest["skipped_by_reason"], {"missing_periodeid": 1, "unsupported_sagstype": 1})
        self.assertEqual(manifest["summary"]["remaining_without_public_url"], 2)
        skipped = {item["external_id"]: item for item in manifest["skipped_records"]}
        self.assertEqual(skipped["42"]["publication_id"], 1)
        self.assertEqual(skipped["42"]["reason"], "unsupported_sagstype")
        self.assertEqual(skipped["42"]["source_metadata"], json.loads(ORIGINAL))
        self.assertEqual(skipped["42"]["oda_audit"]["sagstype"], "§ 20-spørgsmål")
        self.assertEqual(manifest["summary"]["skipped"], len(manifest["skipped_records"]))
        self.assertEqual(sum(manifest["skipped_by_reason"].values()), len(manifest["skipped_records"]))

    def test_missing_periode_id_and_missing_periode_entity_are_distinct(self):
        rows = [publication(1, "42"), publication(2, "43")]
        manifest = build_manifest(rows, {
            "42": RouteResolution(None, "missing_periodeid", {"sag_id": 42, "periode_id": None}),
            "43": RouteResolution(None, "periode_not_found", {"sag_id": 43, "periode_id": 7, "periode_found": False}),
        })
        self.assertEqual(manifest["skipped_by_reason"], {"missing_periodeid": 1, "periode_not_found": 1})
        self.assertEqual([item["oda_audit"]["periode_id"] for item in manifest["skipped_records"]], [None, 7])

    def test_missing_and_empty_numbers_have_distinct_skip_reasons(self):
        manifest = build_manifest([publication(1, "42"), publication(2, "43")], {
            "42": RouteResolution(None, "missing_number", {"number": None}),
            "43": RouteResolution(None, "empty_number", {"number": "  "}),
        })
        self.assertEqual(manifest["skipped_by_reason"], {"empty_number": 1, "missing_number": 1})

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

    def test_wrangler_invocation_uses_worker_context_from_arbitrary_cwd(self):
        calls = []

        def runner(args, **kwargs):
            calls.append((args, kwargs))
            return SimpleNamespace(returncode=0, stdout=json.dumps([{"results": []}]), stderr="")

        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                WranglerD1Store(runner=runner)._execute("SELECT 1 AS ok")
            finally:
                os.chdir(original_cwd)

        args, kwargs = calls[0]
        expected_worker = Path(__file__).resolve().parents[1] / "worker"
        self.assertEqual(kwargs["cwd"], expected_worker)
        self.assertEqual(args[:3], ["npx", "--no-install", "wrangler"])
        self.assertIn("--remote", args)
        self.assertIn("--json", args)
        config = Path(args[args.index("--config") + 1])
        self.assertEqual(config, expected_worker / "wrangler.jsonc")
        self.assertTrue(config.is_absolute())

    def test_wrangler_failure_reports_redacted_stderr_and_exit_code(self):
        def runner(_args, **_kwargs):
            return SimpleNamespace(
                returncode=1, stdout="", stderr=(
                    "D1 failed: CLOUDFLARE_API_TOKEN=topsecret "
                    "Authorization: Bearer oauth-secret access_token=another-secret"
                ),
            )

        with self.assertRaises(RuntimeError) as raised:
            WranglerD1Store(runner=runner)._execute("SELECT 1")
        message = str(raised.exception)
        self.assertIn("exit 1", message)
        self.assertIn("D1 failed", message)
        self.assertIn("[redacted]", message)
        self.assertNotIn("topsecret", message)
        self.assertNotIn("oauth-secret", message)
        self.assertNotIn("another-secret", message)

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

    def test_failed_bulk_sag_lookup_recovers_once_with_singleton_fallback(self):
        from collectors.folketinget_models import Diagnostic

        calls = []
        sag = {"id": 42, "nummer": "B 7", "titel": "Case", "typeid": 5, "kategoriid": 15,
               "statusid": 1, "periodeid": 9, "opdateringsdato": None}

        def fake_bulk(_client, collection, _field, mapping, *, workers, batch_size):
            calls.append((collection, tuple(mapping), batch_size))
            if collection == "Sag" and batch_size == 40:
                return [], [Diagnostic("related_page_failed", "Sag", "bulk relationship lookup failed (HTTP_400:page=1)", 42)], False
            if collection == "Sag":
                return [sag], [], True
            if collection == "Periode":
                return [{"id": 9, "kode": "20251", "type": "samling"}], [], True
            if collection == "Sagstype":
                return [{"id": 5, "type": "Beslutningsforslag"}], [], True
            raise AssertionError(collection)

        with patch("collectors.folketinget_relations._bulk_scan_groups", side_effect=fake_bulk):
            result = resolve_oda_routes([publication(external_id="42")], client=object())
        self.assertEqual(result["42"].public_url, "https://www.ft.dk/samling/20251/beslutningsforslag/b7/index.htm")
        self.assertIn(("Sag", (42,), 1), calls)
        self.assertEqual(sum(1 for collection, *_ in calls if collection == "Sag"), 2)

    def test_router_supported_s20_and_aktstykke_routes_are_planned_and_apply_safely(self):
        sag_rows = [
            {"id": 42, "nummer": "S 601", "titel": "Question", "typeid": 5, "kategoriid": 15,
             "statusid": 1, "periodeid": 9, "opdateringsdato": None},
            {"id": 43, "nummer": "Aktstk. 176", "titel": "Appropriation", "typeid": 6, "kategoriid": 15,
             "statusid": 1, "periodeid": 9, "opdateringsdato": None},
            {"id": 44, "nummer": "F 8", "titel": "Unsupported", "typeid": 7, "kategoriid": 15,
             "statusid": 1, "periodeid": 9, "opdateringsdato": None},
        ]

        def fake_bulk(_client, collection, _field, mapping, *, workers, batch_size):
            if collection == "Sag":
                return sag_rows, [], True
            if collection == "Periode":
                return [{"id": 9, "kode": "20241", "type": "samling"}], [], True
            if collection == "Sagstype":
                return [
                    {"id": 5, "type": "§ 20-spørgsmål"},
                    {"id": 6, "type": "Aktstykke"},
                    {"id": 7, "type": "Forslag til vedtagelse"},
                ], [], True
            raise AssertionError(collection)

        rows = [publication(1, "42"), publication(2, "43"), publication(3, "44")]
        with patch("collectors.folketinget_relations._bulk_scan_groups", side_effect=fake_bulk):
            resolutions = resolve_oda_routes(rows, client=object())

        expected = {
            "42": "https://www.ft.dk/samling/20241/spoergsmaal/S601/index.htm",
            "43": "https://www.ft.dk/samling/20241/aktstykke/aktstk.176/index.htm",
        }
        for external_id, public_url in expected.items():
            with self.subTest(external_id=external_id):
                self.assertEqual(resolutions[external_id].public_url, public_url)
        self.assertEqual(resolutions["44"].public_url, None)
        self.assertEqual(resolutions["44"].reason, "unsupported_sagstype")

        manifest = build_manifest(rows, resolutions)
        self.assertEqual(len(manifest["records"]), 2)
        self.assertEqual(len(manifest["skipped_records"]), 1)
        self.assertEqual(manifest["summary"]["planned_updates"], 2)
        self.assertEqual(manifest["summary"]["skipped"], 1)
        self.assertEqual({row["external_id"]: row["public_url"] for row in manifest["records"]}, expected)
        self.assertEqual(manifest["skipped_records"][0]["external_id"], "44")
        validate_manifest(manifest)
        tampered = {**manifest, "records": [dict(row) for row in manifest["records"]]}
        tampered["records"][0]["oda_audit"] = {**manifest["records"][0]["oda_audit"], "sag_id": 999}
        with self.assertRaisesRegex(ValueError, "invalid or unsafe manifest record"):
            validate_manifest(tampered)

        store = FakeStore(rows)
        self.assertEqual(apply_manifest(manifest, store), {"applied": 2})
        self.assertEqual(rollback_manifest(manifest, store), {"rolled_back": 2})
        self.assertEqual(store.publication_writes, 4)

    def test_failed_fallback_stays_skipped_with_bounded_lookup_diagnostic(self):
        from collectors.folketinget_models import Diagnostic

        calls = []

        def fake_bulk(_client, collection, _field, mapping, *, workers, batch_size):
            calls.append((collection, batch_size))
            return [], [Diagnostic("related_page_failed", collection, "bulk relationship lookup failed (cursor_not_advanced:page=2)", 42)], False

        with patch("collectors.folketinget_relations._bulk_scan_groups", side_effect=fake_bulk):
            result = resolve_oda_routes([publication(external_id="42")], client=object())
        self.assertEqual(result["42"].reason, "oda_lookup_failed")
        self.assertEqual(result["42"].diagnostic["collection"], "Sag")
        self.assertEqual(result["42"].diagnostic["failure_type"], "pagination_cursor_failure")
        self.assertEqual([batch_size for collection, batch_size in calls if collection == "Sag"], [40, 1])

    def test_skipped_records_never_participate_in_apply(self):
        rows = [publication(1, "42"), publication(2, "43")]
        manifest = build_manifest(rows, {
            "42": RouteResolution(PUBLIC_URL),
            "43": RouteResolution(None, "unsupported_sagstype", {"sagstype": "§ 20-spørgsmål"}),
        })
        self.assertEqual(len(manifest["records"]), 1)
        self.assertEqual(len(manifest["skipped_records"]), 1)
        store = FakeStore(rows)
        self.assertEqual(apply_manifest(manifest, store), {"applied": 1})
        self.assertEqual(store.publication_writes, 1)
        self.assertNotIn("public_url", json.loads(store.get_publication(2)["source_metadata_json"]))

    def test_old_manifest_version_is_rejected(self):
        manifest = build_manifest([], {})
        self.assertEqual(manifest["version"], MANIFEST_VERSION)
        manifest["version"] = 1
        with self.assertRaisesRegex(ValueError, "unsupported backfill manifest schema"):
            validate_manifest(manifest)


if __name__ == "__main__":
    unittest.main()
