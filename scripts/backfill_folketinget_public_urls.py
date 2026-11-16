#!/usr/bin/env python3
"""Manifest-driven, one-off Folketinget public URL metadata backfill.

Default mode is read-only against D1 and ODA, and writes only a local manifest.
Apply and rollback require an existing manifest and never call ingestion APIs.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_SCHEMA = "denmarktgbot.folketinget-public-url-backfill"
MANIFEST_VERSION = 2
APPLY_BATCH_SIZE = 25
_SAG_ID = re.compile(r"^[1-9]\d{0,19}$")
_ALLOWED_SKIP_REASONS = frozenset({
    "invalid_metadata", "missing_sag_id", "oda_lookup_failed", "missing_sag",
    "missing_periodeid", "periode_not_found", "invalid_periode_code",
    "unsupported_sagstype", "sagstype_not_found", "missing_sagstype_id",
    "missing_number", "empty_number", "route_invalid_number",
    "number_type_mismatch", "route_validation_failure",
})


def _sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _json_object(value: object) -> dict[str, Any] | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _load_url_validator() -> Callable[[object], str | None]:
    # Keep the exact ingestion allowlist/path validation as the single source
    # of truth for URLs written to D1.
    from collectors.ingestion_client import _validated_folketinget_public_url
    return _validated_folketinget_public_url


@dataclass(frozen=True)
class RouteResolution:
    public_url: str | None
    reason: str | None = None
    audit: Mapping[str, object] | None = None
    diagnostic: Mapping[str, str] | None = None


def _diagnostic_failure(message: str) -> tuple[str, str]:
    """Map bounded ODA diagnostics to a small, stable set of safe labels."""
    detail = message.strip()[:240]
    lowered = detail.lower()
    if re.search(r"\bHTTP_\d+\b|\bHTTP_error\b", detail):
        return "request_http_failure", detail
    if any(token in lowered for token in ("cursor", "pagination", "page_limit", "upper_bound", "page=")):
        return "pagination_cursor_failure", detail
    return "other_read_failure", detail


def _bulk_scan_with_bounded_fallback(
    client: Any, collection: str, field: str, mapping: Mapping[int, set[int]],
) -> tuple[list[dict[str, Any]], list[Any]]:
    """Retry only IDs from failed groups, once, as singleton read-only scans."""
    from collectors.folketinget_relations import _bulk_scan_groups

    rows, diagnostics, _ = _bulk_scan_groups(client, collection, field, mapping, workers=4, batch_size=40)
    failed_sags = {item.entity_id for item in diagnostics if item.entity_id is not None}
    if not failed_sags:
        return rows, []
    retry_mapping = {
        key: set(sag_ids) for key, sag_ids in mapping.items()
        if set(sag_ids).intersection(failed_sags)
    }
    if not retry_mapping:
        return rows, diagnostics
    retry_rows, retry_diagnostics, _ = _bulk_scan_groups(
        client, collection, field, retry_mapping, workers=4, batch_size=1,
    )
    # Partial rows from failed scans are retained; dedup by ODA entity ID.
    by_id: dict[int, dict[str, Any]] = {}
    for row in [*rows, *retry_rows]:
        identifier = row.get("id")
        if isinstance(identifier, int) and not isinstance(identifier, bool):
            by_id[identifier] = row
    return list(by_id.values()), retry_diagnostics


def _hydrate_route_fields(client: Any, sags: Sequence[Any]) -> tuple[list[Any], dict[int, list[dict[str, str]]], set[int], set[int]]:
    from dataclasses import replace

    periods = {item.periodeid: {item.id} for item in sags if item.periodeid is not None}
    types = {item.typeid: {item.id} for item in sags if item.typeid is not None}
    period_rows, period_diagnostics = _bulk_scan_with_bounded_fallback(client, "Periode", "id", periods) if periods else ([], [])
    type_rows, type_diagnostics = _bulk_scan_with_bounded_fallback(client, "Sagstype", "id", types) if types else ([], [])
    period_by_id = {row.get("id"): row for row in period_rows if isinstance(row.get("id"), int)}
    type_by_id = {row.get("id"): row for row in type_rows if isinstance(row.get("id"), int)}
    period_failed = {item.entity_id: item.message for item in period_diagnostics if item.entity_id is not None}
    type_failed = {item.entity_id: item.message for item in type_diagnostics if item.entity_id is not None}
    lookup_by_sag: dict[int, list[dict[str, str]]] = {}
    hydrated = []
    for sag in sags:
        period = period_by_id.get(sag.periodeid)
        sagtype = type_by_id.get(sag.typeid)
        code = period.get("kode") if period and period.get("type") == "samling" else None
        name = sagtype.get("type") if sagtype else None
        hydrated.append(replace(
            sag,
            periode_code=code if isinstance(code, str) and re.fullmatch(r"\d{5}", code) else None,
            sagstype=name if isinstance(name, str) else None,
        ))
        problems: list[tuple[str, str]] = []
        if sag.id in period_failed:
            problems.append(("Periode", period_failed[sag.id]))
        if sag.id in type_failed:
            problems.append(("Sagstype", type_failed[sag.id]))
        if problems:
            lookup_by_sag[sag.id] = [
                {"collection": collection, "failure_type": _diagnostic_failure(message)[0],
                 "detail": _diagnostic_failure(message)[1]}
                for collection, message in problems
            ]
    return hydrated, lookup_by_sag, set(period_by_id), set(type_by_id)


class WranglerD1Store:
    """Narrow D1 adapter; every write statement targets publications only."""

    def __init__(
        self,
        database: str = "denmarktgbot-db",
        config: Path | None = None,
        *,
        runner: Callable[..., Any] = subprocess.run,
    ) -> None:
        self.database = database
        self.config = config or ROOT / "worker" / "wrangler.jsonc"
        self.runner = runner
        local = ROOT / "worker" / "node_modules" / ".bin" / "wrangler"
        self.wrangler = str(local) if local.exists() else (shutil.which("wrangler") or "wrangler")

    def _execute(self, sql: str) -> dict[str, Any]:
        completed = self.runner(
            [self.wrangler, "d1", "execute", self.database, "--remote", "--config", str(self.config), "--json", "--command", sql],
            cwd=ROOT / "worker", capture_output=True, text=True, check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"Wrangler D1 command failed (exit {completed.returncode})")
        output = completed.stdout.strip()
        payload: Any = None
        for index, char in enumerate(output):
            if char not in "[{":
                continue
            try:
                payload = json.loads(output[index:])
                break
            except json.JSONDecodeError:
                continue
        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
            return payload[0]
        if isinstance(payload, dict):
            return payload
        raise RuntimeError("Wrangler D1 returned an unreadable JSON response")

    def list_folketinget_publications(self) -> list[dict[str, Any]]:
        result = self._execute(
            "SELECT p.id AS publication_id, p.external_id, p.external_url, "
            "p.source_metadata_json, s.code AS source_code "
            "FROM publications p JOIN sources s ON s.id=p.source_id "
            "WHERE s.code='folketinget' ORDER BY p.id"
        )
        rows = result.get("results", [])
        return rows if isinstance(rows, list) else []

    def get_publication(self, publication_id: int) -> dict[str, Any] | None:
        result = self._execute(
            "SELECT p.id AS publication_id, p.external_id, p.external_url, "
            "p.source_metadata_json, s.code AS source_code "
            "FROM publications p LEFT JOIN sources s ON s.id=p.source_id "
            f"WHERE p.id={publication_id} LIMIT 1"
        )
        rows = result.get("results", [])
        return rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else None

    def apply_public_url(
        self, publication_id: int, external_id: str, external_url: str,
        original_metadata: str, public_url: str,
    ) -> bool:
        result = self._execute(
            "UPDATE publications SET source_metadata_json=json_set(source_metadata_json, "
            f"'$.public_url', {_sql_text(public_url)}) "
            f"WHERE id={publication_id} "
            "AND source_id=(SELECT id FROM sources WHERE code='folketinget') "
            f"AND external_id={_sql_text(external_id)} "
            f"AND external_url={_sql_text(external_url)} "
            f"AND source_metadata_json={_sql_text(original_metadata)} "
            "AND json_valid(source_metadata_json) AND json_type(source_metadata_json)='object' "
            "AND json_type(source_metadata_json, '$.public_url') IS NULL"
        )
        return _changes(result) == 1

    def remove_public_url(
        self, publication_id: int, external_id: str, current_metadata: str, public_url: str,
    ) -> bool:
        result = self._execute(
            "UPDATE publications SET source_metadata_json=json_remove(source_metadata_json, '$.public_url') "
            f"WHERE id={publication_id} "
            "AND source_id=(SELECT id FROM sources WHERE code='folketinget') "
            f"AND external_id={_sql_text(external_id)} "
            f"AND source_metadata_json={_sql_text(current_metadata)} "
            "AND json_valid(source_metadata_json) AND json_type(source_metadata_json)='object' "
            f"AND json_extract(source_metadata_json, '$.public_url')={_sql_text(public_url)}"
        )
        return _changes(result) == 1


def _changes(result: Mapping[str, Any]) -> int:
    meta = result.get("meta")
    changes = meta.get("changes", 0) if isinstance(meta, Mapping) else 0
    return changes if isinstance(changes, int) and not isinstance(changes, bool) else 0


def resolve_oda_routes(
    publications: Sequence[Mapping[str, Any]], client: Any | None = None,
) -> dict[str, RouteResolution]:
    """Read only Sag rows plus their Periode/Sagstype lookups for candidate IDs."""
    from collectors.folketinget import ODataClient
    from collectors.folketinget_models import Sag, normalize_many
    from collectors.folketinget_relations import _PUBLIC_ROUTE_TYPES, _public_case_url

    validator = _load_url_validator()
    candidates: dict[str, Mapping[str, Any]] = {}
    for row in publications:
        external_id = row.get("external_id")
        metadata = _json_object(row.get("source_metadata_json"))
        if row.get("source_code") == "folketinget" and isinstance(external_id, str) and _SAG_ID.fullmatch(external_id) and metadata is not None and "public_url" not in metadata:
            candidates[external_id] = row
    if not candidates:
        return {}

    odata = client or ODataClient()
    ids = {int(item): {int(item)} for item in candidates}
    raw_sags, diagnostics = _bulk_scan_with_bounded_fallback(odata, "Sag", "id", ids)
    normalized = normalize_many(raw_sags, "Sag")
    sags = [item for item in (normalized.record or ()) if isinstance(item, Sag)]
    failed_ids = {str(item.entity_id) for item in diagnostics if item.entity_id is not None}
    hydrated, lookup_failures, found_period_ids, found_type_ids = _hydrate_route_fields(odata, sags)
    by_id = {str(item.id): item for item in hydrated}

    resolutions: dict[str, RouteResolution] = {}
    for external_id in candidates:
        if external_id in failed_ids:
            messages = [item.message for item in diagnostics if str(item.entity_id) == external_id]
            failure_type, detail = _diagnostic_failure(messages[0] if messages else "Sag read failed")
            resolutions[external_id] = RouteResolution(
                None, "oda_lookup_failed", diagnostic={"collection": "Sag", "failure_type": failure_type, "detail": detail},
            )
            continue
        sag = by_id.get(external_id)
        if sag is None:
            resolutions[external_id] = RouteResolution(
                None, "missing_sag",
                diagnostic={"collection": "Sag", "failure_type": "complete_read_no_match", "detail": "completed ODA lookup returned no Sag row"},
            )
            continue
        audit = {
            "sag_id": sag.id, "number": sag.nummer, "type_id": sag.typeid,
            "sagstype": sag.sagstype, "periode_id": sag.periodeid,
            "periode_code": sag.periode_code,
            "periode_found": sag.periodeid in found_period_ids if sag.periodeid is not None else False,
            "sagstype_found": sag.typeid in found_type_ids if sag.typeid is not None else False,
        }
        if sag.id in lookup_failures:
            diagnostic = lookup_failures[sag.id][0]
            resolutions[external_id] = RouteResolution(None, "oda_lookup_failed", audit, diagnostic)
            continue
        if sag.periodeid is None:
            reason = "missing_periodeid"
        elif not isinstance(sag.periode_code, str) or not re.fullmatch(r"\d{5}", sag.periode_code):
            reason = "periode_not_found" if not audit["periode_found"] else "invalid_periode_code"
        elif sag.nummer is None:
            reason = "missing_number"
        elif not sag.nummer.strip():
            reason = "empty_number"
        elif sag.sagstype not in _PUBLIC_ROUTE_TYPES:
            reason = "missing_sagstype_id" if sag.typeid is None else ("sagstype_not_found" if not audit["sagstype_found"] else "unsupported_sagstype")
        else:
            url = _public_case_url(sag)
            validated = validator(url)
            if validated:
                resolutions[external_id] = RouteResolution(validated, audit=audit)
                continue
            number = re.fullmatch(r"\s*([LBF])\s*(\d+)\s*", sag.nummer or "", flags=re.IGNORECASE)
            route_prefix = _PUBLIC_ROUTE_TYPES.get(sag.sagstype or "", ("", ""))[1]
            reason = "route_invalid_number" if number is None else ("number_type_mismatch" if number.group(1).upper() != route_prefix else "route_validation_failure")
        resolutions[external_id] = RouteResolution(None, reason, audit, {"detail": reason})
    return resolutions


def build_manifest(
    publications: Sequence[Mapping[str, Any]],
    resolutions: Mapping[str, RouteResolution],
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    folketinget = [row for row in publications if row.get("source_code") == "folketinget"]
    already = 0
    candidates: list[Mapping[str, Any]] = []
    skipped_records: list[dict[str, Any]] = []
    for row in folketinget:
        metadata = _json_object(row.get("source_metadata_json"))
        if metadata is None:
            candidates.append(row)
            continue
        elif "public_url" in metadata:
            already += 1
        elif not isinstance(row.get("external_id"), str) or not _SAG_ID.fullmatch(row["external_id"]):
            candidates.append(row)
        else:
            candidates.append(row)

    validator = _load_url_validator()
    records: list[dict[str, Any]] = []
    skipped: Counter[str] = Counter()
    for row in candidates:
        external_id = str(row["external_id"])
        metadata = _json_object(row.get("source_metadata_json"))
        publication_id = row.get("publication_id")
        resolution = resolutions.get(external_id)
        reason: str | None = None
        if metadata is None:
            reason = "invalid_metadata"
        elif not isinstance(row.get("external_id"), str) or not _SAG_ID.fullmatch(str(row.get("external_id"))):
            reason = "missing_sag_id"
        elif resolution is None:
            reason = "missing_sag"
        elif resolution.public_url is None:
            reason = resolution.reason if resolution.reason in _ALLOWED_SKIP_REASONS else "route_validation_failure"
        if reason:
            skipped[reason] += 1
            skipped_records.append({
                "publication_id": publication_id if isinstance(publication_id, int) and not isinstance(publication_id, bool) else None,
                "external_id": row.get("external_id"),
                "reason": reason,
                "source_metadata": metadata,
                "oda_audit": dict(resolution.audit or {}) if resolution and resolution.audit else {},
                "diagnostic": dict(resolution.diagnostic or {"detail": reason}) if resolution else {"detail": reason},
            })
            continue
        assert resolution is not None
        url = validator(resolution.public_url)
        original = row.get("source_metadata_json")
        metadata = _json_object(original)
        external_url = row.get("external_url")
        if not url or metadata is None:
            skipped["route_validation_failure"] += 1
            skipped_records.append({"publication_id": publication_id, "external_id": row.get("external_id"), "reason": "route_validation_failure", "source_metadata": metadata, "oda_audit": dict(resolution.audit or {}), "diagnostic": {"detail": "manifest URL or source metadata failed validation"}})
            continue
        if isinstance(publication_id, bool) or not isinstance(publication_id, int) or publication_id <= 0 or not isinstance(external_url, str):
            skipped["route_validation_failure"] += 1
            skipped_records.append({"publication_id": publication_id if isinstance(publication_id, int) and not isinstance(publication_id, bool) else None, "external_id": row.get("external_id"), "reason": "route_validation_failure", "source_metadata": metadata, "oda_audit": dict(resolution.audit or {}), "diagnostic": {"detail": "publication identity or external URL failed validation"}})
            continue
        records.append({
            "publication_id": publication_id,
            "source_code": "folketinget",
            "external_id": external_id,
            "external_url": external_url,
            "original_source_metadata_json": original,
            "public_url": url,
            "oda_audit": dict(resolution.audit or {}),
        })

    total = len(folketinget)
    planned = len(records)
    skipped_by_sagstype = Counter(
        str(item.get("oda_audit", {}).get("sagstype") or "<missing>")
        for item in skipped_records if item["reason"] == "unsupported_sagstype"
    )
    skipped_by_lookup: Counter[str] = Counter()
    for item in skipped_records:
        if item["reason"] == "oda_lookup_failed":
            diagnostic = item.get("diagnostic", {})
            skipped_by_lookup[f"{diagnostic.get('collection', 'unknown')}:{diagnostic.get('failure_type', 'other_read_failure')}"] += 1
    summary = {
        "total_folketinget_publications": total,
        "already_have_public_url": already,
        "candidates_checked": sum(
            1 for row in candidates
            if _json_object(row.get("source_metadata_json")) is not None
            and isinstance(row.get("external_id"), str)
            and _SAG_ID.fullmatch(row["external_id"])
        ),
        "backfillable": planned,
        "skipped": sum(skipped.values()),
        "planned_updates": planned,
        "remaining_without_public_url": total - already - planned,
        "skipped_by_sagstype": dict(sorted(skipped_by_sagstype.items())),
        "skipped_by_lookup_failure": dict(sorted(skipped_by_lookup.items())),
    }
    return {
        "schema": MANIFEST_SCHEMA,
        "version": MANIFEST_VERSION,
        "created_at": created_at or datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "summary": summary,
        "skipped_by_reason": dict(sorted(skipped.items())),
        "skipped_records": skipped_records,
        "records": records,
    }


def validate_manifest(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("schema") != MANIFEST_SCHEMA or value.get("version") != MANIFEST_VERSION:
        raise ValueError("unsupported backfill manifest schema")
    records = value.get("records")
    if not isinstance(records, list):
        raise ValueError("manifest records must be a list")
    skipped_records = value.get("skipped_records")
    if not isinstance(skipped_records, list):
        raise ValueError("manifest skipped_records must be a list")
    validator = _load_url_validator()
    seen: set[int] = set()
    for row in records:
        if not isinstance(row, dict):
            raise ValueError("manifest record must be an object")
        pub_id, external_id = row.get("publication_id"), row.get("external_id")
        original, external_url, public_url = row.get("original_source_metadata_json"), row.get("external_url"), row.get("public_url")
        metadata = _json_object(original)
        if (isinstance(pub_id, bool) or not isinstance(pub_id, int) or pub_id <= 0 or pub_id in seen
                or row.get("source_code") != "folketinget" or not isinstance(external_id, str) or not _SAG_ID.fullmatch(external_id)
                or not isinstance(external_url, str) or metadata is None or "public_url" in metadata
                or validator(public_url) != public_url or not isinstance(row.get("oda_audit"), dict)):
            raise ValueError("invalid or unsafe manifest record")
        seen.add(pub_id)
    return value


def write_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _read_manifest(path: Path) -> dict[str, Any]:
    return validate_manifest(json.loads(path.read_text(encoding="utf-8")))


def dry_run(store: Any, manifest_path: Path, *, resolver: Callable[[Sequence[Mapping[str, Any]]], Mapping[str, RouteResolution]] | None = None) -> dict[str, Any]:
    rows = store.list_folketinget_publications()
    route_resolutions = (resolver or resolve_oda_routes)(rows)
    manifest = build_manifest(rows, route_resolutions)
    write_manifest(manifest_path, manifest)
    return manifest


def apply_manifest(manifest: Mapping[str, Any], store: Any) -> dict[str, int]:
    checked = validate_manifest(dict(manifest))
    counts = Counter()
    records = checked["records"]
    for start in range(0, len(records), APPLY_BATCH_SIZE):
        for record in records[start:start + APPLY_BATCH_SIZE]:
            current = store.get_publication(record["publication_id"])
            if not current or current.get("source_code") != "folketinget" or current.get("external_id") != record["external_id"]:
                counts["stale_or_wrong_source"] += 1
                continue
            metadata = _json_object(current.get("source_metadata_json"))
            if metadata is None:
                counts["stale_metadata"] += 1
            elif metadata.get("public_url") == record["public_url"]:
                counts["already_applied"] += 1
            elif "public_url" in metadata or current.get("source_metadata_json") != record["original_source_metadata_json"] or current.get("external_url") != record["external_url"]:
                counts["stale_metadata"] += 1
            elif store.apply_public_url(
                record["publication_id"], record["external_id"], record["external_url"],
                record["original_source_metadata_json"], record["public_url"],
            ):
                counts["applied"] += 1
            else:
                counts["stale_metadata"] += 1
    return dict(counts)


def rollback_manifest(manifest: Mapping[str, Any], store: Any) -> dict[str, int]:
    checked = validate_manifest(dict(manifest))
    counts = Counter()
    records = checked["records"]
    for start in range(0, len(records), APPLY_BATCH_SIZE):
        for record in records[start:start + APPLY_BATCH_SIZE]:
            current = store.get_publication(record["publication_id"])
            if not current or current.get("source_code") != "folketinget" or current.get("external_id") != record["external_id"]:
                counts["stale_or_wrong_source"] += 1
                continue
            metadata_text = current.get("source_metadata_json")
            metadata = _json_object(metadata_text)
            if metadata is None:
                counts["stale_metadata"] += 1
            elif "public_url" not in metadata:
                counts["already_rolled_back"] += 1
            elif metadata.get("public_url") != record["public_url"]:
                counts["changed_url"] += 1
            elif store.remove_public_url(
                record["publication_id"], record["external_id"], str(metadata_text), record["public_url"],
            ):
                counts["rolled_back"] += 1
            else:
                counts["stale_metadata"] += 1
    return dict(counts)


def _summary_for_cli(manifest: Mapping[str, Any]) -> None:
    print(json.dumps({"summary": manifest["summary"], "skipped_by_reason": manifest["skipped_by_reason"]}, ensure_ascii=False, indent=2))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--apply", action="store_true", help="apply only the exact planned records in the manifest")
    modes.add_argument("--rollback", action="store_true", help="remove only matching public_url values from the manifest")
    parser.add_argument("--manifest", type=Path, required=True, help="local JSON manifest path")
    parser.add_argument("--database", default="denmarktgbot-db", help="Wrangler D1 database name")
    parser.add_argument("--config", type=Path, default=ROOT / "worker" / "wrangler.jsonc", help="Wrangler config path")
    args = parser.parse_args(argv)
    store = WranglerD1Store(args.database, args.config)
    try:
        if args.apply:
            result = apply_manifest(_read_manifest(args.manifest), store)
            print(json.dumps({"mode": "apply", "result": result}, indent=2))
        elif args.rollback:
            result = rollback_manifest(_read_manifest(args.manifest), store)
            print(json.dumps({"mode": "rollback", "result": result}, indent=2))
        else:
            manifest = dry_run(store, args.manifest)
            _summary_for_cli(manifest)
            print(f"manifest={args.manifest}")
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"backfill stopped: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
