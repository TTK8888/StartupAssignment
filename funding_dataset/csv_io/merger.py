"""
Merge new bundles into an existing dataset.csv without losing verified rows.

Match key: (normalized_startup_name, normalized_country, month_year).
Conflict criteria (per matched pair): any of Amount (USD), Round Type,
Lead Investor, the Investor set, or Investor Type differs between the
existing CSV row and the new bundle.

On conflict the existing row stays in dataset.csv unchanged and a row is
appended to conflicts.csv.
On match with no conflict the new bundle is dropped.
On a brand-new match key the new bundle's row is appended to dataset.csv.
"""

from __future__ import annotations

import csv
import re
from datetime import datetime, timezone
from pathlib import Path

from ..models import RecordBundle
from .headers import CONFLICTS_HEADER, DATASET_HEADER
from .writer import dataset_rows


_NORMALIZE_PATTERN = re.compile(r"[^a-z0-9]+")
_INVESTOR_SPLIT_PATTERN = re.compile(r"[;,]")
_PLACEHOLDER_VALUES = {"", "Not Available", "Not Stated", "Unknown", "Undisclosed"}


def _normalize(value: str) -> str:
    return _NORMALIZE_PATTERN.sub("", (value or "").lower())


def _normalized_investor_set(value: str) -> frozenset[str]:
    if not value or value in _PLACEHOLDER_VALUES:
        return frozenset()
    parts = [part.strip() for part in _INVESTOR_SPLIT_PATTERN.split(value) if part.strip()]
    return frozenset(_normalize(part) for part in parts if _normalize(part))


def _row_to_dict(row: list[str]) -> dict[str, str]:
    return {header: row[index] if index < len(row) else "" for index, header in enumerate(DATASET_HEADER)}


def _source_url_set(row_dict: dict[str, str]) -> frozenset[str]:
    urls = [
        row_dict.get("Source URL 1", ""),
        row_dict.get("Source URL 2", ""),
    ]
    return frozenset(_normalize(url) for url in urls if _normalize(url))


def _is_same_source_refresh(existing_row: dict[str, str], new_row_dict: dict[str, str]) -> bool:
    existing_sources = _source_url_set(existing_row)
    new_sources = _source_url_set(new_row_dict)
    return bool(existing_sources) and existing_sources == new_sources


def _refresh_existing_row(existing_row: dict[str, str], new_row_dict: dict[str, str]) -> dict[str, str]:
    refreshed = dict(new_row_dict)
    refreshed["Record ID"] = existing_row.get("Record ID", "") or new_row_dict.get("Record ID", "")
    return refreshed


def _match_key_from_row(row_dict: dict[str, str]) -> tuple[str, str, str]:
    return (
        _normalize(row_dict.get("Startup", "")),
        _normalize(row_dict.get("Startup Country", "")),
        (row_dict.get("Investment Month & Year", "") or "").strip().lower(),
    )


def _match_key_string(key: tuple[str, str, str]) -> str:
    return ":".join(part or "_" for part in key)


def load_existing_dataset(path: Path) -> dict[tuple[str, str, str], dict[str, str]]:
    """Read dataset.csv (if any) keyed by (startup, country, month_year)."""
    if not path.exists():
        return {}
    existing: dict[tuple[str, str, str], dict[str, str]] = {}
    with path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        rows = list(reader)
    if not rows:
        return {}
    header = rows[0]
    for raw_row in rows[1:]:
        padded = raw_row + [""] * (len(DATASET_HEADER) - len(raw_row))
        row_dict = {column: padded[index] if index < len(padded) else "" for index, column in enumerate(header)}
        # ensure all expected columns are addressable even if existing csv lacked some
        for column in DATASET_HEADER:
            row_dict.setdefault(column, "")
        existing[_match_key_from_row(row_dict)] = row_dict
    return existing


def _values_conflict(field: str, existing_value: str, new_value: str) -> bool:
    existing_value = (existing_value or "").strip()
    new_value = (new_value or "").strip()
    if existing_value in _PLACEHOLDER_VALUES or new_value in _PLACEHOLDER_VALUES:
        return False
    if field == "Investor Name":
        existing_set = _normalized_investor_set(existing_value)
        new_set = _normalized_investor_set(new_value)
        if not existing_set or not new_set:
            return False
        return existing_set != new_set
    return _normalize(existing_value) != _normalize(new_value)


def _detect_conflicts(
    existing_row: dict[str, str],
    new_row_dict: dict[str, str],
    bundle: RecordBundle,
    detected_at: str,
) -> list[list[str]]:
    fields = [
        "Investment Amount USD",
        "Investment Type",
        "Lead Investor",
        "Investor Name",
        "Investor Type",
    ]
    new_source_urls = "; ".join(
        url for url in (new_row_dict.get("Source URL 1", ""), new_row_dict.get("Source URL 2", "")) if url
    )
    existing_source_url = existing_row.get("Source URL 1", "") or existing_row.get("Source URL 2", "")
    key = _match_key_string(_match_key_from_row(existing_row))
    conflicts: list[list[str]] = []
    for field in fields:
        existing_value = existing_row.get(field, "")
        new_value = new_row_dict.get(field, "")
        if not _values_conflict(field, existing_value, new_value):
            continue
        conflicts.append(
            [
                key,
                field,
                existing_value,
                existing_source_url,
                new_value,
                new_source_urls,
                detected_at,
            ]
        )
    return conflicts


def merge(
    existing_path: Path,
    bundles: list[RecordBundle],
    *,
    detected_at: str | None = None,
) -> tuple[list[list[str]], list[list[str]]]:
    """
    Returns (merged_dataset_rows, conflict_rows).

    merged_dataset_rows is the full set to write to dataset.csv (existing rows
    in original order followed by appended new rows that didn't match).
    conflict_rows is the set to append to conflicts.csv.
    """
    timestamp = detected_at or datetime.now(timezone.utc).isoformat()
    existing = load_existing_dataset(existing_path)
    existing_keys_in_order: list[tuple[str, str, str]] = list(existing.keys())

    new_rows = dataset_rows(bundles)
    bundles_by_row_id: dict[str, RecordBundle] = {bundle.record_id: bundle for bundle in bundles}

    appended_rows: list[list[str]] = []
    conflict_rows: list[list[str]] = []
    for new_row in new_rows:
        new_dict = _row_to_dict(new_row)
        key = _match_key_from_row(new_dict)
        if not any(key):
            appended_rows.append(new_row)
            continue
        if key not in existing:
            appended_rows.append(new_row)
            existing[key] = new_dict
            existing_keys_in_order.append(key)
            continue
        bundle = bundles_by_row_id.get(new_dict.get("Record ID", ""))
        if bundle is None:
            continue
        if _is_same_source_refresh(existing[key], new_dict):
            existing[key] = _refresh_existing_row(existing[key], new_dict)
            continue
        conflicts = _detect_conflicts(existing[key], new_dict, bundle, timestamp)
        conflict_rows.extend(conflicts)

    merged_rows: list[list[str]] = []
    for key in existing_keys_in_order:
        row_dict = existing[key]
        merged_rows.append([row_dict.get(column, "") for column in DATASET_HEADER])
    return merged_rows, conflict_rows


__all__ = ["load_existing_dataset", "merge", "CONFLICTS_HEADER"]
