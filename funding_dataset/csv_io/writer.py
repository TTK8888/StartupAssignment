from __future__ import annotations

import csv
import json
from pathlib import Path

from ..bundling import (
    _MISSING_VALUE_SENTINELS,
    choose_value,
    is_complete_trusted_media_source,
)
from ..models import ArticleRecord, Evidence, RecordBundle
from ..scraper.amounts import normalized_amount_values
from ..scraper.articles import month_year
from ..settings import (
    AMOUNT_PATTERN,
    DEAL_TITLE_PATTERN,
    NON_TARGET_COUNTRY_PATTERN,
    ROUNDUP_TITLE_PATTERN,
    STANDALONE_SOURCE_TYPES,
)
from ..urls import canonical_source_url


def dataset_rows(bundles: list[RecordBundle]) -> list[list[str]]:
    rows: list[list[str]] = []
    for bundle in bundles:
        unique_sources: list[Evidence] = []
        seen: set[str] = set()
        for source in bundle.sources:
            canonical_url = canonical_source_url(source.url)
            if canonical_url in seen:
                continue
            seen.add(canonical_url)
            unique_sources.append(source)

        if not unique_sources:
            continue
        if not bundle.startup:
            continue

        primary = unique_sources[0]
        secondary = unique_sources[1] if len(unique_sources) > 1 else None
        is_cross_verified = secondary is not None
        is_standalone = (
            secondary is None
            and primary.source_type in STANDALONE_SOURCE_TYPES
        )
        is_trusted_complete = (
            secondary is None
            and is_complete_trusted_media_source(primary, bundle.startup)
        )
        if not (is_cross_verified or is_standalone or is_trusted_complete):
            continue

        soft_issues = [
            issue
            for issue in bundle.issues
            if issue
            not in {
                "Needs a second source URL before final dataset use",
                "Startup name needs manual verification",
            }
        ]
        if is_cross_verified:
            verification_status = "Cross-verified"
            if soft_issues:
                verification_status = "Cross-verified, partial metadata"
        elif is_trusted_complete:
            verification_status = "Single trusted-media source"
        else:
            verification_status = "Single authoritative source"

        conflict_notes: list[str] = []
        if secondary:
            for label, attr in [
                ("amount", "amount"),
                ("round", "round_type"),
                ("lead", "lead_investor"),
            ]:
                value_a = getattr(primary, attr)
                value_b = getattr(secondary, attr)
                if value_a and value_b and value_a != value_b:
                    conflict_notes.append(f"Conflict {label}: A={value_a} | B={value_b}")

        notes_parts = [
            "Cross-verified public sources" if is_cross_verified else "Authoritative single source",
        ]
        if conflict_notes:
            notes_parts.extend(conflict_notes)
        if soft_issues:
            notes_parts.extend(soft_issues)
        notes = "; ".join(notes_parts)
        amount = choose_value(unique_sources, "amount", "Undisclosed")
        published = choose_value(unique_sources, "published", "")
        normalized_usd, normalized_bdt = normalized_amount_values(amount, published)

        # union of needs_review across the sources kept for this bundle row
        needs_review_fields: list[str] = []
        seen_review: set[str] = set()
        for source in unique_sources:
            for field_name in source.needs_review:
                if field_name in seen_review:
                    continue
                seen_review.add(field_name)
                needs_review_fields.append(field_name)
        needs_review_value = ", ".join(needs_review_fields)

        rows.append(
            [
                bundle.record_id,
                bundle.startup,
                choose_value(unique_sources, "investor_names", "Not Available"),
                month_year(published),
                bundle.country,
                amount,
                normalized_usd,
                normalized_bdt,
                choose_value(unique_sources, "round_type", "Not Available"),
                choose_value(unique_sources, "lead_investor", "Not Stated"),
                choose_value(unique_sources, "investor_type", "Not Available"),
                primary.url,
                secondary.url if secondary else "",
                notes,
                verification_status,
                needs_review_value,
            ]
        )
    return rows


def source_documentation_rows(bundles: list[RecordBundle]) -> list[list[str]]:
    rows: list[list[str]] = []
    for bundle in bundles:
        primary = bundle.sources[0]
        secondary = bundle.sources[1] if len(bundle.sources) > 1 else None
        primary_title = primary.title or ""
        title_url_blob = f"{primary_title} {primary.url}"
        names_non_target = bool(NON_TARGET_COUNTRY_PATTERN.search(title_url_blob))
        is_trusted_complete = (
            secondary is None
            and primary.source_type == "Trusted media"
            and not primary.headline_only
            and bundle.startup
            and primary.country not in _MISSING_VALUE_SENTINELS
            and not names_non_target
            and primary.amount not in {"Undisclosed", "", None}
            and bool(DEAL_TITLE_PATTERN.search(primary_title))
            and bool(AMOUNT_PATTERN.search(primary_title))
            and not ROUNDUP_TITLE_PATTERN.search(primary_title)
        )
        if not bundle.startup:
            status = "Needs review"
        elif (
            secondary is None
            and primary.source_type not in STANDALONE_SOURCE_TYPES
            and not is_trusted_complete
        ):
            status = "Needs review"
        elif secondary is None and is_trusted_complete:
            status = "Single trusted-media source"
        elif secondary is None:
            status = "Single authoritative source"
        elif bundle.issues:
            status = "Cross-verified, partial metadata"
        else:
            status = "Cross-verified"
        rows.append(
            [
                bundle.record_id,
                bundle.startup,
                primary.url,
                primary.source_type,
                secondary.url if secondary else "",
                secondary.source_type if secondary else "",
                status,
                "; ".join(bundle.issues),
            ]
        )
    return rows


def comparison_rows(bundles: list[RecordBundle]) -> list[list[str]]:
    rows: list[list[str]] = []
    fields = [
        ("Total Investment Amount", "amount", "Amount discrepancy"),
        ("Investment Type", "round_type", "Round classification difference"),
        ("Lead Investor", "lead_investor", "Lead investor difference"),
    ]
    for bundle in bundles:
        if len(bundle.sources) < 2:
            continue
        source_a = bundle.sources[0]
        source_b = bundle.sources[1]
        for label, attr, difference_type in fields:
            value_a = getattr(source_a, attr)
            value_b = getattr(source_b, attr)
            if value_a == value_b:
                continue
            rows.append(
                [
                    bundle.startup,
                    bundle.country,
                    label,
                    source_a.url,
                    value_a,
                    source_b.url,
                    value_b,
                    difference_type,
                    "Use official or company press source first; keep conflict in notes",
                ]
            )
    return rows


def lead_rows(evidence_items: list[Evidence], captured_at: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for evidence in evidence_items:
        status = "Needs review" if evidence.notes else "Extracted"
        rows.append(
            [
                captured_at,
                evidence.startup,
                evidence.country,
                evidence.source_type,
                evidence.title,
                evidence.url,
                evidence.published,
                evidence.amount,
                evidence.round_type,
                evidence.lead_investor,
                status,
                "; ".join(evidence.notes),
            ]
        )
    return rows


def crawl_log_rows(log_entries) -> list[list[str]]:
    return [[entry.url, entry.source, entry.action, entry.reason] for entry in log_entries]


def write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)


def append_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    """Append rows to an existing CSV; create with header if missing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        if write_header:
            writer.writerow(header)
        writer.writerows(rows)


def write_articles_json(path: Path, articles: list[ArticleRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            "title": article.title,
            "date": article.date,
            "source": article.source,
            "url": article.url,
            "content": article.content,
            "keywords": article.keywords,
        }
        for article in articles
    ]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
