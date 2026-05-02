#!/usr/bin/env python3
"""
Entry point for the verified funding dataset builder.

Conductor flow:
1. parse args
2. resolve sources (--index-url is exclusive: when set, configured sources are skipped)
3. crawl + scrape + follow outbound links
4. bundle and prune
5. merge with existing dataset.csv (conflicts go to conflicts.csv, existing rows stay)
6. write all output files
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import requests

from funding_dataset.bundling import (
    build_bundles,
    follow_outbound_links,
    prune_irrelevant_leads,
)
from funding_dataset.crawler import (
    collect_leads,
    configured_sources,
    source_from_seed,
)
from funding_dataset.csv_io.headers import (
    COMPARISON_HEADER,
    CONFLICTS_HEADER,
    CRAWL_LOG_HEADER,
    DATASET_HEADER,
    LEADS_HEADER,
    SOURCE_DOC_HEADER,
)
from funding_dataset.csv_io.merger import merge as merge_dataset
from funding_dataset.csv_io.writer import (
    append_csv,
    comparison_rows,
    crawl_log_rows,
    lead_rows,
    source_documentation_rows,
    write_articles_json,
    write_csv,
)
from funding_dataset.bundling import evidence_from_lead
from funding_dataset.http_client import shutdown_browser
from funding_dataset.scraper.articles import (
    date_filter_active,
    make_date_filter,
)
from funding_dataset.settings import SourceConfig
from funding_dataset.urls import classify_source, is_allowed_source_type


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build instruction-aligned startup funding outputs from public sources."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("generated_outputs/funding_verification"),
        help="Directory for generated CSV and JSON outputs.",
    )
    parser.add_argument(
        "--source-profile",
        default="default",
        help="Named source config profile. 'small' loads config/sources.small.yaml.",
    )
    parser.add_argument("--feed", action="append", default=[], help="Extra RSS or Atom feed URL.")
    parser.add_argument(
        "--index-url",
        action="append",
        default=[],
        help="Index page to scan for links. When set, configured sources are skipped (exclusive mode).",
    )
    parser.add_argument("--url", action="append", default=[], help="Specific article or press URL to verify.")
    parser.add_argument("--max-pages", type=int, default=25, help="Maximum pages to fetch per source.")
    parser.add_argument("--max-depth", type=int, default=1, help="Maximum link depth from each seed URL.")
    parser.add_argument("--max-links-per-page", type=int, default=20, help="Maximum scored links to queue per page.")
    parser.add_argument("--request-delay", type=float, default=1.0, help="Delay between requests to the same domain.")
    parser.add_argument("--min-bangladesh", type=int, default=10, help="Required Bangladesh record count.")
    parser.add_argument("--min-sea", type=int, default=30, help="Required Southeast Asia record count.")
    parser.add_argument(
        "--year",
        action="append",
        type=int,
        default=[],
        metavar="YYYY",
        help="Limit results to this calendar year. Repeat to allow multiple years.",
    )
    parser.add_argument(
        "--from-date",
        type=str,
        default="",
        metavar="YYYY[-MM[-DD]]",
        help="Inclusive lower bound on published date.",
    )
    parser.add_argument(
        "--to-date",
        type=str,
        default="",
        metavar="YYYY[-MM[-DD]]",
        help="Inclusive upper bound on published date (year/month auto-expand to end of period).",
    )
    parser.add_argument(
        "--include-undated",
        action="store_true",
        help="Keep records whose published date could not be parsed (default: drop them when a date filter is set).",
    )
    return parser.parse_args()


def _resolve_sources(args: argparse.Namespace) -> list[SourceConfig]:
    """
    --index-url is exclusive: when at least one is provided, configured
    sources are skipped entirely and only the supplied index URLs become
    crawl seeds. --feed and --url remain additive in non-exclusive mode.
    """
    if args.index_url:
        sources: list[SourceConfig] = []
        for index_url in args.index_url:
            source_type = classify_source(index_url)
            if not is_allowed_source_type(source_type):
                print(f"[WARN] Skipping disallowed --index-url domain: {index_url}")
                continue
            sources.append(source_from_seed(index_url, source_type))
        return sources
    return configured_sources(args)


def main() -> None:
    args = parse_args()
    captured_at = datetime.now(timezone.utc).isoformat()
    session = requests.Session()
    sources = _resolve_sources(args)

    crawl_result = collect_leads(sources, args, session)
    date_filter = make_date_filter(args)
    filter_active = date_filter_active(args)
    leads_to_process = crawl_result.leads
    if filter_active:
        leads_to_process = [lead for lead in crawl_result.leads if date_filter(lead.published)]
    extracted = [
        extracted_item
        for lead in leads_to_process
        if (extracted_item := evidence_from_lead(lead, session, sources)) is not None
    ]
    if filter_active:
        extracted = [
            (ev, art, links) for ev, art, links in extracted if date_filter(ev.published)
        ]
    child_evidence, child_articles = follow_outbound_links(
        extracted, session, sources, args.request_delay
    )
    evidence_items = [evidence for evidence, _, _ in extracted] + child_evidence
    evidence_items = prune_irrelevant_leads(evidence_items, sources)
    kept_urls = {evidence.url for evidence in evidence_items}
    article_records = (
        crawl_result.articles
        + [article for _, article, _ in extracted]
        + child_articles
    )
    article_records = [article for article in article_records if article.url in kept_urls]
    bundles = build_bundles(evidence_items)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    dataset_path = args.output_dir / "dataset.csv"
    merged_rows, conflict_rows = merge_dataset(dataset_path, bundles, detected_at=captured_at)

    write_csv(dataset_path, DATASET_HEADER, merged_rows)
    if conflict_rows:
        append_csv(args.output_dir / "conflicts.csv", CONFLICTS_HEADER, conflict_rows)
    write_csv(
        args.output_dir / "source_documentation.csv",
        SOURCE_DOC_HEADER,
        source_documentation_rows(bundles),
    )
    write_csv(
        args.output_dir / "cross_source_comparison.csv",
        COMPARISON_HEADER,
        comparison_rows(bundles),
    )
    write_csv(args.output_dir / "funding_leads.csv", LEADS_HEADER, lead_rows(evidence_items, captured_at))
    write_csv(
        args.output_dir / "crawl_log.csv",
        CRAWL_LOG_HEADER,
        crawl_log_rows(crawl_result.log_entries),
    )
    write_articles_json(args.output_dir / "investment_articles.json", article_records)

    print(f"Checked {len(crawl_result.leads)} funding-like leads.")
    if filter_active:
        parts: list[str] = []
        if args.year:
            parts.append("years " + ", ".join(str(y) for y in sorted(set(args.year))))
        if args.from_date:
            parts.append(f"from {args.from_date}")
        if args.to_date:
            parts.append(f"to {args.to_date}")
        if args.include_undated:
            parts.append("including undated")
        else:
            parts.append("excluding undated")
        print(f"Date filter: {'; '.join(parts)}.")
    print(f"Wrote {len(merged_rows)} dataset rows to {dataset_path} ({len(conflict_rows)} conflicts logged).")
    print(f"Review unresolved leads in {args.output_dir / 'funding_leads.csv'}.")


if __name__ == "__main__":
    try:
        main()
    finally:
        shutdown_browser()
