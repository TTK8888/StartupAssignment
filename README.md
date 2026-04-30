# Verified Funding Dataset Builder

A single-script crawler that collects startup funding announcements from Bangladesh and Southeast Asia, cross-verifies each deal across multiple public sources, and emits structured outputs.

## What it does

`build_verified_funding_dataset.py` walks RSS feeds, index pages, and individual article URLs, extracts deal details (company, round, amount, date, investors), and keeps only records that meet a source-quality bar. A deal is kept when it is reported by an official startup or investor site, a press release wire, a trusted media outlet, or a startup database, with priority resolved in that order. Trusted media can stand alone when the headline and body pass the completeness checks; weaker single-source items are dropped to `funding_leads.csv` for manual review.

Crawling respects `robots.txt`, applies a per-domain delay, filters URLs that look like auth, author, opinion, or binary asset paths, and rejects headlines that read like roundups, ecosystem reports, or non-target geographies.

## Outputs

Written under `--output-dir` (default `generated_outputs/funding_verification`):

- `dataset.csv` — verified deals, one row per company-round
- `source_documentation.csv` — every source URL backing a deal, with type and priority
- `cross_source_comparison.csv` — per-field agreement across sources
- `funding_leads.csv` — unverified or partially verified leads
- `crawl_log.csv` — fetch decisions per URL (kept, skipped, disallowed)
- `investment_articles.json` — raw article payloads retained for kept URLs

## Usage

```
python build_verified_funding_dataset.py \
  --feed https://example.com/rss \
  --index-url https://example.com/startups \
  --url https://example.com/article \
  --year 2025 \
  --from-date 2025-01 --to-date 2025-12 \
  --max-pages 25 --max-depth 1 --request-delay 1.0
```

Quotas: `--min-bangladesh` and `--min-sea` set the required record counts. Date filters drop undated records unless `--include-undated` is set.

## Dependencies

Python 3.10+, `requests`, `beautifulsoup4`. A headless browser path is wired in for sources that need JS rendering.
