# Verified Funding Dataset Builder

A modular crawler that collects startup funding announcements from Bangladesh and Southeast Asia, cross-verifies each deal across multiple public sources, and emits structured outputs.

## What it does

`main.py` walks RSS feeds, index pages, and individual article URLs, extracts deal details (company, round, amount, date, investors), and keeps only records that meet a source-quality bar. A deal is kept when it is reported by an official startup or investor site, a press release wire, a trusted media outlet, or a startup database, with priority resolved in that order. Trusted media can stand alone when the headline and body pass the completeness checks; weaker single-source items are dropped to `funding_leads.csv` for manual review.

Crawling respects `robots.txt`, applies a per-domain delay, filters URLs that look like auth, author, opinion, or binary asset paths, and rejects headlines that read like roundups, ecosystem reports, or non-target geographies.

## Project layout

```
StartupAssignment/
├── main.py                              # entry point / conductor
├── build_verified_funding_dataset.py    # legacy shim that calls main()
├── config/
│   ├── sources.yaml                     # source configs, domain whitelists, source priority
│   ├── patterns.yaml                    # regex patterns
│   ├── keywords.yaml                    # crawl keywords + search terms
│   ├── countries.yaml                   # country term whitelist
│   └── currency.yaml                    # USD/BDT rates
└── funding_dataset/
    ├── settings.py                      # loads YAML config into module-level constants
    ├── models.py                        # dataclasses
    ├── http_client.py                   # requests + playwright fetch, robots.txt
    ├── urls.py                          # URL helpers, classification
    ├── crawler.py                       # bounded crawl, lead extraction
    ├── bundling.py                      # evidence build, dedupe, bundle, prune
    ├── scraper/
    │   ├── articles.py                  # title/date/body extraction
    │   ├── amounts.py                   # currency parse + USD/BDT normalization
    │   ├── entities.py                  # startup/country/round/investor extraction
    │   ├── classification.py            # is_funding_*, has_deal_action, etc.
    │   └── confidence.py                # per-field confidence -> needs_review list
    └── csv_io/
        ├── headers.py                   # all CSV column lists
        ├── writer.py                    # row generators + write_csv/write_articles_json
        └── merger.py                    # merge new bundles with existing dataset.csv
```

## Outputs

Written under `--output-dir` (default `generated_outputs/funding_verification`):

- `dataset.csv` — verified deals, one row per company-round, with a `needs_review` column listing fields the scraper isn't confident about
- `conflicts.csv` — created when a re-run produces a row that disagrees with an existing dataset row; the existing row is kept untouched
- `source_documentation.csv` — every source URL backing a deal, with type and priority
- `cross_source_comparison.csv` — per-field agreement across sources
- `funding_leads.csv` — unverified or partially verified leads
- `crawl_log.csv` — fetch decisions per URL (kept, skipped, disallowed)
- `investment_articles.json` — raw article payloads retained for kept URLs

## Re-run behavior

`dataset.csv` is treated as the source of truth on every run. Match key is `(normalized startup, normalized country, month/year)`:

- match key not present in existing CSV: new bundle is appended
- match key present and all compared fields agree: new bundle is dropped (no duplicate)
- match key present but `Investment Amount USD`, `Investment Type`, `Lead Investor`, `Investor Name` set, or `Investor Type` differ: existing row stays as-is and a row is appended to `conflicts.csv` describing each disagreeing field

## Usage

### Run from scratch

From a fresh checkout, run these commands from the project root.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
python main.py
```

`python main.py` uses the configured sources in `config/sources.yaml` and writes outputs to `generated_outputs/funding_verification`.

To use a smaller source profile, pass `--source-profile small`. This loads `config/sources.small.yaml` instead of `config/sources.yaml`:

```bash
python main.py --source-profile small
```

If you prefer the legacy command, this still calls the same entry point:

```bash
python build_verified_funding_dataset.py
```

To rebuild a clean output folder, remove the generated files first or pass a new output directory:

```bash
python main.py --output-dir generated_outputs/funding_verification_fresh
```

### Optional flags

All flags are optional. Use them when you want to add sources, limit dates, or change crawl bounds.

```
python main.py \
  --source-profile small \
  --feed https://example.com/rss \
  --index-url https://example.com/startups \
  --url https://example.com/article \
  --year 2025 \
  --from-date 2025-01 --to-date 2025-12 \
  --max-pages 25 --max-depth 1 --request-delay 1.0
```

`--index-url` is exclusive: when at least one is supplied, the configured sources in `config/sources.yaml` are skipped and only the supplied URLs become crawl seeds. `--feed` and `--url` remain additive.

`--source-profile` selects a named source file before the crawl starts. The default profile loads `config/sources.yaml`; `--source-profile small` loads `config/sources.small.yaml`.

Quotas: `--min-bangladesh` and `--min-sea` set the required record counts. Date filters drop undated records unless `--include-undated` is set.

## Dependencies

Python 3.10+. Install with `pip install -r requirements.txt`.

- `requests`, `beautifulsoup4`, `lxml` — fetching and parsing
- `PyYAML` — config loading
- `playwright` — used only for sources marked `needs_browser: true` in `config/sources.yaml`. After install, run `python -m playwright install chromium` once.
