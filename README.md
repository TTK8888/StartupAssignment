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

From a fresh checkout, run these commands from the project root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
python main.py
```

`python main.py` loads `config/sources.yaml` and writes files to `generated_outputs/funding_verification`.

If you want a clean run without overwriting the default output folder, pass a new output directory:

```bash
python main.py --output-dir generated_outputs/funding_verification_fresh
```

The legacy command still calls the same entry point:

```bash
python build_verified_funding_dataset.py
```

### Run with the small source profile

Use `--source-profile small` to load `config/sources.small.yaml`. The small profile filters the main source config down to the domains listed in `include_domains`.

```bash
python main.py --source-profile small
```

This writes files to `generated_outputs/funding_verification_small` by default.

To run the small profile into a separate folder:

```bash
python main.py \
  --source-profile small \
  --output-dir generated_outputs/funding_verification_small_fresh
```

### Run one specific URL

Use `--url` when you want to verify one article, press release, or announcement URL.

```bash
python main.py --url https://example.com/article
```

You can combine it with the small profile:

```bash
python main.py \
  --source-profile small \
  --url https://example.com/article
```

`--url` is additive. The configured sources still run unless you also change the source set with another flag. To keep the output easy to review for a one-off URL, pass a fresh `--output-dir`.

### Add or remove sources

The default run reads source definitions from `config/sources.yaml`.

To add a new crawl source to the full run:

1. Add its domain to the right allowlist, such as `allowed_trusted_media_domains`, `allowed_startup_database_domains`, or `source_type_by_domain`.
2. Add an entry under `source_configs` with `name`, `source_type`, `allowed_domains`, `seed_urls`, and `url_patterns`.
3. Add site search templates under `site_search_templates_by_domain` if the site has useful search URLs.
4. Set `needs_browser: true` only when regular HTTP fetches do not return usable article HTML.

To remove a source from the full run, remove its `source_configs` entry. Also remove its domain from the allowlists and search templates if no other source uses it.

To add or remove a source only for the small profile, edit `config/sources.small.yaml`:

```yaml
base_config: sources.yaml

include_domains:
  - techinasia.com
  - dealstreetasia.com

followable_outbound_extra: []
```

Add a domain to `include_domains` to include it in `python main.py --source-profile small`. Remove a domain from `include_domains` to skip it. Domains in the small profile must already exist in `config/sources.yaml`; the small file does not define new sources by itself.

### Common flags

Use these flags to add crawl inputs, limit dates, or adjust crawl size:

```bash
python main.py \
  --source-profile small \
  --feed https://example.com/rss \
  --index-url https://example.com/startups \
  --url https://example.com/article \
  --year 2025 \
  --from-date 2025-01 --to-date 2025-12 \
  --max-pages 25 --max-depth 1 --request-delay 1.0
```

`--feed` adds an RSS or Atom feed to the configured sources.

`--index-url` is exclusive. When supplied, configured sources are skipped and only the supplied index URLs become crawl seeds.

`--url` adds a specific URL as a lead.

`--source-profile` selects a named source file before the crawl starts. `default` loads `config/sources.yaml`; `small` loads `config/sources.small.yaml`. Passing `--output-dir` overrides the profile output directory.

`--min-bangladesh` and `--min-sea` set the required record counts. Date filters drop undated records unless `--include-undated` is set.

## Dependencies

Python 3.10+. Install with `pip install -r requirements.txt`.

- `requests`, `beautifulsoup4`, `lxml` — fetching and parsing
- `PyYAML` — config loading
- `playwright` — used only for sources marked `needs_browser: true` in `config/sources.yaml`. After install, run `python -m playwright install chromium` once.
