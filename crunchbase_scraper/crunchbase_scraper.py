#!/usr/bin/env python3
"""
Standalone Crunchbase Scraper

uses authenticated Playwright session to extract funding rounds

"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import path

from funding_dataset.csv_io.headers import (
    CRAWL_LOG_HEADER,
    DATASET_HEADER,
    LEADS_HEADER,
)

from funding_dataset.csv_io.writer import write_csv
from funding_dataset.models import Evidence
from funding_dataset.scraper.amounts import normalized_amount_values
from funding_dataset.scraper.articles import month_year
from funding_dataset.scraper.confidence import assess_evidence
from funding_dataset.scraper.entities import (
    clean_entity_list,
    country_from_text,
    investor_type_from_name,
    round_from_text,
)

# ------------------------------
#
# Constants
#
# ------------------------------

DEFAULT_OUTPUT_DIR = Path("generated_outputs/crunchbase")
DEFAULT_STORAGE_STATE = Path("secrets/crunchbase_storage_state.json")
DEFAULT_REQUEST_DELAY = 3.0
PAGE_TIMEOUT_MS = 45_000
LOGIN_TIMEOUT_MS = 5 * 60 * 1000 # five mins for 2FA

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)

DEFAULT_HUB_URLS = [
    "https://www.crunchbase.com/hub/southeast-asia-startups",
    "https://www.crunchbase.com/hub/bangladesh-companies",
]

DEFAULT_REGIONS = [
    "Bangladesh", "Singapore", "Indonesia", "Malaysia", "Philippines",
    "Vietnam", "Thailand", "Myanmar", "Cambodia", "Laos", "Brunei",
    "Timor-Leste",
]

# All Crunchbase-DOM-coupled strings live here. When CB changes their UI,
# this is the only place to edit. Verify against a live page; see --debug-html.
SELECTORS = {
    "login_url": "https://www.crunchbase.com/login",
    "post_login_path_excluded": "/login",
    "hub_company_link": "a[href^='/organization/']",
    "company_name": "h1",
    "company_location_links": "a[href*='/location/']",
    "company_description": "description-card, [class*='description']",
    "funding_rounds_url_suffix": "/company_financials",
    "round_detail_link": "a[href^='/funding_round/']",
    "round_field_money": "field-formatter[type='money']",
    "round_field_date": "field-formatter[type='date']",
    "round_field_enum": "field-formatter[type='enum']",
    "round_lead_investor": "investor-stack[is_lead='true'] a, "
                          "[data-testid='lead-investor'] a",
    "round_investors": "investor-stack a, [data-testid='investor-list'] a",
    "search_funding_rounds_url": "https://www.crunchbase.com/discover/funding_rounds",
}

def parse_args() -> argparse.Namespace: 
    p = argparse.ArgumentParser(
        description="Scrape crunchbase funding rounds into the dataset schema"
    )
    p.add_argument()




