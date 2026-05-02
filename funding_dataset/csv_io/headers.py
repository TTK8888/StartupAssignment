from __future__ import annotations


DATASET_HEADER = [
    "Record ID",
    "Startup",
    "Investor Name",
    "Investment Month & Year",
    "Startup Country",
    "Total Investment Amount",
    "Investment Amount USD",
    "Investment Amount BDT",
    "Investment Type",
    "Lead Investor",
    "Investor Type",
    "Source URL 1",
    "Source URL 2",
    "Notes",
    "Verification Status",
    "needs_review",
]

SOURCE_DOC_HEADER = [
    "Record ID",
    "Startup",
    "Primary Source URL",
    "Primary Source Type",
    "Secondary Source URL",
    "Secondary Source Type",
    "Verification Status",
    "Notes",
]

COMPARISON_HEADER = [
    "Startup",
    "Country",
    "Field Compared",
    "Source A",
    "Value A",
    "Source B",
    "Value B",
    "Difference Type",
    "Resolution",
]

LEADS_HEADER = [
    "captured_at_utc",
    "startup_guess",
    "country_guess",
    "source_type",
    "title",
    "url",
    "published",
    "amount_guess",
    "round_guess",
    "lead_investor_guess",
    "status",
    "notes",
]

CRAWL_LOG_HEADER = ["url", "source", "action", "reason"]

CONFLICTS_HEADER = [
    "match_key",
    "field",
    "existing_value",
    "existing_source_url",
    "new_value",
    "new_source_urls",
    "detected_at",
]
