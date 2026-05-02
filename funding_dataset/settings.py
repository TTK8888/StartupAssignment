"""
Loads YAML config files and exposes them as module-level constants.

Downstream modules import constants directly from here instead of touching YAML.
The plan calls for a "Settings object" — implemented as module constants for
pragmatic ergonomics (every helper function references several patterns and
threading a Settings object through them all is more plumbing than it's worth
for a single-script tool).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import yaml


CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


def _flatten_pattern(raw: str) -> str:
    """
    YAML stores regex patterns as multi-line literal blocks for readability.
    Original code concatenated string literals with no whitespace between them,
    so we strip per-line leading/trailing whitespace and join without separator.
    Spaces inside a line (e.g. "venture capital") are preserved.
    """
    return "".join(line.strip() for line in raw.splitlines())


def _load_yaml(name: str) -> dict:
    with (CONFIG_DIR / name).open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@dataclass(frozen=True)
class SourceConfig:
    name: str
    source_type: str
    allowed_domains: tuple[str, ...]
    seed_urls: tuple[str, ...]
    url_patterns: tuple[str, ...]
    needs_browser: bool = False


# ---- keywords.yaml ----
_keywords = _load_yaml("keywords.yaml")
USER_AGENT: str = _keywords["user_agent"]
CRAWL_KEYWORDS: list[str] = list(_keywords["crawl_keywords"])
TRUSTED_MEDIA_SEARCH_TERMS: list[str] = list(_keywords["trusted_media_search_terms"])
OFFICIAL_SOURCE_SEARCH_TERMS: list[str] = list(_keywords["official_source_search_terms"])
DEFAULT_URL_PATTERNS: list[str] = list(_keywords["default_url_patterns"])

# ---- patterns.yaml ----
_patterns = _load_yaml("patterns.yaml")
SKIP_URL_PATTERN = re.compile(_flatten_pattern(_patterns["skip_url_pattern"]), re.IGNORECASE)
FUNDING_PATTERN = re.compile(_flatten_pattern(_patterns["funding_pattern"]), re.IGNORECASE)
DEAL_ACTION_PATTERN = re.compile(_flatten_pattern(_patterns["deal_action_pattern"]), re.IGNORECASE)
ROUND_OR_AMOUNT_PATTERN = re.compile(_flatten_pattern(_patterns["round_or_amount_pattern"]), re.IGNORECASE)
DEAL_TITLE_PATTERN = re.compile(_flatten_pattern(_patterns["deal_title_pattern"]), re.IGNORECASE)
ROUNDUP_TITLE_PATTERN = re.compile(_flatten_pattern(_patterns["roundup_title_pattern"]), re.IGNORECASE)
NEGATIVE_TOPIC_PATTERN = re.compile(_flatten_pattern(_patterns["negative_topic_pattern"]), re.IGNORECASE)
AMOUNT_PATTERN = re.compile(_flatten_pattern(_patterns["amount_pattern"]), re.IGNORECASE)
NON_TARGET_COUNTRY_PATTERN = re.compile(_flatten_pattern(_patterns["non_target_country_pattern"]), re.IGNORECASE)

ROUND_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(entry["pattern"], re.IGNORECASE), entry["label"])
    for entry in _patterns["round_patterns"]
]

# ---- countries.yaml ----
_countries = _load_yaml("countries.yaml")
COUNTRY_TERMS: dict[str, list[str]] = {k: list(v) for k, v in _countries["country_terms"].items()}
COUNTRY_HINT_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(term) for terms in COUNTRY_TERMS.values() for term in terms) + r")\b",
    re.IGNORECASE,
)

# ---- currency.yaml ----
_currency = _load_yaml("currency.yaml")
USD_TO_BDT_BY_YEAR: dict[int, Decimal] = {
    int(year): Decimal(rate) for year, rate in _currency["usd_to_bdt_by_year"].items()
}
NON_USD_TO_USD_RATE: dict[str, Decimal] = {
    code: Decimal(rate) for code, rate in _currency["non_usd_to_usd_rate"].items()
}

# ---- sources.yaml ----
_sources = _load_yaml("sources.yaml")
DISALLOWED_SOURCE_TYPE: str = _sources["disallowed_source_type"]
ALLOWED_TRUSTED_MEDIA_DOMAINS: set[str] = set(_sources["allowed_trusted_media_domains"])
ALLOWED_STARTUP_DATABASE_DOMAINS: set[str] = set(_sources["allowed_startup_database_domains"])
PAYWALLED_HEADLINE_ONLY_DOMAINS: set[str] = set(_sources["paywalled_headline_only_domains"])
FOLLOWABLE_OUTBOUND_DOMAINS: set[str] = ALLOWED_TRUSTED_MEDIA_DOMAINS | set(_sources["followable_outbound_extra"])

SOURCE_TYPE_BY_DOMAIN: dict[str, str] = dict(_sources["source_type_by_domain"])
SOURCE_PRIORITY: dict[str, int] = dict(_sources["source_priority"])
STANDALONE_SOURCE_TYPES: set[str] = set(_sources["standalone_source_types"])

SOURCE_CONFIGS: list[SourceConfig] = [
    SourceConfig(
        name=entry["name"],
        source_type=entry["source_type"],
        allowed_domains=tuple(entry["allowed_domains"]),
        seed_urls=tuple(entry["seed_urls"]),
        url_patterns=tuple(entry.get("url_patterns") or DEFAULT_URL_PATTERNS),
        needs_browser=bool(entry.get("needs_browser", False)),
    )
    for entry in _sources["source_configs"]
]

SITE_SEARCH_TEMPLATES_BY_DOMAIN: dict[str, tuple[str, ...]] = {
    domain: tuple(templates)
    for domain, templates in _sources["site_search_templates_by_domain"].items()
}
