#!/usr/bin/env python3
"""
The script writes all required outputs into a generated directory:
- dataset.csv
- source_documentation.csv
- cross_source_comparison.csv
- funding_leads.csv
- crawl_log.csv
- investment_articles.json
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus, urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup


# identifies the crawler in robots txt and outbound requests
# lets site operators attribute traffic and apply agent rules
USER_AGENT = "Mozilla/5.0 (compatible; FundingResearchBot/1.0; +public-source-verification)"

CRAWL_KEYWORDS = [
    "investment",
    "funding",
    "startup",
    "venture",
    "seed",
    "series",
    "raised",
    "raises",
    "invests",
    "financing",
]

TRUSTED_MEDIA_SEARCH_TERMS = [
    "bangladesh startup raises seed funding",
    "singapore startup raises series a",
    "southeast asia startup secures funding",
    "startup closes funding round",
]

OFFICIAL_SOURCE_SEARCH_TERMS = [
    "startup raises series a",
    "startup secures seed funding",
    "investment announcement",
]

DEFAULT_URL_PATTERNS = [
    "/news/",
    "/article/",
    "/startup",
    "/startups",
    "/business",
    "/technology",
    "/tech-startup",
    "/funding",
    "/press",
    "/portfolio",
    "/tag/funding",
]

# drops auth pages boilerplate author pages and binary assets before fetching
# keeps request budget for pages that can contain funding text
SKIP_URL_PATTERN = re.compile(
    r"("
    r"/login|/signin|/sign-in|/signup|/register|/account|/profile|/user|"
    r"/author/|/authors/|/advertis|"
    r"/privacy|/terms|/contact|/about|/subscribe|/newsletter|/events|"
    # investor site portfolio listings are usually company profiles not deals
    r"/portfolio/[a-z0-9-]+/?$|/portfolio/$|/companies/[a-z0-9-]+/?$|"
    # opinion paths are not specific deal coverage
    r"/partner-insights/|/perspectives/|/op-?eds?/|/opinion/|"
    r"\.(?:jpg|jpeg|png|gif|webp|svg|pdf|zip|mp4|mp3|css|js)$"
    r")",
    re.IGNORECASE,
)

FUNDING_PATTERN = re.compile(
    r"\b("
    r"raise|raises|raised|raising|funding|funded|investment|invests|invested|"
    r"financing|venture capital|pre-seed|seed|series\s+[a-z]|pre-series|"
    r"lead investor|led by|backed by"
    r")\b",
    re.IGNORECASE,
)

DEAL_ACTION_PATTERN = re.compile(
    r"\b("
    r"raise|raises|raised|raising|"
    r"secure|secures|secured|"
    r"close|closes|closed|"
    r"bag|bags|bagged|"
    r"land|lands|landed|"
    r"invests|invested|backs|backed|led by|co-led by"
    r")\b",
    re.IGNORECASE,
)

ROUND_OR_AMOUNT_PATTERN = re.compile(
    r"\b("
    r"pre[-\s]?seed|seed|pre[-\s]?series\s+[a-z]|series\s+[a-z]|"
    r"bridge round|strategic investment|debt financing|funding round"
    r")\b|"
    r"(?<!\w)(?:US\$|USD|BDT|Tk|SGD|S\$|MYR|RM|PHP|IDR|THB|VND|\$)\s?\d+(?:[.,]\d+)?",
    re.IGNORECASE,
)

# matches titles that read like one funding announcement
# gates trusted media when no second source exists
DEAL_TITLE_PATTERN = re.compile(
    r"\b(raises?|raised|secures?|secured|lands?|bags?|nets?|nabs?|"
    r"closes?|closed|wraps?|grabs?|hauls?|attracts?|pockets?|scores?|"
    r"banks?|backs?|invests?\s+(?:in|\$)|leads?\s+\$|gets?\s+\$)\b",
    re.IGNORECASE,
)
# rejects roundup titles even when a dollar amount appears
ROUNDUP_TITLE_PATTERN = re.compile(
    r"\b(ecosystem|raised so far|year in review|outlook|industry report|"
    r"market report|state of|decline|drop|plunge|year[-\s]end|decade|"
    r"top \d+|wraps? up|annual)\b",
    re.IGNORECASE,
)

NEGATIVE_TOPIC_PATTERN = re.compile(
    r"\b("
    r"organisation news|town hall|bank asia|unionpay|debit card|credit card|"
    r"launch(?:es|ed)?|unveil(?:s|ed)?|introduc(?:e|es|ed)|"
    r"policy|regulation|guideline|report|review|opinion|how to|"
    r"earnings|profit|revenue|inflation|market outlook|"
    r"electric vehicle concept|hypercar|playstation|chatbot|gemini|"
    r"appointment|appointed|chief executive|ceo interview|webinar|summit"
    r")\b",
    re.IGNORECASE,
)

ROUND_PATTERNS = [
    (re.compile(r"\bpre[-\s]?seed\b", re.IGNORECASE), "Pre-Seed"),
    (re.compile(r"\bseed\b", re.IGNORECASE), "Seed"),
    (re.compile(r"\bpre[-\s]?series\s+a\b", re.IGNORECASE), "Seed/Bridge"),
    (re.compile(r"\bseries\s+a\b", re.IGNORECASE), "Series A"),
    (re.compile(r"\bseries\s+b\b", re.IGNORECASE), "Series B"),
    (re.compile(r"\bseries\s+c\b", re.IGNORECASE), "Series C"),
    (re.compile(r"\bseries\s+d\b", re.IGNORECASE), "Series D"),
    (re.compile(r"\bstrategic investment\b", re.IGNORECASE), "Strategic Investment"),
    (re.compile(r"\bdebt financing\b", re.IGNORECASE), "Debt Financing"),
    (re.compile(r"\bbridge round\b", re.IGNORECASE), "Bridge Round"),
]

AMOUNT_PATTERN = re.compile(
    r"(?<!\w)(?:"
    r"(?:US\$|USD|BDT|Tk|SGD|S\$|MYR|RM|PHP|IDR|THB|VND|\$)\s?"
    r"\d+(?:[.,]\d+)?\s?(?:million|billion|crore|lakh|mn|m|b)?"
    r"|"
    r"\d+(?:[.,]\d+)?\s?(?:million|billion|crore|lakh|mn|m|b)\s?"
    r"(?:US dollars|dollars|USD|BDT|taka|baht|dong|rupiah|pesos|ringgit)"
    r")(?!\w)",
    re.IGNORECASE,
)

COUNTRY_TERMS = {
    "Bangladesh": ["bangladesh", "dhaka", ".bd"],
    "Singapore": ["singapore"],
    "Indonesia": ["indonesia", "jakarta"],
    "Malaysia": ["malaysia", "kuala lumpur"],
    "Philippines": ["philippines", "manila"],
    "Vietnam": ["vietnam", "viet nam", "ho chi minh", "hanoi"],
    "Thailand": ["thailand", "bangkok"],
    "Myanmar": ["myanmar"],
    "Cambodia": ["cambodia"],
    "Laos": ["laos"],
    "Brunei": ["brunei"],
    "Timor-Leste": ["timor-leste", "timor leste"],
}

COUNTRY_HINT_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(term) for terms in COUNTRY_TERMS.values() for term in terms) + r")\b",
    re.IGNORECASE,
)

# rejects headlines or urls that name countries outside bangladesh and sea
# avoids footer and nav text making unrelated stories look local
NON_TARGET_COUNTRY_PATTERN = re.compile(
    r"\b(?:"
    r"australia|australian|"
    r"united states|u\.?s\.?a?|america|american|silicon valley|"
    r"united kingdom|u\.?k\.?|britain|british|england|london|"
    r"china|chinese|beijing|shanghai|shenzhen|hong kong|"
    r"japan|japanese|tokyo|"
    r"korea|korean|seoul|"
    r"taiwan|taipei|"
    r"europe|european|germany|german|berlin|france|french|paris|"
    r"canada|canadian|toronto|"
    r"israel|israeli|tel aviv|"
    r"uae|dubai|saudi|riyadh|qatar|doha|"
    r"africa|nigeria|kenya|egypt|south africa|"
    r"brazil|brazilian|mexico|argentina|colombia|chile|"
# global brands that often appear in local or sea outlets
    r"anthropic|openai|mistral|deepmind|nvidia|"
    r"stripe|databricks|snowflake|palantir|"
    r"spacex|neuralink|rivian|waymo"
    r")\b",
    re.IGNORECASE,
)

DISALLOWED_SOURCE_TYPE = "Disallowed source"

ALLOWED_TRUSTED_MEDIA_DOMAINS = {
    "techinasia.com",
    "dealstreetasia.com",
    "techcrunch.com",
    "tbsnews.net",
    "futurestartup.com",
    "thedailystar.net",
    "prothomalo.com",
}

ALLOWED_STARTUP_DATABASE_DOMAINS = {
    "crunchbase.com",
    "tracxn.com",
    "pitchbook.com",
    "cbinsights.com",
}

PAYWALLED_HEADLINE_ONLY_DOMAINS = {
    "dealstreetasia.com",
}

# outbound domains worth following one hop from a parent article
# keeps the crawler away from ads social links and unrelated press
FOLLOWABLE_OUTBOUND_DOMAINS = ALLOWED_TRUSTED_MEDIA_DOMAINS | {
    "prnewswire.com",
    "businesswire.com",
    "globenewswire.com",
}

SOURCE_TYPE_BY_DOMAIN = {
    "startupbangladesh.vc": "Official startup/investor site",
    "pathao.com": "Official startup/investor site",
    "wavemaker.vc": "Official startup/investor site",
    "openspace.vc": "Official startup/investor site",
    "jungle-ventures.com": "Official startup/investor site",
    "insignia.vc": "Official startup/investor site",
    "insigniaventures.com": "Official startup/investor site",
    "vertexventures.sg": "Official startup/investor site",
    "vertexholdings.com": "Official startup/investor site",
    "goldengate.vc": "Official startup/investor site",
    "monkshill.com": "Official startup/investor site",
    "east.vc": "Official startup/investor site",
    "eastventures.com": "Official startup/investor site",
    "peakxv.com": "Official startup/investor site",
    "acv.vc": "Official startup/investor site",
    "prnewswire.com": "Company press release",
    "businesswire.com": "Company press release",
    "globenewswire.com": "Company press release",
    "techinasia.com": "Trusted media",
    "dealstreetasia.com": "Trusted media",
    "techcrunch.com": "Trusted media",
    "thedailystar.net": "Trusted media",
    "tbsnews.net": "Trusted media",
    "futurestartup.com": "Trusted media",
    "prothomalo.com": "Trusted media",
    "crunchbase.com": "Startup database",
    "tracxn.com": "Startup database",
    "pitchbook.com": "Startup database",
    "cbinsights.com": "Startup database",
}

# lower number wins when choosing the primary source
# official and press release sources outrank aggregators and media
SOURCE_PRIORITY = {
    "Official startup/investor site": 1,
    "Company press release": 2,
    "LinkedIn announcement": 3,
    "Trusted media": 4,
    "Startup database": 5,
    DISALLOWED_SOURCE_TYPE: 99,
}

# source tiers accepted with one url when no second source exists
# trusted media uses its own completeness gate
STANDALONE_SOURCE_TYPES = {
    "Official startup/investor site",
    "Company press release",
}


@dataclass(frozen=True)
class SourceConfig:
    name: str
    source_type: str
    allowed_domains: tuple[str, ...]
    seed_urls: tuple[str, ...]
    url_patterns: tuple[str, ...] = tuple(DEFAULT_URL_PATTERNS)
    # true for bot challenged or js rendered sources that need a browser
    needs_browser: bool = False


SOURCE_CONFIGS = [
    SourceConfig(
        name="Startup Bangladesh",
        source_type="Official startup/investor site",
        allowed_domains=("startupbangladesh.vc",),
        seed_urls=(
            "https://www.startupbangladesh.vc/news/",
            "https://www.startupbangladesh.vc/portfolio/",
        ),
        url_patterns=("/news/", "/portfolio/", "/startup-bangladesh-invests"),
    ),
    SourceConfig(
        name="PR Newswire venture capital",
        source_type="Company press release",
        allowed_domains=("prnewswire.com",),
        seed_urls=(
            "https://www.prnewswire.com/rss/venture-capital-latest-news/venture-capital-latest-news-list.rss",
        ),
        url_patterns=("/news-releases/", "/rss/"),
    ),
    SourceConfig(
        name="Business Wire",
        source_type="Company press release",
        allowed_domains=("businesswire.com",),
        seed_urls=("https://www.businesswire.com/newsroom",),
        url_patterns=("/news/", "/newsroom"),
    ),
    SourceConfig(
        name="Tech in Asia",
        source_type="Trusted media",
        allowed_domains=("techinasia.com",),
        seed_urls=(
            "https://www.techinasia.com/tag/funding/feed",
            "https://www.techinasia.com/category/investments/feed",
            "https://www.techinasia.com/tag/startups/feed",
            "https://www.techinasia.com/tag/singapore",
            "https://www.techinasia.com/tag/indonesia",
            "https://www.techinasia.com/tag/vietnam",
            "https://www.techinasia.com/tag/malaysia",
            "https://www.techinasia.com/tag/philippines",
            "https://www.techinasia.com/tag/thailand",
        ),
        url_patterns=("/news/", "/"),
        needs_browser=True,
    ),
    SourceConfig(
        name="DealStreetAsia",
        source_type="Trusted media",
        allowed_domains=("dealstreetasia.com",),
        seed_urls=("https://www.dealstreetasia.com/section/startups",),
        url_patterns=("/stories/", "/section/startups", "/deals/"),
    ),
    SourceConfig(
        name="TechCrunch startups",
        source_type="Trusted media",
        allowed_domains=("techcrunch.com",),
        seed_urls=("https://techcrunch.com/category/startups/",),
        url_patterns=("/category/startups/", "/20"),
    ),
    SourceConfig(
        name="The Business Standard startups",
        source_type="Trusted media",
        allowed_domains=("tbsnews.net",),
        seed_urls=(
            "https://www.tbsnews.net/economy/corporates",
            "https://www.tbsnews.net/tags/startup",
        ),
        url_patterns=("/economy/", "/bangladesh/", "/tags/startup"),
    ),
    SourceConfig(
        name="Future Startup",
        source_type="Trusted media",
        allowed_domains=("futurestartup.com",),
        seed_urls=("https://futurestartup.com/tag/funding/",),
        url_patterns=("/tag/funding/", "/20"),
    ),
    SourceConfig(
        name="The Daily Star business and startup",
        source_type="Trusted media",
        allowed_domains=("thedailystar.net",),
        seed_urls=(
            "https://www.thedailystar.net/business/rss.xml",
            "https://www.thedailystar.net/tech-startup/rss.xml",
            "https://www.thedailystar.net/business",
            "https://www.thedailystar.net/tech-startup",
        ),
        url_patterns=("/business/", "/tech-startup/", "/rss.xml"),
    ),
    SourceConfig(
        name="Prothom Alo business",
        source_type="Trusted media",
        allowed_domains=("prothomalo.com",),
        seed_urls=("https://en.prothomalo.com/business",),
        url_patterns=("/business/", "/technology/"),
    ),
    SourceConfig(
        name="Crunchbase",
        source_type="Startup database",
        allowed_domains=("crunchbase.com",),
        seed_urls=("https://www.crunchbase.com/hub/southeast-asia-startups",),
        url_patterns=("/organization/", "/hub/"),
    ),
    SourceConfig(
        name="Tracxn",
        source_type="Startup database",
        allowed_domains=("tracxn.com",),
        seed_urls=("https://tracxn.com/explore/Startups-in-Southeast-Asia",),
        url_patterns=("/explore/", "/d/companies/"),
    ),
    SourceConfig(
        name="CB Insights",
        source_type="Startup database",
        allowed_domains=("cbinsights.com",),
        seed_urls=("https://www.cbinsights.com/research/startup-funding/",),
        url_patterns=("/research/", "/company/"),
    ),
    SourceConfig(
        name="PitchBook",
        source_type="Startup database",
        allowed_domains=("pitchbook.com",),
        seed_urls=("https://pitchbook.com/news",),
        url_patterns=("/news/", "/profiles/"),
    ),
    SourceConfig(
        name="Wavemaker Partners",
        source_type="Official startup/investor site",
        allowed_domains=("wavemaker.vc",),
        seed_urls=(
            "https://wavemaker.vc/feed",
            "https://wavemaker.vc/category/wavemaker-category-news-news/",
        ),
        url_patterns=("/", "/feed"),
    ),
    SourceConfig(
        name="Openspace Ventures",
        source_type="Official startup/investor site",
        allowed_domains=("openspace.vc",),
        seed_urls=("https://openspace.vc/news/",),
        url_patterns=("/news/", "/"),
    ),
    SourceConfig(
        name="Jungle Ventures",
        source_type="Official startup/investor site",
        allowed_domains=("jungle-ventures.com",),
        seed_urls=(
            "https://jungle-ventures.com/news/",
            "https://jungle-ventures.com/feed/",
        ),
        url_patterns=("/news/", "/feed", "/"),
    ),
    SourceConfig(
        name="Insignia Ventures",
        source_type="Official startup/investor site",
        allowed_domains=("insignia.vc", "insigniaventures.com"),
        seed_urls=(
            "https://insignia.vc/news/",
            "https://insignia.vc/feed/",
        ),
        url_patterns=("/news/", "/feed", "/"),
    ),
    SourceConfig(
        name="Vertex Ventures SEA",
        source_type="Official startup/investor site",
        allowed_domains=("vertexventures.sg", "vertexholdings.com"),
        seed_urls=(
            "https://www.vertexventures.sg/news",
            "https://www.vertexholdings.com/news",
        ),
        url_patterns=("/news", "/insights", "/"),
    ),
    SourceConfig(
        name="Golden Gate Ventures",
        source_type="Official startup/investor site",
        allowed_domains=("goldengate.vc",),
        seed_urls=(
            "https://goldengate.vc/news",
            "https://goldengate.vc/feed",
        ),
        url_patterns=("/news", "/feed", "/"),
    ),
    SourceConfig(
        name="Monks Hill Ventures",
        source_type="Official startup/investor site",
        allowed_domains=("monkshill.com",),
        seed_urls=(
            "https://www.monkshill.com/news",
            "https://www.monkshill.com/insights",
        ),
        url_patterns=("/news", "/insights", "/"),
    ),
    SourceConfig(
        name="East Ventures",
        source_type="Official startup/investor site",
        allowed_domains=("east.vc", "eastventures.com"),
        seed_urls=(
            "https://east.vc/news",
            "https://east.vc/insights",
        ),
        url_patterns=("/news", "/insights", "/"),
    ),
    SourceConfig(
        name="Peak XV Partners",
        source_type="Official startup/investor site",
        allowed_domains=("peakxv.com",),
        seed_urls=(
            "https://www.peakxv.com/newsroom/",
            "https://www.peakxv.com/insights/",
        ),
        url_patterns=("/newsroom", "/insights", "/"),
    ),
    SourceConfig(
        name="AC Ventures",
        source_type="Official startup/investor site",
        allowed_domains=("acv.vc",),
        seed_urls=(
            "https://acv.vc/insights/",
            "https://acv.vc/portfolio/",
        ),
        url_patterns=("/insights", "/portfolio", "/"),
    ),
]

SITE_SEARCH_TEMPLATES_BY_DOMAIN = {
    "techinasia.com": (
        "https://www.techinasia.com/search?query={query}",
    ),
    "dealstreetasia.com": (
        "https://www.dealstreetasia.com/?s={query}",
    ),
    "techcrunch.com": (
        "https://techcrunch.com/search/{query}",
    ),
    "tbsnews.net": (
        "https://www.tbsnews.net/search/google?search={query}",
    ),
    "futurestartup.com": (
        "https://futurestartup.com/?s={query}",
    ),
    "thedailystar.net": (
        "https://www.thedailystar.net/search?search_api_fulltext={query}",
    ),
    "prothomalo.com": (
        "https://en.prothomalo.com/search?query={query}",
    ),
}

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


@dataclass
class Lead:
    title: str
    url: str
    published: str
    source_url: str
    matched_keywords: list[str] = field(default_factory=list)


@dataclass
class ArticleRecord:
    title: str
    date: str
    source: str
    url: str
    content: str
    keywords: list[str]


@dataclass
class CrawlLogEntry:
    url: str
    source: str
    action: str
    reason: str


@dataclass
class CrawlResult:
    leads: list[Lead]
    articles: list[ArticleRecord]
    log_entries: list[CrawlLogEntry]


@dataclass
class Evidence:
    title: str
    url: str
    source_type: str
    published: str
    startup: str
    country: str
    amount: str
    round_type: str
    investor_names: str
    lead_investor: str
    investor_type: str
    notes: list[str] = field(default_factory=list)
    headline_only: bool = False
    # links followed from a parent article merge back into the parent bundle
    parent_lead_key: str = ""


@dataclass
class RecordBundle:
    record_id: str
    startup: str
    country: str
    sources: list[Evidence]
    issues: list[str]


def fetch(url: str, session: requests.Session) -> str:
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            response = session.get(
                url,
                timeout=30,
                headers={"User-Agent": USER_AGENT},
            )
            if response.status_code >= 500:
                last_error = requests.HTTPError(f"{response.status_code} for {url}")
                if attempt == 0:
                    time.sleep(2)
                    continue
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            if attempt == 0:
                time.sleep(2)
                continue
            raise
    raise last_error if last_error else requests.RequestException(f"fetch failed: {url}")


# lazy playwright state for sources that need rendered html
_BROWSER_STATE: dict = {"playwright": None, "browser": None, "context": None}


def _get_browser_context():
    if _BROWSER_STATE["context"] is not None:
        return _BROWSER_STATE["context"]
    from playwright.sync_api import sync_playwright

    pw = sync_playwright().start()
    browser = pw.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled"],
    )
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 800},
    )
    _BROWSER_STATE.update(playwright=pw, browser=browser, context=context)
    return context


def shutdown_browser() -> None:
    if _BROWSER_STATE["browser"] is not None:
        try:
            _BROWSER_STATE["browser"].close()
        except Exception:
            pass
    if _BROWSER_STATE["playwright"] is not None:
        try:
            _BROWSER_STATE["playwright"].stop()
        except Exception:
            pass
    _BROWSER_STATE.update(playwright=None, browser=None, context=None)


def fetch_with_browser(url: str) -> str:
    context = _get_browser_context()
    page = context.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        # allow challenge pages and spa hydration to settle
        page.wait_for_timeout(5000)
        return page.content()
    finally:
        page.close()


def fetch_html(
    url: str,
    session: requests.Session,
    source: SourceConfig | None,
) -> str:
    if source is not None and source.needs_browser:
        return fetch_with_browser(url)
    return fetch(url, session)


def normalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    clean = parsed._replace(fragment="", query="")
    return urlunparse(clean)


def domain_for(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        return host[4:]
    return host


def canonical_source_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme.lower() or "https", domain_for(url), parsed.path.rstrip("/"), "", "", ""))


def source_type_for_domain(domain: str) -> str:
    if any(domain == allowed or domain.endswith(f".{allowed}") for allowed in ALLOWED_TRUSTED_MEDIA_DOMAINS):
        return "Trusted media"
    if any(domain == allowed or domain.endswith(f".{allowed}") for allowed in ALLOWED_STARTUP_DATABASE_DOMAINS):
        return "Startup database"
    for source in SOURCE_CONFIGS:
        if any(domain == allowed or domain.endswith(f".{allowed}") for allowed in source.allowed_domains):
            return source.source_type
    if domain == "linkedin.com" or domain.endswith(".linkedin.com"):
        return "LinkedIn announcement"
    for known_domain, source_type in SOURCE_TYPE_BY_DOMAIN.items():
        if domain == known_domain or domain.endswith(f".{known_domain}"):
            return source_type
    return DISALLOWED_SOURCE_TYPE


def classify_source(url: str) -> str:
    return source_type_for_domain(domain_for(url))


def is_allowed_source_type(source_type: str) -> bool:
    return source_type in {
        "Official startup/investor site",
        "Company press release",
        "LinkedIn announcement",
        "Trusted media",
        "Startup database",
    }


def source_for_url(url: str, sources: list[SourceConfig]) -> SourceConfig | None:
    domain = domain_for(url)
    for source in sources:
        if any(domain == allowed or domain.endswith(f".{allowed}") for allowed in source.allowed_domains):
            return source
    return None


def source_search_urls(source: SourceConfig) -> list[str]:
    domain = source.allowed_domains[0]
    if source.source_type == "Trusted media":
        terms = TRUSTED_MEDIA_SEARCH_TERMS
    elif source.source_type == "Official startup/investor site":
        terms = OFFICIAL_SOURCE_SEARCH_TERMS
    else:
        return []
    templates = SITE_SEARCH_TEMPLATES_BY_DOMAIN.get(
        domain,
        (
            f"https://{domain}/?s={{query}}",
            f"https://{domain}/search?q={{query}}",
            f"https://{domain}/search?query={{query}}",
        ),
    )
    urls: list[str] = []
    for template in templates:
        for term in terms:
            urls.append(template.format(query=quote_plus(term)))
    return urls


def is_same_allowed_domain(url: str, source: SourceConfig) -> bool:
    domain = domain_for(url)
    return any(domain == allowed or domain.endswith(f".{allowed}") for allowed in source.allowed_domains)


def is_probably_feed(url: str, html_text: str) -> bool:
    path = urlparse(url).path.lower()
    stripped = html_text.lstrip()[:120].lower()
    return path.endswith((".rss", ".xml")) or "/feed" in path or stripped.startswith(("<?xml", "<rss", "<feed"))


def should_skip_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return "Unsupported URL scheme"
    if SKIP_URL_PATTERN.search(url):
        return "Irrelevant URL pattern"
    return ""


def matched_keywords(*values: str) -> list[str]:
    blob = " ".join(values).lower()
    return [keyword for keyword in CRAWL_KEYWORDS if re.search(rf"\b{re.escape(keyword)}\b", blob)]


def url_matches_source_pattern(url: str, source: SourceConfig) -> bool:
    path = urlparse(url).path.lower()
    return any(pattern.lower() in path for pattern in source.url_patterns)


def is_listing_url(url: str) -> bool:
    path = urlparse(url).path.strip("/").lower()
    listing_paths = {
        "",
        "news",
        "newsroom",
        "portfolio",
        "business",
        "tech-startup",
        "category/startups",
        "tag/funding",
        "tags/startup",
        "section/startups",
        "research/startup-funding",
    }
    if path in listing_paths:
        return True
    if re.fullmatch(r"(tag|tags|category|section)/[^/]+", path):
        return True
    if re.search(r"(^|/)page/\d+/?$", path):
        return True
    if re.fullmatch(r"portfolio/page/\d+", path):
        return True
    return False


def looks_like_article_url(url: str) -> bool:
    if is_listing_url(url):
        return False
    path = urlparse(url).path.strip("/")
    segments = [segment for segment in path.split("/") if segment]
    if re.search(r"/20\d{2}/\d{2}/", f"/{path}/"):
        return True
    if len(segments) >= 3:
        return True
    if segments and len(segments[-1]) >= 35 and FUNDING_PATTERN.search(segments[-1].replace("-", " ")):
        return True
    return False


def link_score(url: str, text: str, source: SourceConfig) -> int:
    # ranks links so bounded crawling prefers funding articles over menus
    score = 0
    if url_matches_source_pattern(url, source):
        score += 3
    score += len(matched_keywords(url, text)) * 2
    if AMOUNT_PATTERN.search(f"{url} {text}"):
        score += 2
    if FUNDING_PATTERN.search(f"{url} {text}"):
        score += 2
    return score


def is_valid_article(url: str, title: str, content: str, source: SourceConfig) -> bool:
    # scans the article lead because funding terms usually appear early
    if not looks_like_article_url(url):
        return False
    if not url_matches_source_pattern(url, source):
        return False
    if not matched_keywords(title, url, content[:3000]):
        return False
    if not (
        FUNDING_PATTERN.search(f"{title} {content[:3000]}")
        or AMOUNT_PATTERN.search(content[:3000])
    ):
        return False
    return is_strict_funding_announcement(title, url, content, source)


def article_record_from_page(
    url: str,
    html_text: str,
    source: SourceConfig,
    fallback_title: str = "",
    fallback_date: str = "",
) -> ArticleRecord:
    title, published, content = page_fields_from_html(html_text)
    title = title or fallback_title
    published = published or fallback_date
    return ArticleRecord(
        title=title,
        date=published,
        source=source.name,
        url=url,
        content=content,
        keywords=matched_keywords(title, url, content),
    )


def robot_parser_for(
    source: SourceConfig,
    session: requests.Session,
    robots_cache: dict[str, RobotFileParser],
) -> RobotFileParser:
    domain = source.allowed_domains[0]
    if domain in robots_cache:
        return robots_cache[domain]
    robots_url = f"https://{domain}/robots.txt"
    parser = RobotFileParser()
    parser.set_url(robots_url)
    try:
        response = session.get(robots_url, timeout=10, headers={"User-Agent": USER_AGENT})
        if response.ok:
            parser.parse(response.text.splitlines())
        else:
            parser.parse([])
    except requests.RequestException:
        parser.parse([])
    robots_cache[domain] = parser
    return parser


def can_fetch_url(
    url: str,
    source: SourceConfig,
    session: requests.Session,
    robots_cache: dict[str, RobotFileParser],
) -> bool:
    parser = robot_parser_for(source, session, robots_cache)
    return parser.can_fetch(USER_AGENT, url)


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def page_fields_from_html(html_text: str) -> tuple[str, str, str]:
    soup = BeautifulSoup(html_text, "html.parser")

    title = ""
    candidates = [
        soup.find("meta", property="og:title"),
        soup.find("meta", attrs={"name": "twitter:title"}),
    ]
    for candidate in candidates:
        content = candidate.get("content") if candidate else ""
        if content:
            title = clean_text(content)
            break
    if not title:
        if soup.title and soup.title.text:
            title = clean_text(soup.title.text)
        else:
            h1 = soup.find("h1")
            title = clean_text(h1.text) if h1 else ""

    published = ""
    selectors = [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"name": "pubdate"}),
        ("meta", {"name": "publish-date"}),
        ("meta", {"name": "date"}),
    ]
    for name, attrs in selectors:
        tag = soup.find(name, attrs=attrs)
        content = tag.get("content") if tag else ""
        if content:
            published = content.strip()
            break
    if not published:
        time_tag = soup.find("time")
        if time_tag:
            published = (time_tag.get("datetime") or time_tag.text or "").strip()

    for tag in soup(["script", "style", "noscript", "nav", "footer", "aside", "header"]):
        tag.decompose()
    text = clean_text(soup.get_text(" "))
    return title, published, text


def text_from_html(html_text: str) -> str:
    _, _, text = page_fields_from_html(html_text)
    return text


def outbound_links_from_html(html_text: str, base_url: str, max_links: int = 3) -> list[str]:
    # follows only known funding outlets linked from another article
    if not html_text:
        return []
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup(["script", "style", "noscript", "nav", "footer", "aside", "header"]):
        tag.decompose()
    base_domain = domain_for(base_url)
    links: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        absolute = normalize_url(urljoin(base_url, href))
        if absolute in seen:
            continue
        seen.add(absolute)
        target_domain = domain_for(absolute)
        if not target_domain or target_domain == base_domain:
            continue
        if not any(
            target_domain == d or target_domain.endswith(f".{d}")
            for d in FOLLOWABLE_OUTBOUND_DOMAINS
        ):
            continue
        if should_skip_url(absolute):
            continue
        links.append(absolute)
        if len(links) >= max_links:
            break
    return links


def title_from_url_slug(url: str) -> str:
    # converts url slugs into readable headlines when html has no article title
    path = urlparse(url).path.strip("/")
    if not path:
        return ""
    last = path.rsplit("/", 1)[-1]
    text = last.replace("-", " ").replace("_", " ").strip()
    if not text or len(text) < 8:
        return ""
    # rejoins decimal amounts split during slug cleanup
    text = re.sub(r"\b(\d{1,3})\s+(\d{1,3})\s*(m|k|bn|cr|million|billion)\b", r"\1.\2 \3", text, flags=re.IGNORECASE)
    text = re.sub(r"\busd?\s+(\d)", r"USD \1", text, flags=re.IGNORECASE)
    parts = text.split()
    out = []
    for p in parts:
        if len(p) <= 4 and p.isupper():
            out.append(p)
        else:
            out.append(p[0].upper() + p[1:] if p else p)
    return " ".join(out)


def _looks_generic_banner(title: str) -> bool:
    if not title:
        return True
    lowered = title.lower()
    banners = [
        "tech in asia",
        "venture capital news",
        "startup ecosystem",
        "connecting asia",
        "wavemaker ventures",
    ]
    return any(b in lowered for b in banners)


def best_article_title(html_title: str, lead_title: str) -> str:
    # prefers feed titles when rendered pages expose only a generic banner
    candidates = [t for t in (lead_title, html_title) if t]
    if not candidates:
        return ""
    if len(candidates) == 1:
        return candidates[0]

    def score(title: str) -> int:
        value = len(title)
        if FUNDING_PATTERN.search(title):
            value += 100
        if COUNTRY_HINT_PATTERN.search(title):
            value += 50
        return value

    return max(candidates, key=score)


def title_from_html(html_text: str) -> str:
    title, _, _ = page_fields_from_html(html_text)
    return title


def published_from_html(html_text: str) -> str:
    _, published, _ = page_fields_from_html(html_text)
    return published


def parse_published_date(value: str) -> date | None:
    # year only dates become january one so year filters can compare them
    if not value:
        return None
    for parser in (
        lambda raw: datetime.fromisoformat(raw.replace("Z", "+00:00")),
        parsedate_to_datetime,
    ):
        try:
            parsed = parser(value)
            if parsed is not None:
                return parsed.date()
        except (TypeError, ValueError, IndexError, OverflowError):
            continue
    iso_match = re.search(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", value)
    if iso_match:
        try:
            return date(int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3)))
        except ValueError:
            pass
    month_year_match = re.search(
        r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})\b",
        value,
        re.IGNORECASE,
    )
    if month_year_match:
        try:
            month_num = datetime.strptime(month_year_match.group(1)[:3].title(), "%b").month
            return date(int(month_year_match.group(2)), month_num, 1)
        except ValueError:
            pass
    year_match = re.search(r"\b(19|20)\d{2}\b", value)
    if year_match:
        try:
            return date(int(year_match.group(0)), 1, 1)
        except ValueError:
            pass
    return None


def parse_date_arg(value: str, *, end_of_period: bool = False) -> date:
    # expands partial to dates to the end of the period
    parts = value.strip().split("-")
    try:
        if len(parts) == 1:
            year = int(parts[0])
            return date(year, 12, 31) if end_of_period else date(year, 1, 1)
        if len(parts) == 2:
            year, month = int(parts[0]), int(parts[1])
            if end_of_period:
                next_month = date(year + (month // 12), (month % 12) + 1, 1)
                return date.fromordinal(next_month.toordinal() - 1)
            return date(year, month, 1)
        if len(parts) == 3:
            return date(int(parts[0]), int(parts[1]), int(parts[2]))
    except ValueError:
        pass
    raise argparse.ArgumentTypeError(
        f"Invalid date {value!r}; expected YYYY, YYYY-MM, or YYYY-MM-DD"
    )


def date_filter_active(args: argparse.Namespace) -> bool:
    return bool(args.year) or bool(args.from_date) or bool(args.to_date)


def make_date_filter(args: argparse.Namespace):
    years = set(args.year or [])
    from_date = parse_date_arg(args.from_date) if args.from_date else None
    to_date = parse_date_arg(args.to_date, end_of_period=True) if args.to_date else None
    if not years and not from_date and not to_date:
        return lambda _published: True
    include_undated = bool(args.include_undated)

    def in_range(published: str) -> bool:
        parsed = parse_published_date(published)
        if parsed is None:
            return include_undated
        if years and parsed.year not in years:
            return False
        if from_date and parsed < from_date:
            return False
        if to_date and parsed > to_date:
            return False
        return True

    return in_range


def month_year(value: str) -> str:
    if not value:
        return "Not Available"
    parsers = [
        lambda raw: datetime.fromisoformat(raw.replace("Z", "+00:00")),
        parsedate_to_datetime,
    ]
    for parser in parsers:
        try:
            parsed = parser(value)
            return parsed.strftime("%b %Y")
        except (TypeError, ValueError, IndexError, OverflowError):
            continue
    match = re.search(
        r"\b("
        r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
        r")\s+\d{4}\b",
        value,
        re.IGNORECASE,
    )
    return match.group(0) if match else "Not Available"


def country_from_text(*values: str) -> str:
    for value in values:
        blob = value.lower()
        for country, terms in COUNTRY_TERMS.items():
            if any(term in blob for term in terms):
                return country
        if re.search(r"\b(southeast asia|south-east asia)\b", blob):
            return "Southeast Asia"
    return "Unknown"


def amount_from_text(text: str) -> str:
    if re.search(r"\bundisclosed\b", text, re.IGNORECASE):
        return "Undisclosed"
    match = AMOUNT_PATTERN.search(text)
    return clean_text(match.group(0)) if match else "Undisclosed"


USD_TO_BDT_BY_YEAR = {
    2020: Decimal("84.8"),
    2021: Decimal("85.1"),
    2022: Decimal("94.0"),
    2023: Decimal("108.8"),
    2024: Decimal("117.0"),
    2025: Decimal("122.0"),
    2026: Decimal("122.0"),
}

NON_USD_TO_USD_RATE = {
    "SGD": Decimal("0.74"),
    "MYR": Decimal("0.21"),
    "PHP": Decimal("0.017"),
    "IDR": Decimal("0.000061"),
    "THB": Decimal("0.027"),
    "VND": Decimal("0.000039"),
}


def usd_to_bdt_rate_for_year(year: int | None) -> Decimal:
    if year in USD_TO_BDT_BY_YEAR:
        return USD_TO_BDT_BY_YEAR[year]
    if year is None:
        return USD_TO_BDT_BY_YEAR[max(USD_TO_BDT_BY_YEAR)]
    nearest_year = min(USD_TO_BDT_BY_YEAR, key=lambda known_year: abs(known_year - year))
    return USD_TO_BDT_BY_YEAR[nearest_year]


def _amount_year(published: str) -> int | None:
    parsed = parse_published_date(published)
    return parsed.year if parsed else None


def _amount_multiplier(raw_amount: str) -> Decimal:
    lowered = raw_amount.lower()
    if re.search(r"(?:\d[\d,.]*\s*(?:billion|bn|b)(?![a-z])|\bbillion\b|\bbn\b)", lowered):
        return Decimal("1000000000")
    if re.search(r"(?:\d[\d,.]*\s*(?:million|mn|m)(?![a-z])|\bmillion\b|\bmn\b)", lowered):
        return Decimal("1000000")
    if re.search(r"\bcrore\b", lowered):
        return Decimal("10000000")
    if re.search(r"\blakh\b", lowered):
        return Decimal("100000")
    return Decimal("1")


def _amount_currency(raw_amount: str) -> str:
    lowered = raw_amount.lower()
    if re.search(r"(?:us\$|\busd\b|\$|\bdollars?\b)", lowered):
        return "USD"
    if re.search(r"(?:\bsgd\b|(?<!u)s\$)", lowered):
        return "SGD"
    if re.search(r"(?:\bmyr\b|\brm\b)", lowered):
        return "MYR"
    if re.search(r"\bphp\b", lowered):
        return "PHP"
    if re.search(r"\bidr\b", lowered):
        return "IDR"
    if re.search(r"\bthb\b", lowered):
        return "THB"
    if re.search(r"\bvnd\b", lowered):
        return "VND"
    if re.search(r"(?:\bbdt\b|\btk(?=\d|\b)|\btaka\b)", lowered):
        return "BDT"
    return ""


def _amount_number(raw_amount: str) -> Decimal | None:
    match = re.search(r"\d[\d,]*(?:\.\d+)?|\d+(?:,\d+)?", raw_amount)
    if not match:
        return None
    value = match.group(0)
    if "," in value and "." not in value:
        groups = value.split(",")
        if len(groups) > 1 and all(len(group) == 3 for group in groups[1:]):
            value = "".join(groups)
        elif _amount_multiplier(raw_amount) != Decimal("1"):
            value = value.replace(",", ".")
        else:
            value = value.replace(",", "")
    else:
        value = value.replace(",", "")
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def _format_normalized_amount(value: Decimal, currency: str) -> str:
    rounded = value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return f"{int(rounded):,} {currency}"


def normalized_amount_values(amount: str, published: str) -> tuple[str, str]:
    if not amount or amount == "Undisclosed":
        return "", ""
    currency = _amount_currency(amount)
    number = _amount_number(amount)
    if not currency or number is None:
        return "", ""

    raw_value = number * _amount_multiplier(amount)
    rate = usd_to_bdt_rate_for_year(_amount_year(published))
    if currency == "USD":
        usd_value = raw_value
        bdt_value = raw_value * rate
    elif currency == "BDT":
        bdt_value = raw_value
        usd_value = raw_value / rate
    elif currency in NON_USD_TO_USD_RATE:
        usd_value = raw_value * NON_USD_TO_USD_RATE[currency]
        bdt_value = usd_value * rate
    else:
        return "", ""
    return _format_normalized_amount(usd_value, "USD"), _format_normalized_amount(bdt_value, "BDT")


def round_from_text(text: str) -> str:
    for pattern, label in ROUND_PATTERNS:
        if pattern.search(text):
            return label
    if re.search(r"\b(funding round|financing round|investment round)\b", text, re.IGNORECASE):
        return "Funding Round"
    if FUNDING_PATTERN.search(text):
        return "Funding Round"
    return "Not Available"


def lead_investor_from_text(text: str) -> str:
    patterns = [
        r"\bco-led by\s+([^.;:]{2,120}?)(?:,?\s+(?:with|alongside|and participation|participating|joins portfolio|portfolio news|written by|share|read more)|[.;])",
        r"\bled by\s+([^.;:]{2,120}?)(?:,?\s+(?:with|alongside|and participation|participating|joins portfolio|portfolio news|written by|share|read more)|[.;])",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            candidate = clean_entity_list(match.group(1))
            if 2 <= len(candidate) <= 200 and not _looks_like_prose(candidate):
                return candidate
    return "Not Stated"


def investors_from_text(text: str, lead_investor: str) -> str:
    # reuses a clear lead investor before trying looser investor patterns
    if lead_investor != "Not Stated":
        return lead_investor
    patterns = [
        r"\b([A-Z][\w'’&.-]+(?:\s+[A-Z][\w'’&.-]+){0,4})\s+backs\s+[^.;:]{2,120}?(?:[.;]|\s+to\s+)",
        r"\b(?:raised|secured|received|closed|raise|raises)\b[^.;:]{0,80}?\bfrom\s+([^.;:]{2,120}?)(?:[.;]|\s+to\s+)",
        r"\b(?:funding|investment|round|capital)\s+from\s+([^.;:]{2,120}?)(?:[.;]|\s+to\s+)",
        r"\bbacked by\s+([^.;:]{2,120}?)(?:[.;])",
        r"\bparticipation from\s+([^.;:]{2,120}?)(?:[.;])",
        r"\b(?:investors|backers)\s+include[ds]?\s+([^.;:]{2,120}?)(?:[.;])",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            candidate = clean_entity_list(match.group(1))
            if 2 <= len(candidate) <= 200 and not _looks_like_prose(candidate):
                return candidate
    return "Not Available"


def _looks_like_prose(value: str) -> bool:
    # rejects sentence fragments that were captured as entity lists
    lowered = f" {value.lower()} "
    prose_markers = [
        " is ", " was ", " are ", " were ", " has ", " have ",
        " will ", " would ", " should ", " can ", " may ", " might ",
        " which ", " that ", " help ", " helps ", " expand ",
        " catalyze ", " scale ", " grow ", " their ", " its ", " on ",
    ]
    if any(marker in lowered for marker in prose_markers):
        return True
    return False


def clean_entity_list(value: str) -> str:
    value = re.split(
        r"\b(?:joins portfolio|portfolio news|written by|share|read more)\b",
        value,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    value = re.sub(r"\s+(?:and|alongside)\s+", "; ", value.strip(), flags=re.IGNORECASE)
    value = value.replace("&", ";")
    value = re.sub(r",\s*", "; ", value)
    value = re.sub(r"\s{2,}", " ", value)
    return value.strip(" ;,-")


def investor_type_from_names(investor_names: str) -> str:
    if investor_names in {"", "Not Available"}:
        return "Not Available"
    lowered = investor_names.lower()
    if any(term in lowered for term in ["angel", "angels"]):
        return "Angel"
    if any(term in lowered for term in ["accelerator", "incubator"]):
        return "Accelerator"
    if any(term in lowered for term in ["family office", "family-office"]):
        return "Family Office"
    if any(term in lowered for term in ["private equity", "buyout", " pe "]):
        return "PE"
    if any(term in lowered for term in ["government", "startup bangladesh", "sovereign"]):
        return "Government VC"
    if any(term in lowered for term in ["corporate", "bank", "telco", "group"]):
        return "Corporate VC"
    if any(term in lowered for term in ["ventures", "venture", "capital", "vc"]):
        return "VC"
    return "Not Available"


_INVESTOR_DEAL_VERBS = {
    "raises", "raised", "secures", "secured", "lands", "landed",
    "bags", "bagged", "closes", "closed", "wraps", "wrapped",
    "grabs", "grabbed", "hauls", "hauled", "attracts", "attracted",
    "pockets", "pocketed", "scores", "scored", "banks", "banked",
    "backs", "backed", "invests", "leads", "led", "gets", "got",
    "nets", "netted", "snags", "snagged", "nabs", "nabbed", "rakes",
    "raked", "completes", "completed", "announces", "announced",
    "receives", "received", "brings", "brought",
}

_DEAL_VERB_ALT = "|".join(sorted(_INVESTOR_DEAL_VERBS))
_DEAL_VERB_PATTERN = re.compile(rf"\b(?:{_DEAL_VERB_ALT})\b", re.IGNORECASE)

# deal verbs where the startup is the subject of the title
_TITLE_SUBJECT_VERBS = _INVESTOR_DEAL_VERBS - {
    "backs", "backed", "invests", "leads", "led",
}
_TITLE_SUBJECT_VERB_ALT = "|".join(sorted(_TITLE_SUBJECT_VERBS))


_SLUG_NOISE_TOKENS = {
    "startup", "startups", "fintech", "edtech", "healthtech", "deeptech",
    "foodtech", "agritech", "proptech", "climatetech", "biotech", "insurtech",
    "health", "tech", "deep", "food", "agri", "prop", "climate", "bio", "insur",
    "ai", "ml",
    "portfolio", "news", "announcement", "company", "firm", "business",
    "platform", "app", "marketplace", "player", "venture", "ventures",
    "based", "backed", "backs", "led",
    "the", "a", "an", "is", "are", "was", "were", "will",
    "has", "have", "had",
    "bangladesh", "bangladeshi", "bangladeshs",
    "singapore", "singaporean", "singapores", "singaporeans",
    "indonesia", "indonesian", "indonesias",
    "malaysia", "malaysian", "malaysias",
    "vietnam", "vietnamese", "vietnams",
    "philippines", "filipino", "philippine",
    "thailand", "thai", "thailands",
    "cambodia", "cambodian",
    "laos", "myanmar",
    "southeast", "south", "asia",
}

_SLUG_PREPOSITION_TOKENS = {"in", "to", "by", "on", "into", "from", "with"}


def _is_slug_noise(token: str) -> bool:
    if not token:
        return True
    lower = token.lower()
    if lower in _SLUG_NOISE_TOKENS:
        return True
    # url encoded native script cannot become a clean latin name
    if "%" in lower:
        return True
    if re.fullmatch(r"\d+(?:[.,]\d+)?[a-z]*", lower):
        return True
    return False


def _name_from_tokens(tokens: list[str]) -> str:
    if not tokens:
        return ""
    name = " ".join(t.capitalize() for t in tokens)
    cleaned = clean_startup_name(name)
    return cleaned if cleaned and not _is_generic_startup_name(cleaned) else ""


_BODY_NAME_NOISE = {
    "bangladesh", "singapore", "indonesia", "vietnam", "philippines",
    "malaysia", "thailand", "cambodia", "laos", "myanmar",
    "southeast asia", "south asia", "asia", "south", "southeast",
    "the company", "the firm", "the startup", "the business",
    "tech in asia", "deal street asia", "the daily star", "the business standard",
    "tbs", "techcrunch", "future startup", "startup bangladesh",
}

_BODY_SUBJECT_PATTERN = re.compile(
    r"\b([A-Z][a-zA-Z][a-zA-Z'’&-]*(?:\s+[A-Z][a-zA-Z'’&-]*){0,2})\s+"
    r"(?:has\s+|have\s+|just\s+|reportedly\s+|recently\s+)?"
    rf"(?:{_DEAL_VERB_ALT})\b"
)


def startup_from_body(body: str) -> str:
    if not body:
        return ""
    counts: dict[str, int] = {}
    for match in _BODY_SUBJECT_PATTERN.finditer(body[:4000]):
        cleaned = clean_startup_name(match.group(1).strip())
        if not cleaned or _is_generic_startup_name(cleaned):
            continue
        if cleaned.lower() in _BODY_NAME_NOISE:
            continue
        counts[cleaned] = counts.get(cleaned, 0) + 1
    if not counts:
        return ""
    return max(counts.items(), key=lambda kv: (kv[1], -len(kv[0])))[0]


def startup_from_investor_slug(url: str) -> str:
    # finds the proper noun near the deal verb in the last url segment
    segments = [s for s in urlparse(url).path.rstrip("/").split("/") if s]
    if not segments:
        return ""
    tail = segments[-1]
    tokens = [t for t in tail.split("-") if t]
    if not tokens:
        return ""

    verb_indexes = [i for i, t in enumerate(tokens) if t.lower() in _INVESTOR_DEAL_VERBS]
    subject_anchor_verbs = {"raises", "raised", "secures", "secured", "closes", "closed"}
    verb_index = next(
        (i for i in verb_indexes if tokens[i].lower() in subject_anchor_verbs),
        verb_indexes[0] if verb_indexes else -1,
    )

    if verb_index >= 0:
        next_idx = verb_index + 1
        if next_idx < len(tokens) and tokens[next_idx].lower() in _SLUG_PREPOSITION_TOKENS:
            forward: list[str] = []
            j = next_idx + 1
            while j < len(tokens) and not _is_slug_noise(tokens[j]) and len(forward) < 3:
                forward.append(tokens[j])
                j += 1
            recovered = _name_from_tokens(forward)
            if recovered:
                return recovered
        i = verb_index - 1
        while i >= 0 and _is_slug_noise(tokens[i]):
            i -= 1
        backward: list[str] = []
        while i >= 0 and not _is_slug_noise(tokens[i]) and len(backward) < 3:
            backward.append(tokens[i])
            i -= 1
        backward.reverse()
        return _name_from_tokens(backward)

    for token in tokens:
        if not _is_slug_noise(token):
            recovered = _name_from_tokens([token])
            if recovered:
                return recovered
    return ""


_TITLE_SEPARATOR_PATTERN = re.compile(r"\s+[|–—-]\s+")
_LATIN_PROPER_NOUN_PATTERN = re.compile(r"[A-Z][a-zA-Z][a-zA-Z'’&-]+")


def _is_native_script_alias(segment: str) -> bool:
    # native script segments are usually aliases for the previous name
    if not segment:
        return False
    latin = sum(1 for ch in segment if ch.isascii() and ch.isalpha())
    non_latin = sum(1 for ch in segment if ch.isalpha() and not ch.isascii())
    return non_latin > 0 and non_latin >= latin


def _starts_with_native_script(segment: str) -> bool:
    # rejoins segments split around a native script alias
    for ch in segment:
        if ch.isspace() or not ch.isalpha():
            continue
        return not ch.isascii()
    return False


def _select_title_segment(title: str) -> str:
    if not title:
        return ""
    raw_segments = [seg.strip() for seg in _TITLE_SEPARATOR_PATTERN.split(title) if seg.strip()]
    if not raw_segments:
        return title.strip()
    # keeps native script aliases with the surrounding title segment
    segments: list[str] = []
    for seg in raw_segments:
        if segments and (_is_native_script_alias(seg) or _starts_with_native_script(seg)):
            segments[-1] = f"{segments[-1]} {seg}"
        else:
            segments.append(seg)
    if len(segments) == 1:
        return segments[0]
    qualified = [
        seg for seg in segments
        if _DEAL_VERB_PATTERN.search(seg) and _LATIN_PROPER_NOUN_PATTERN.search(seg)
    ]
    if qualified:
        return max(qualified, key=len)
    verb_segments = [seg for seg in segments if _DEAL_VERB_PATTERN.search(seg)]
    if verb_segments:
        return max(verb_segments, key=len)
    return max(segments, key=len)


def startup_from_title(title: str) -> str:
    title = re.sub(r"^In\s+\d+\s+Words:\s+", "", title, flags=re.IGNORECASE)
    cleaned = _select_title_segment(title)
    subject_patterns = [
        rf"^(.+?)\s+(?:{_TITLE_SUBJECT_VERB_ALT})\b",
        r"^(.+?)\s+(?:closes|completes|wraps)\b.+?\bround\b",
        r"^(.+?)\s+(?:gets|receives|attracts|draws)\b.+?\b(?:backing|investment|funding)\b",
        r"^(.+?)\s+announces\b.+?\bfunding\b",
        r"^(.+?)\s+receives\b.+?\binvestment\b",
        r"\b(?:funding|investment)\s+announcement:\s+(.+?)$",
    ]
    for pattern in subject_patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if match:
            return clean_startup_name(match.group(1))
    object_patterns = [
        r"\binvests?\s+(?:\S+\s+){0,3}?in\s+([\w'’.& -]+?)(?:\s+(?:to|at|for|with|by|from|in|on)\b|\s+[–\-](?:\s|$)|\s*[,.]|$)",
        r"\bbacks\s+([\w'’.& -]+?)(?:\s+(?:to|at|for|with|by|from|in|on)\b|\s+[–\-](?:\s|$)|\s*[,.]|$)",
        r"\binvestment\s+in\s+([\w'’.& -]+?)(?:\s+(?:to|at|for|with|by|from|in|on)\b|\s+[–\-](?:\s|$)|\s*[,.]|$)",
    ]
    for pattern in object_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            return clean_startup_name(match.group(1))
    return ""


_PREFIX_STRIP_PATTERNS = [
    re.compile(r"^(?:singapore|indonesia|vietnam|philippines|malaysia|thailand|bangladesh|cambodia|laos|myanmar)['’]s?\s+", re.IGNORECASE),
    re.compile(r"^(?:bangladeshi|singaporean|indonesian|malaysian|filipino|vietnamese|thai|cambodian)\s+", re.IGNORECASE),
    re.compile(r"^(?:bangladesh-based|singapore-based|indonesia-based|malaysia-based|vietnam-based|philippines-based|thailand-based)\s+", re.IGNORECASE),
    re.compile(r"^(?:[\w-]+\s+){0,4}?(?:firm|company|startup|platform|player|marketplace|app)\s+", re.IGNORECASE),
    re.compile(r"^(?:startup|fintech|edtech|healthtech|deeptech|foodtech|agritech|proptech|climatetech|biotech|insurtech)\s+", re.IGNORECASE),
]

# helper verb fragments left by greedy title capture
_TRAILING_FRAGMENT_PATTERN = re.compile(
    r"\s+(?:has|have|had|is|are|was|were|will|just|finally|reportedly)$",
    re.IGNORECASE,
)


def clean_startup_name(value: str) -> str:
    value = value.strip()
    value = re.sub(
        r"^(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|"
        r"Dec(?:ember)?)\s+\d{1,2},\s+\d{4}\s+",
        "",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(r"^(?:\d{4}[-/]\d{1,2}[-/]\d{1,2}\s+)?Portfolio News\s+", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+Portfolio News$", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^[A-Z][\w'’&.-]+-backed\s+", "", value)
    value = re.sub(r"\bjoins?\b.+$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\blaunch(?:es|ed)?\b.+$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\bannounces?\b.+$", "", value, flags=re.IGNORECASE).strip()
    # trims descriptive geography from captured startup names
    geo_split = re.search(
        r"(?:^|\s+)(?:southeast\s+asia|south-?east\s+asia|south\s+asia|apac|asia)\b",
        value,
        flags=re.IGNORECASE,
    )
    if geo_split:
        value = value[: geo_split.start()].strip()
    # repeats prefix stripping because prefixes can be stacked
    while True:
        previous = value
        for pattern in _PREFIX_STRIP_PATTERNS:
            value = pattern.sub("", value)
        if value == previous:
            break
    # removes trailing helper verbs left by greedy subject capture
    while True:
        previous = value
        value = _TRAILING_FRAGMENT_PATTERN.sub("", value).strip()
        if value == previous:
            break
    # removes trailing native script aliases
    value = re.sub(r"\s+[^\s\x00-\x7f]+\s*$", "", value).strip()
    # keeps only the proper noun after leading descriptive prefixes
    descriptive_prefix = re.match(
        r"^(?:country['’]s\s+(?:first|leading|largest|biggest|top)\s+|the\s+(?:first|leading|largest|biggest|top)\s+|leading\s+|first\s+)[\w\s'’&-]*?\s+([A-Z][\w'’&-]+(?:\s+[A-Z][\w'’&-]+)?)\s*$",
        value,
    )
    if descriptive_prefix:
        value = descriptive_prefix.group(1)
    value = re.sub(r"\s+(?:ai|ml|app|io)$", "", value, flags=re.IGNORECASE)
    cleaned = value.strip(" :,-")
    if _is_generic_startup_name(cleaned):
        return ""
    return cleaned


_GENERIC_STARTUP_NAMES = {
    "local startups", "local startup", "multiple startups", "startups",
    "the startup", "the company", "the firm", "the business",
    "portfolio", "portfolio archive", "funding", "investment",
    "various startups", "several startups", "two startups", "three startups",
    "firm", "company", "startup", "platform", "player", "marketplace",
    "business", "venture", "fund", "round", "deal",
    "southeast asia", "south-east asia", "asia", "region", "southeast", "south-east",
    "climate investment", "venture capital", "growth stage",
    # investor names that can be mistaken for startups in portfolio posts
    "openspace", "wavemaker", "monkshill", "vertex", "insignia",
    "jungle", "golden gate", "peak xv", "sequoia", "east ventures",
}


def _is_generic_startup_name(value: str) -> bool:
    if not value:
        return True
    return value.lower() in _GENERIC_STARTUP_NAMES


def lead_key(evidence: Evidence) -> str:
    # groups the same deal across sources and avoids merging unknown startups
    if evidence.parent_lead_key:
        return evidence.parent_lead_key
    startup = re.sub(r"[^a-z0-9]+", "", evidence.startup.lower())
    country = evidence.country.lower()
    if startup:
        return f"{startup}:{country}"
    return hashlib.sha1(evidence.url.encode("utf-8")).hexdigest()


def has_deal_action(*values: str) -> bool:
    return bool(DEAL_ACTION_PATTERN.search(" ".join(values)))


def has_round_or_amount(*values: str) -> bool:
    return bool(ROUND_OR_AMOUNT_PATTERN.search(" ".join(values)))


def has_negative_topic(*values: str) -> bool:
    return bool(NEGATIVE_TOPIC_PATTERN.search(" ".join(values)))


def strict_startup_candidate(title: str, url: str, source: SourceConfig | None) -> str:
    startup = startup_from_title(title)
    if startup:
        return startup
    if source and source.source_type == "Official startup/investor site":
        return startup_from_investor_slug(url)
    return ""


def passes_strict_funding_checks(
    title: str,
    url: str,
    source: SourceConfig | None,
    content: str = "",
) -> bool:
    if not title or not url:
        return False
    window = content[:6000]
    values = (title, url, window) if window else (title, url)
    if has_negative_topic(title, url):
        return False
    startup = strict_startup_candidate(title, url, source)
    if not startup or _is_generic_startup_name(startup):
        return False
    if not has_deal_action(*values):
        return False
    if not has_round_or_amount(*values):
        return False
    if ROUNDUP_TITLE_PATTERN.search(title):
        return False
    if source and source.source_type == "Trusted media":
        if NON_TARGET_COUNTRY_PATTERN.search(f"{title} {url}"):
            return False
        if country_from_text(*values) == "Unknown":
            return False
    return True


def is_strict_funding_headline(title: str, url: str, source: SourceConfig | None) -> bool:
    return passes_strict_funding_checks(title, url, source)


def is_strict_funding_announcement(
    title: str,
    url: str,
    content: str,
    source: SourceConfig | None,
) -> bool:
    return passes_strict_funding_checks(title, url, source, content)


def is_funding_like(title: str, url: str) -> bool:
    return bool(FUNDING_PATTERN.search(f"{title} {url}")) and not has_negative_topic(title, url)


def extract_feed_leads(feed_url: str, xml_text: str, source: SourceConfig | None = None) -> list[Lead]:
    soup = BeautifulSoup(xml_text, "xml")
    leads: list[Lead] = []
    for item in soup.find_all(["item", "entry"]):
        title = clean_text(item.title.text if item.title else "")
        link_tag = item.find("link")
        if link_tag is None:
            continue
        url = normalize_url(link_tag.get("href") or link_tag.text or "")
        keywords = matched_keywords(title, url)
        if not title or not url or not keywords or not is_funding_like(title, url):
            continue
        if not is_strict_funding_headline(title, url, source):
            continue
        published = ""
        if item.find("pubDate") and item.pubDate.text:
            published = item.pubDate.text.strip()
        elif item.find("published") and item.published.text:
            published = item.published.text.strip()
        leads.append(
            Lead(
                title=title,
                url=url,
                published=published,
                source_url=feed_url,
                matched_keywords=keywords,
            )
        )
    return leads


def extract_index_leads(index_url: str, html_text: str, source: SourceConfig | None = None) -> list[Lead]:
    soup = BeautifulSoup(html_text, "html.parser")
    leads: list[Lead] = []
    for link in soup.find_all("a", href=True):
        title = clean_text(link.get_text(" "))
        url = normalize_url(urljoin(index_url, link["href"]))
        keywords = matched_keywords(title, url)
        if not title or not url.startswith("http") or not keywords or not is_funding_like(title, url):
            continue
        if not is_strict_funding_headline(title, url, source):
            continue
        leads.append(
            Lead(
                title=title,
                url=url,
                published="",
                source_url=index_url,
                matched_keywords=keywords,
            )
        )
    return leads


def evidence_from_lead(
    lead: Lead,
    session: requests.Session,
    sources: list[SourceConfig],
    parent_lead_key: str = "",
) -> tuple[Evidence, ArticleRecord, list[str]] | None:
    source = source_for_url(lead.url, sources)
    source_type = classify_source(lead.url)
    if not is_allowed_source_type(source_type):
        return None

    def headline_only_result(reason: str) -> tuple[Evidence, ArticleRecord, list[str]] | None:
        headline = lead.title.strip() if lead.title else title_from_url_slug(lead.url)
        if not headline:
            return None
        if not is_strict_funding_headline(headline, lead.url, source):
            return None
        combined = f"{headline}. {lead.url}"
        startup = startup_from_title(headline)
        if not startup and source and source.source_type == "Official startup/investor site":
            startup = startup_from_investor_slug(lead.url)
        country = country_from_text(headline, lead.url)
        amount = amount_from_text(combined)
        round_type = round_from_text(combined)
        lead_investor = lead_investor_from_text(combined)
        investor_names = investors_from_text(combined, lead_investor)
        investor_type = investor_type_from_names(investor_names)
        notes = [reason]
        if not startup:
            notes.append("Startup name needs manual verification")
        if country == "Unknown":
            notes.append("Country needs manual verification")
        if month_year(lead.published) == "Not Available":
            notes.append("Date needs manual verification")
        evidence = Evidence(
            title=headline,
            url=lead.url,
            source_type=source_type,
            published=lead.published,
            startup=startup,
            country=country,
            amount=amount,
            round_type=round_type,
            investor_names=investor_names,
            lead_investor=lead_investor,
            investor_type=investor_type,
            notes=notes,
            headline_only=True,
        )
        article = ArticleRecord(
            title=headline,
            date=lead.published,
            source=source.name if source else domain_for(lead.url),
            url=lead.url,
            content="",
            keywords=matched_keywords(headline, lead.url),
        )
        return evidence, article, []

    try:
        html_text = fetch_html(lead.url, session, source)
    except Exception:
        if domain_for(lead.url) in PAYWALLED_HEADLINE_ONLY_DOMAINS:
            return headline_only_result("Paywalled or gated source, headline-only extraction")
        return None

    page_title = best_article_title(title_from_html(html_text), lead.title)
    # uses the url slug when rendered html only gives a generic page title
    slug_title = title_from_url_slug(lead.url)
    if slug_title and (not page_title or _looks_generic_banner(page_title)):
        page_title = slug_title
    body_text = text_from_html(html_text)
    published = published_from_html(html_text) or lead.published
    if domain_for(lead.url) in PAYWALLED_HEADLINE_ONLY_DOMAINS and not body_text:
        return headline_only_result("Paywalled or gated source, headline-only extraction")
    body_head = body_text[:6000]
    combined = f"{page_title}. {body_head}"
    if not parent_lead_key and not is_strict_funding_announcement(page_title, lead.url, body_text, source):
        return None
    startup = startup_from_title(page_title)
    if not startup and source and source.source_type == "Official startup/investor site":
        startup = startup_from_investor_slug(lead.url)
    if not startup:
        startup = startup_from_body(body_head)
    country = country_from_text(page_title, lead.url, body_head)
    amount = amount_from_text(combined)
    round_type = round_from_text(combined)
    lead_investor = lead_investor_from_text(combined)
    investor_names = investors_from_text(combined, lead_investor)
    investor_type = investor_type_from_names(investor_names)
    notes = []

    if not startup:
        notes.append("Startup name needs manual verification")
    if country == "Unknown":
        notes.append("Country needs manual verification")
    if month_year(published) == "Not Available":
        notes.append("Date needs manual verification")

    evidence = Evidence(
        title=page_title,
        url=lead.url,
        source_type=source_type,
        published=published,
        startup=startup,
        country=country,
        amount=amount,
        round_type=round_type,
        investor_names=investor_names,
        lead_investor=lead_investor,
        investor_type=investor_type,
        notes=notes,
        parent_lead_key=parent_lead_key,
    )
    article = ArticleRecord(
        title=page_title,
        date=published,
        source=source.name if source else domain_for(lead.url),
        url=lead.url,
        content=body_text,
        keywords=matched_keywords(page_title, lead.url, body_text),
    )
    return evidence, article, outbound_links_from_html(html_text, lead.url)


def follow_outbound_links(
    parent_extracted: list[tuple[Evidence, ArticleRecord, list[str]]],
    session: requests.Session,
    sources: list[SourceConfig],
    request_delay: float,
) -> tuple[list[Evidence], list[ArticleRecord]]:
    # child links inherit the parent key so citations stay in the same bundle
    new_evidence: list[Evidence] = []
    new_articles: list[ArticleRecord] = []
    seen_urls: set[str] = {ev.url for ev, _, _ in parent_extracted}
    last_fetched: dict[str, float] = {}
    for parent_evidence, _parent_article, parent_links in parent_extracted:
        if not parent_links or parent_evidence.headline_only:
            continue
        parent_key = lead_key(parent_evidence)
        for link in parent_links:
            if link in seen_urls:
                continue
            seen_urls.add(link)
            domain = domain_for(link)
            elapsed = time.monotonic() - last_fetched.get(domain, 0.0)
            if elapsed < request_delay:
                time.sleep(request_delay - elapsed)
            last_fetched[domain] = time.monotonic()
            child_lead = Lead(
                title="",
                url=link,
                published="",
                source_url=parent_evidence.url,
                matched_keywords=[],
            )
            try:
                result = evidence_from_lead(child_lead, session, sources, parent_key)
            except Exception:
                continue
            if result is None:
                continue
            child_evidence, child_article, _child_links = result
            child_evidence.notes.append(
                f"Followed from {domain_for(parent_evidence.url)} citation"
            )
            new_evidence.append(child_evidence)
            new_articles.append(child_article)
    return new_evidence, new_articles


def dedupe_leads(leads: Iterable[Lead]) -> list[Lead]:
    seen: set[str] = set()
    unique: list[Lead] = []
    for lead in leads:
        if lead.url in seen:
            continue
        seen.add(lead.url)
        unique.append(lead)
    return unique


def source_from_seed(url: str, source_type: str) -> SourceConfig:
    domain = domain_for(url)
    if source_type == DISALLOWED_SOURCE_TYPE or not is_allowed_source_type(source_type):
        raise ValueError(f"Domain is not in allowed source policy: {domain}")
    return SourceConfig(
        name=domain,
        source_type=source_type,
        allowed_domains=(domain,),
        seed_urls=(url,),
    )


def configured_sources(args: argparse.Namespace) -> list[SourceConfig]:
    sources = list(SOURCE_CONFIGS)
    for feed_url in args.feed:
        source_type = classify_source(feed_url)
        if not is_allowed_source_type(source_type):
            print(f"[WARN] Skipping disallowed --feed domain: {feed_url}")
            continue
        sources.append(source_from_seed(feed_url, source_type))
    for index_url in args.index_url:
        source_type = classify_source(index_url)
        if not is_allowed_source_type(source_type):
            print(f"[WARN] Skipping disallowed --index-url domain: {index_url}")
            continue
        sources.append(source_from_seed(index_url, source_type))
    return sources


def crawl_source(
    source: SourceConfig,
    args: argparse.Namespace,
    session: requests.Session,
    robots_cache: dict[str, RobotFileParser],
    last_request_at: dict[str, float],
) -> CrawlResult:
    # bounded crawl across allowed domains depth page count and link count
    leads: list[Lead] = []
    articles: list[ArticleRecord] = []
    log_entries: list[CrawlLogEntry] = []
    initial_seeds = list(source.seed_urls)
    initial_seeds.extend(source_search_urls(source))
    deduped_seeds = list(dict.fromkeys(initial_seeds))
    queue = deque((seed_url, 0) for seed_url in deduped_seeds)
    visited: set[str] = set()

    while queue and len(visited) < args.max_pages:
        current_url, depth = queue.popleft()
        current_url = normalize_url(current_url)
        if current_url in visited:
            continue

        skip_reason = should_skip_url(current_url)
        if skip_reason:
            log_entries.append(CrawlLogEntry(current_url, source.name, "skipped", skip_reason))
            continue
        if not is_same_allowed_domain(current_url, source):
            log_entries.append(CrawlLogEntry(current_url, source.name, "skipped", "Outside allowed domain"))
            continue
        if not can_fetch_url(current_url, source, session, robots_cache):
            log_entries.append(CrawlLogEntry(current_url, source.name, "skipped", "Blocked by robots.txt"))
            continue

        # enforces one request gap per host across all sources
        domain = domain_for(current_url)
        wait_seconds = args.request_delay - (time.monotonic() - last_request_at.get(domain, 0.0))
        if wait_seconds > 0:
            time.sleep(wait_seconds)

        try:
            html_text = fetch_html(current_url, session, source)
        except Exception as exc:
            log_entries.append(CrawlLogEntry(current_url, source.name, "failed", str(exc)))
            continue

        visited.add(current_url)
        last_request_at[domain] = time.monotonic()

        if is_probably_feed(current_url, html_text):
            feed_leads = extract_feed_leads(current_url, html_text, source)
            leads.extend(feed_leads)
            log_entries.append(
                CrawlLogEntry(current_url, source.name, "fetched_feed", f"{len(feed_leads)} leads")
            )
            continue

        index_leads = extract_index_leads(current_url, html_text, source)
        if index_leads:
            leads.extend(index_leads)
            log_entries.append(
                CrawlLogEntry(current_url, source.name, "fetched_index", f"{len(index_leads)} leads")
            )

        article = article_record_from_page(current_url, html_text, source)
        if is_valid_article(current_url, article.title, article.content, source):
            articles.append(article)
            leads.append(
                Lead(
                    title=article.title,
                    url=current_url,
                    published=article.date,
                    source_url=current_url,
                    matched_keywords=article.keywords,
                )
            )
            log_entries.append(CrawlLogEntry(current_url, source.name, "kept", "Funding article"))
        else:
            log_entries.append(CrawlLogEntry(current_url, source.name, "scanned", "Not a funding article"))

        if depth >= args.max_depth:
            continue

        soup = BeautifulSoup(html_text, "html.parser")
        scored_links: list[tuple[int, str, str]] = []
        for link in soup.find_all("a", href=True):
            link_text = clean_text(link.get_text(" "))
            next_url = normalize_url(urljoin(current_url, link["href"]))
            if next_url in visited:
                continue
            if should_skip_url(next_url) or not is_same_allowed_domain(next_url, source):
                continue
            score = link_score(next_url, link_text, source)
            if score <= 0:
                continue
            scored_links.append((score, next_url, link_text))

        for _, next_url, _ in sorted(scored_links, reverse=True)[: args.max_links_per_page]:
            queue.append((next_url, depth + 1))

    return CrawlResult(dedupe_leads(leads), articles, log_entries)


def collect_leads(args: argparse.Namespace, session: requests.Session) -> CrawlResult:
    sources = configured_sources(args)
    all_leads: list[Lead] = []
    all_articles: list[ArticleRecord] = []
    all_log_entries: list[CrawlLogEntry] = []
    robots_cache: dict[str, RobotFileParser] = {}
    last_request_at: dict[str, float] = {}

    for source in sources:
        result = crawl_source(source, args, session, robots_cache, last_request_at)
        all_leads.extend(result.leads)
        all_articles.extend(result.articles)
        all_log_entries.extend(result.log_entries)

    for url in args.url:
        normalized_url = normalize_url(url)
        source_type = classify_source(normalized_url)
        if not is_allowed_source_type(source_type):
            print(f"[WARN] Skipping disallowed --url domain: {normalized_url}")
            continue
        all_leads.append(
            Lead(
                title=normalized_url,
                url=normalized_url,
                published="",
                source_url="manual-url",
                matched_keywords=matched_keywords(normalized_url),
            )
        )

    return CrawlResult(dedupe_leads(all_leads), all_articles, all_log_entries)


def build_bundles(evidence_items: Iterable[Evidence]) -> list[RecordBundle]:
    # bundles multi source coverage and ranks the best source first
    groups: dict[str, list[Evidence]] = {}
    for evidence in evidence_items:
        groups.setdefault(lead_key(evidence), []).append(evidence)

    bundles: list[RecordBundle] = []
    for index, sources in enumerate(groups.values(), start=1):
        sorted_sources = sorted(
            sources,
            key=lambda item: (SOURCE_PRIORITY.get(item.source_type, 99), item.url),
        )
        primary = sorted_sources[0]
        startup = primary.startup
        country = primary.country
        prefix = "BD" if country == "Bangladesh" else "SEA"
        record_id = f"{prefix}-AUTO-{index:03d}"
        issues = bundle_issues(sorted_sources)
        bundles.append(
            RecordBundle(
                record_id=record_id,
                startup=startup,
                country=country,
                sources=sorted_sources,
                issues=issues,
            )
        )
    return bundles


def bundle_issues(sources: list[Evidence]) -> list[str]:
    # issue text here keeps bundles out of the final dataset
    issues: list[str] = []
    primary = sources[0]
    if len({canonical_source_url(item.url) for item in sources}) < 2:
        issues.append("Needs a second source URL before final dataset use")
    if not primary.startup:
        issues.append("Startup name needs manual verification")
    if primary.headline_only:
        issues.append("Paywalled or gated source, headline-only extraction")
    if primary.country == "Unknown":
        issues.append("Country needs manual verification")
    if month_year(primary.published) == "Not Available":
        issues.append("Date needs manual verification")
    return sorted(set(issues + primary.notes))


_MISSING_VALUE_SENTINELS = {"", "Unknown", "Not Available", "Not Stated"}


def choose_value(sources: list[Evidence], attr: str, fallback: str) -> str:
    # first non placeholder value wins in source priority order
    for source in sources:
        value = getattr(source, attr)
        if value not in _MISSING_VALUE_SENTINELS:
            return value
    return fallback


def is_complete_trusted_media_source(source: Evidence, startup: str) -> bool:
    title = source.title or ""
    return (
        source.source_type == "Trusted media"
        and not source.headline_only
        and bool(startup)
        and source.country not in _MISSING_VALUE_SENTINELS
        and not NON_TARGET_COUNTRY_PATTERN.search(f"{title} {source.url}")
        and source.amount not in {"Undisclosed", ""}
        and bool(DEAL_TITLE_PATTERN.search(title))
        and bool(AMOUNT_PATTERN.search(title))
        and not ROUNDUP_TITLE_PATTERN.search(title)
    )


def dataset_rows(bundles: list[RecordBundle]) -> list[list[str]]:
    # writes verified rows and leaves uncertain leads for review outputs
    rows: list[list[str]] = []
    for bundle in bundles:
        unique_sources = []
        seen = set()
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

        conflict_notes = []
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


def prune_irrelevant_leads(evidence_items: list[Evidence], sources: list[SourceConfig]) -> list[Evidence]:
    pruned: list[Evidence] = []
    for evidence in evidence_items:
        if not is_allowed_source_type(evidence.source_type):
            continue
        # followed citations inherit the parent relevance checks
        if evidence.parent_lead_key:
            pruned.append(evidence)
            continue
        source = source_for_url(evidence.url, sources)
        startup = evidence.startup.strip() if evidence.startup else ""
        if not startup or _is_generic_startup_name(startup):
            continue
        if has_negative_topic(evidence.title, evidence.url):
            continue
        if not has_deal_action(evidence.title):
            continue
        signal_blob = f"{evidence.title} {evidence.amount} {evidence.round_type}"
        if not has_round_or_amount(signal_blob):
            continue
        if source and source.source_type == "Trusted media":
            if evidence.country in _MISSING_VALUE_SENTINELS:
                continue
            if NON_TARGET_COUNTRY_PATTERN.search(f"{evidence.title} {evidence.url}"):
                continue
        pruned.append(evidence)
    return pruned


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


def crawl_log_rows(log_entries: list[CrawlLogEntry]) -> list[list[str]]:
    return [[entry.url, entry.source, entry.action, entry.reason] for entry in log_entries]


def write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
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
    parser.add_argument("--feed", action="append", default=[], help="Extra RSS or Atom feed URL.")
    parser.add_argument("--index-url", action="append", default=[], help="Extra index page to scan for links.")
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


def main() -> None:
    args = parse_args()
    captured_at = datetime.now(timezone.utc).isoformat()
    session = requests.Session()
    sources = configured_sources(args)

    crawl_result = collect_leads(args, session)
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
    rows = dataset_rows(bundles)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.output_dir / "dataset.csv", DATASET_HEADER, rows)
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
        ["url", "source", "action", "reason"],
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
    print(f"Wrote {len(rows)} cross-verified dataset rows to {args.output_dir / 'dataset.csv'}.")
    print(f"Review unresolved leads in {args.output_dir / 'funding_leads.csv'}.")


if __name__ == "__main__":
    try:
        main()
    finally:
        shutdown_browser()
