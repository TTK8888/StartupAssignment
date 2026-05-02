from __future__ import annotations

import re
from urllib.parse import quote_plus, urlparse, urlunparse

from .settings import (
    ALLOWED_STARTUP_DATABASE_DOMAINS,
    ALLOWED_TRUSTED_MEDIA_DOMAINS,
    AMOUNT_PATTERN,
    CRAWL_KEYWORDS,
    DISALLOWED_SOURCE_TYPE,
    FUNDING_PATTERN,
    OFFICIAL_SOURCE_SEARCH_TERMS,
    SITE_SEARCH_TEMPLATES_BY_DOMAIN,
    SKIP_URL_PATTERN,
    SOURCE_CONFIGS,
    SOURCE_TYPE_BY_DOMAIN,
    SourceConfig,
    TRUSTED_MEDIA_SEARCH_TERMS,
)


def normalize_url(url: str) -> str:
    """Return a URL without fragment or query string."""
    parsed = urlparse(url.strip())
    clean = parsed._replace(fragment="", query="")
    return urlunparse(clean)


def domain_for(url: str) -> str:
    """Return a lowercased hostname without a leading www prefix."""
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        return host[4:]
    return host


def canonical_source_url(url: str) -> str:
    """Return the canonical source URL used for source deduplication."""
    parsed = urlparse(url)
    return urlunparse((parsed.scheme.lower() or "https", domain_for(url), parsed.path.rstrip("/"), "", "", ""))


def source_type_for_domain(domain: str) -> str:
    """Classify a domain into the configured source policy type."""
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
    """Classify a URL into the configured source policy type."""
    return source_type_for_domain(domain_for(url))


def is_allowed_source_type(source_type: str) -> bool:
    """Return whether a source type is allowed for evidence collection."""
    return source_type in {
        "Official startup/investor site",
        "Company press release",
        "LinkedIn announcement",
        "Trusted media",
        "Startup database",
    }


def source_for_url(url: str, sources: list[SourceConfig]) -> SourceConfig | None:
    """Find the configured source whose allowed domains contain a URL."""
    domain = domain_for(url)
    for source in sources:
        if any(domain == allowed or domain.endswith(f".{allowed}") for allowed in source.allowed_domains):
            return source
    return None


def source_search_urls(source: SourceConfig) -> list[str]:
    """Build configured search URLs for a source when search templates apply."""
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
    """Return whether a URL belongs to one of a source's allowed domains."""
    domain = domain_for(url)
    return any(domain == allowed or domain.endswith(f".{allowed}") for allowed in source.allowed_domains)


def is_probably_feed(url: str, html_text: str) -> bool:
    """Return whether a URL or response body appears to be an RSS or Atom feed."""
    path = urlparse(url).path.lower()
    stripped = html_text.lstrip()[:120].lower()
    return path.endswith((".rss", ".xml")) or "/feed" in path or stripped.startswith(("<?xml", "<rss", "<feed"))


def should_skip_url(url: str) -> str:
    """Return a skip reason for unsupported or irrelevant URLs, or an empty string."""
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return "Unsupported URL scheme"
    if SKIP_URL_PATTERN.search(url):
        return "Irrelevant URL pattern"
    return ""


def matched_keywords(*values: str) -> list[str]:
    """Return crawl keywords found as whole words across the given text values."""
    blob = " ".join(values).lower()
    return [keyword for keyword in CRAWL_KEYWORDS if re.search(rf"\b{re.escape(keyword)}\b", blob)]


def url_matches_source_pattern(url: str, source: SourceConfig) -> bool:
    """Return whether a URL path includes any pattern configured for a source."""
    path = urlparse(url).path.lower()
    return any(pattern.lower() in path for pattern in source.url_patterns)


def is_listing_url(url: str) -> bool:
    """Return whether a URL looks like a listing page instead of an article."""
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
    """Return whether a URL shape is likely to point at an article page."""
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
    """Score a link for bounded crawl priority based on URL and anchor text signals."""
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
