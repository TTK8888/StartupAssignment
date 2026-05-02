from __future__ import annotations

from ..settings import (
    DEAL_ACTION_PATTERN,
    FUNDING_PATTERN,
    NEGATIVE_TOPIC_PATTERN,
    NON_TARGET_COUNTRY_PATTERN,
    ROUND_OR_AMOUNT_PATTERN,
    ROUNDUP_TITLE_PATTERN,
    SourceConfig,
)
from .entities import (
    _is_generic_startup_name,
    country_from_text,
    startup_from_investor_slug,
    startup_from_title,
)


def has_deal_action(*values: str) -> bool:
    """Return whether text contains a funding-deal action phrase."""
    return bool(DEAL_ACTION_PATTERN.search(" ".join(values)))


def has_round_or_amount(*values: str) -> bool:
    """Return whether text contains an investment round or amount signal."""
    return bool(ROUND_OR_AMOUNT_PATTERN.search(" ".join(values)))


def has_negative_topic(*values: str) -> bool:
    """Return whether text matches a topic that should be excluded."""
    return bool(NEGATIVE_TOPIC_PATTERN.search(" ".join(values)))


def strict_startup_candidate(title: str, url: str, source: SourceConfig | None) -> str:
    """Extract a startup candidate used by strict funding checks."""
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
    """Return whether title, URL, and optional content pass strict funding rules."""
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
    """Return whether a headline passes strict funding rules without page content."""
    return passes_strict_funding_checks(title, url, source)


def is_strict_funding_announcement(
    title: str,
    url: str,
    content: str,
    source: SourceConfig | None,
) -> bool:
    """Return whether a full article passes strict funding announcement rules."""
    return passes_strict_funding_checks(title, url, source, content)


def is_funding_like(title: str, url: str) -> bool:
    """Return whether title and URL have broad funding signals and no negative topic."""
    return bool(FUNDING_PATTERN.search(f"{title} {url}")) and not has_negative_topic(title, url)
