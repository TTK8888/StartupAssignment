from __future__ import annotations

import hashlib
import re
import time
from typing import Iterable

import requests

from .models import ArticleRecord, Evidence, Lead, RecordBundle
from .settings import (
    AMOUNT_PATTERN,
    DEAL_TITLE_PATTERN,
    NON_TARGET_COUNTRY_PATTERN,
    PAYWALLED_HEADLINE_ONLY_DOMAINS,
    ROUNDUP_TITLE_PATTERN,
    SOURCE_PRIORITY,
    SourceConfig,
)
from .urls import (
    canonical_source_url,
    classify_source,
    domain_for,
    is_allowed_source_type,
    matched_keywords,
    source_for_url,
)
from .scraper.amounts import amount_from_text
from .scraper.articles import (
    best_article_title,
    month_year,
    outbound_links_from_html,
    published_from_html,
    text_from_html,
    title_from_html,
    title_from_url_slug,
    _looks_generic_banner,
)
from .scraper.classification import (
    has_deal_action,
    has_negative_topic,
    has_round_or_amount,
    is_strict_funding_announcement,
    is_strict_funding_headline,
)
from .scraper.confidence import assess_evidence
from .scraper.entities import (
    _is_generic_startup_name,
    country_from_text,
    investor_type_from_names,
    investors_from_text,
    lead_investor_from_text,
    round_from_text,
    startup_from_body,
    startup_from_investor_slug,
    startup_from_title,
)
from .http_client import fetch_html


_MISSING_VALUE_SENTINELS = {"", "Unknown", "Not Available", "Not Stated"}


def lead_key(evidence: Evidence) -> str:
    """Build a stable grouping key for evidence that appears to describe the same deal."""
    # groups the same deal across sources and avoids merging unknown startups
    if evidence.parent_lead_key:
        return evidence.parent_lead_key
    startup = re.sub(r"[^a-z0-9]+", "", evidence.startup.lower())
    country = evidence.country.lower()
    if startup:
        return f"{startup}:{country}"
    return hashlib.sha1(evidence.url.encode("utf-8")).hexdigest()


def dedupe_leads(leads: Iterable[Lead]) -> list[Lead]:
    """Return leads in input order with duplicate URLs removed."""
    seen: set[str] = set()
    unique: list[Lead] = []
    for lead in leads:
        if lead.url in seen:
            continue
        seen.add(lead.url)
        unique.append(lead)
    return unique


def evidence_from_lead(
    lead: Lead,
    session: requests.Session,
    sources: list[SourceConfig],
    parent_lead_key: str = "",
) -> tuple[Evidence, ArticleRecord, list[str]] | None:
    """Fetch and extract structured funding evidence from a crawl lead."""
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
        evidence.needs_review = assess_evidence(evidence)
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
    evidence.needs_review = assess_evidence(evidence, body_text)
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
    """Extract evidence from trusted outbound citation links attached to parent articles."""
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


def build_bundles(evidence_items: Iterable[Evidence]) -> list[RecordBundle]:
    """Group related evidence into prioritized record bundles for dataset output."""
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
    """Return review issues that affect whether a bundle can enter the final dataset."""
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


def choose_value(sources: list[Evidence], attr: str, fallback: str) -> str:
    """Choose the first non-placeholder attribute value from sources in priority order."""
    # first non placeholder value wins in source priority order
    for source in sources:
        value = getattr(source, attr)
        if value not in _MISSING_VALUE_SENTINELS:
            return value
    return fallback


def prune_irrelevant_leads(
    evidence_items: list[Evidence],
    sources: list[SourceConfig],
) -> list[Evidence]:
    """Filter extracted evidence down to records that satisfy the funding relevance rules."""
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


def is_complete_trusted_media_source(source: Evidence, startup: str) -> bool:
    """Return whether a trusted media item has enough detail to stand alone."""
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
