from __future__ import annotations

import argparse
import time
from collections import deque
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from .bundling import dedupe_leads
from .http_client import can_fetch_url, fetch_html
from .models import CrawlLogEntry, CrawlResult, Lead
from .settings import (
    DISALLOWED_SOURCE_TYPE,
    SOURCE_CONFIGS,
    SourceConfig,
)
from .text_utils import clean_text
from .urls import (
    classify_source,
    domain_for,
    is_allowed_source_type,
    is_probably_feed,
    is_same_allowed_domain,
    link_score,
    matched_keywords,
    normalize_url,
    should_skip_url,
    source_search_urls,
)
from .scraper.articles import (
    article_record_from_page,
    is_valid_article,
)
from .scraper.classification import (
    is_funding_like,
    is_strict_funding_headline,
)


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


def source_from_seed(url: str, source_type: str) -> SourceConfig:
    domain = domain_for(url)
    if source_type == DISALLOWED_SOURCE_TYPE or not is_allowed_source_type(source_type):
        raise ValueError(f"Domain is not in allowed source policy: {domain}")
    return SourceConfig(
        name=domain,
        source_type=source_type,
        allowed_domains=(domain,),
        seed_urls=(url,),
        url_patterns=(),
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
    articles = []
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


def collect_leads(
    sources: list[SourceConfig],
    args: argparse.Namespace,
    session: requests.Session,
) -> CrawlResult:
    all_leads: list[Lead] = []
    all_articles = []
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
