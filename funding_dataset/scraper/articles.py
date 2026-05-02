from __future__ import annotations

import argparse
import re
from datetime import date, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..models import ArticleRecord
from ..settings import (
    AMOUNT_PATTERN,
    COUNTRY_HINT_PATTERN,
    FOLLOWABLE_OUTBOUND_DOMAINS,
    FUNDING_PATTERN,
    SourceConfig,
)
from ..text_utils import clean_text
from ..urls import (
    domain_for,
    looks_like_article_url,
    matched_keywords,
    normalize_url,
    should_skip_url,
    url_matches_source_pattern,
)
from .classification import is_strict_funding_announcement


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
