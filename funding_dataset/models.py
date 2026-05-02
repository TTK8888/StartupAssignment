from __future__ import annotations

from dataclasses import dataclass, field


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
    # NEW: scraper confidence — list of field names the scraper isn't sure about
    needs_review: list[str] = field(default_factory=list)


@dataclass
class RecordBundle:
    record_id: str
    startup: str
    country: str
    sources: list[Evidence]
    issues: list[str]
