"""
Per-evidence confidence assessment.

Returns a list of field names the scraper isn't sure about so they surface
in the dataset's `needs_review` column instead of silently winning.

Heuristics mirror the rules listed in draft-a-plan-to-sleepy-pearl.md:
- startup: generic name, slug-only origin, missing from body
- amount: roundup-style article, multiple amounts in body, no currency symbol
- round: no explicit ROUND_PATTERNS match
- country: only COUNTRY_HINT_PATTERN matched, no direct COUNTRY_TERMS mention
- lead_investor: prose-shaped capture, comma-separated list with no clear lead
"""

from __future__ import annotations

import re

from ..models import Evidence
from ..settings import (
    AMOUNT_PATTERN,
    COUNTRY_HINT_PATTERN,
    COUNTRY_TERMS,
    ROUND_PATTERNS,
    ROUNDUP_TITLE_PATTERN,
)
from .entities import _is_generic_startup_name


_CURRENCY_SYMBOL_PATTERN = re.compile(
    r"(?:US\$|USD|BDT|Tk|SGD|S\$|MYR|RM|PHP|IDR|THB|VND|\$)",
    re.IGNORECASE,
)

# matches the same prose markers used in entities._looks_like_prose so an
# extracted "lead" sentence reads as needing review
_PROSE_MARKERS = (
    " is ", " was ", " are ", " were ", " has ", " have ",
    " will ", " would ", " should ", " can ", " may ", " might ",
    " which ", " that ", " help ", " helps ", " expand ",
    " catalyze ", " scale ", " grow ", " their ", " its ", " on ",
)


def _startup_uncertain(evidence: Evidence, body_text: str) -> bool:
    name = (evidence.startup or "").strip()
    if not name or _is_generic_startup_name(name):
        return True
    if not body_text:
        return False
    return name.lower() not in body_text.lower()


def _amount_uncertain(evidence: Evidence, body_text: str) -> bool:
    amount = evidence.amount or ""
    if amount in {"", "Undisclosed"}:
        return False
    if ROUNDUP_TITLE_PATTERN.search(evidence.title or ""):
        return True
    if not _CURRENCY_SYMBOL_PATTERN.search(amount):
        return True
    if body_text:
        body_window = body_text[:6000]
        unique_amounts = {
            re.sub(r"\s+", " ", match.group(0)).strip().lower()
            for match in AMOUNT_PATTERN.finditer(body_window)
        }
        if len(unique_amounts) > 1:
            return True
    return False


def _round_uncertain(evidence: Evidence) -> bool:
    round_value = evidence.round_type or ""
    if round_value in {"", "Not Available"}:
        return True
    blob = f"{evidence.title} {evidence.amount}"
    return not any(pattern.search(blob) for pattern, _ in ROUND_PATTERNS)


def _country_uncertain(evidence: Evidence, body_text: str) -> bool:
    country = evidence.country or ""
    if country in {"", "Unknown"}:
        return True
    direct_terms = COUNTRY_TERMS.get(country, [])
    blob = f"{evidence.title} {evidence.url} {body_text[:6000]}".lower()
    if any(term in blob for term in direct_terms):
        return False
    return bool(COUNTRY_HINT_PATTERN.search(blob))


def _lead_investor_uncertain(evidence: Evidence) -> bool:
    lead = evidence.lead_investor or ""
    if lead in {"", "Not Stated"}:
        return False
    padded = f" {lead.lower()} "
    if any(marker in padded for marker in _PROSE_MARKERS):
        return True
    return ";" in lead and " led by" not in (evidence.title or "").lower()


def assess_evidence(evidence: Evidence, body_text: str = "") -> list[str]:
    flagged: list[str] = []
    if _startup_uncertain(evidence, body_text):
        flagged.append("startup")
    if _amount_uncertain(evidence, body_text):
        flagged.append("amount")
    if _round_uncertain(evidence):
        flagged.append("round")
    if _country_uncertain(evidence, body_text):
        flagged.append("country")
    if _lead_investor_uncertain(evidence):
        flagged.append("lead_investor")
    return flagged
