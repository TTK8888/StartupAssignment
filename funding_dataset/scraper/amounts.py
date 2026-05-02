from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from ..settings import AMOUNT_PATTERN, NON_USD_TO_USD_RATE, USD_TO_BDT_BY_YEAR
from ..text_utils import clean_text


_MAGNITUDE_PATTERN = re.compile(r"\b(?:million|billion|crore|lakh|mn|m|bn|b)\b", re.IGNORECASE)
_FUNDING_CONTEXT_PATTERN = re.compile(
    r"\b(?:raise|raises|raised|raising|secure|secures|secured|funding|round|investment|financing)\b",
    re.IGNORECASE,
)


def _amount_candidate_score(text: str, match: re.Match[str]) -> tuple[int, int, int]:
    raw_amount = match.group(0)
    start = max(0, match.start() - 80)
    end = min(len(text), match.end() + 80)
    context = text[start:end]
    score = 0
    if _MAGNITUDE_PATTERN.search(raw_amount):
        score += 100
    if _FUNDING_CONTEXT_PATTERN.search(context):
        score += 25
    if _amount_currency(raw_amount):
        score += 10
    return score, len(raw_amount), -match.start()


def amount_from_text(text: str) -> str:
    if re.search(r"\bundisclosed\b", text, re.IGNORECASE):
        return "Undisclosed"
    matches = list(AMOUNT_PATTERN.finditer(text))
    if not matches:
        return "Undisclosed"
    match = max(matches, key=lambda candidate: _amount_candidate_score(text, candidate))
    return clean_text(match.group(0))


def usd_to_bdt_rate_for_year(year: int | None) -> Decimal:
    if year in USD_TO_BDT_BY_YEAR:
        return USD_TO_BDT_BY_YEAR[year]
    if year is None:
        return USD_TO_BDT_BY_YEAR[max(USD_TO_BDT_BY_YEAR)]
    nearest_year = min(USD_TO_BDT_BY_YEAR, key=lambda known_year: abs(known_year - year))
    return USD_TO_BDT_BY_YEAR[nearest_year]


def _amount_year(published: str) -> int | None:
    from .articles import parse_published_date

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
