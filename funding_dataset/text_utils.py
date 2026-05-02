from __future__ import annotations

import re


def clean_text(value: str) -> str:
    """Collapse repeated whitespace and trim surrounding whitespace."""
    return re.sub(r"\s+", " ", value).strip()
