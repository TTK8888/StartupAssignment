#!/usr/bin/env python3
"""
Backwards-compatible shim. The implementation now lives in `main.py` and
the `funding_dataset/` package. This file is kept so existing callers that
invoke `python build_verified_funding_dataset.py` keep working.
"""

from __future__ import annotations

from main import main
from funding_dataset.http_client import shutdown_browser


if __name__ == "__main__":
    try:
        main()
    finally:
        shutdown_browser()
