from __future__ import annotations

import time
from urllib.robotparser import RobotFileParser

import requests

from .settings import USER_AGENT, SourceConfig


def fetch(url: str, session: requests.Session) -> str:
    """Fetch a URL with the configured user agent and one retry for transient failures."""
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            response = session.get(
                url,
                timeout=30,
                headers={"User-Agent": USER_AGENT},
            )
            if response.status_code >= 500:
                last_error = requests.HTTPError(f"{response.status_code} for {url}")
                if attempt == 0:
                    time.sleep(2)
                    continue
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            if attempt == 0:
                time.sleep(2)
                continue
            raise
    raise last_error if last_error else requests.RequestException(f"fetch failed: {url}")


# lazy playwright state for sources that need rendered html
_BROWSER_STATE: dict = {"playwright": None, "browser": None, "context": None}


def _get_browser_context():
    if _BROWSER_STATE["context"] is not None:
        return _BROWSER_STATE["context"]
    from playwright.sync_api import sync_playwright

    pw = sync_playwright().start()
    browser = pw.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled"],
    )
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 800},
    )
    _BROWSER_STATE.update(playwright=pw, browser=browser, context=context)
    return context


def shutdown_browser() -> None:
    """Close and clear the shared Playwright browser state if it was opened."""
    if _BROWSER_STATE["browser"] is not None:
        try:
            _BROWSER_STATE["browser"].close()
        except Exception:
            pass
    if _BROWSER_STATE["playwright"] is not None:
        try:
            _BROWSER_STATE["playwright"].stop()
        except Exception:
            pass
    _BROWSER_STATE.update(playwright=None, browser=None, context=None)


def fetch_with_browser(url: str) -> str:
    """Fetch rendered HTML for a URL using the shared Playwright browser context."""
    context = _get_browser_context()
    page = context.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        # allow challenge pages and spa hydration to settle
        page.wait_for_timeout(5000)
        return page.content()
    finally:
        page.close()


def fetch_html(
    url: str,
    session: requests.Session,
    source: SourceConfig | None,
) -> str:
    """Fetch HTML through Playwright only when the source requires rendered content."""
    if source is not None and source.needs_browser:
        return fetch_with_browser(url)
    return fetch(url, session)


def robot_parser_for(
    source: SourceConfig,
    session: requests.Session,
    robots_cache: dict[str, RobotFileParser],
) -> RobotFileParser:
    """Return a cached robots.txt parser for a source domain."""
    domain = source.allowed_domains[0]
    if domain in robots_cache:
        return robots_cache[domain]
    robots_url = f"https://{domain}/robots.txt"
    parser = RobotFileParser()
    parser.set_url(robots_url)
    try:
        response = session.get(robots_url, timeout=10, headers={"User-Agent": USER_AGENT})
        if response.ok:
            parser.parse(response.text.splitlines())
        else:
            parser.parse([])
    except requests.RequestException:
        parser.parse([])
    robots_cache[domain] = parser
    return parser


def can_fetch_url(
    url: str,
    source: SourceConfig,
    session: requests.Session,
    robots_cache: dict[str, RobotFileParser],
) -> bool:
    """Return whether robots.txt allows the configured user agent to fetch a URL."""
    parser = robot_parser_for(source, session, robots_cache)
    return parser.can_fetch(USER_AGENT, url)
