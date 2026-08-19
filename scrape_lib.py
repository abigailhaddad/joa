#!/usr/bin/env python3
"""Fetching and text extraction, shared by scrape_2026.py and update_daily.py."""

import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
WS = re.compile(r"\s+")


def page_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return WS.sub(" ", soup.get_text(" ")).strip()


def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    return s


def scrape(controls: dict[str, str], workers: int = 12, pause: float = 0.25,
           on_progress=None) -> tuple[list[tuple[str, str, str]], list[str]]:
    """Fetch {control_number: open_date} -> ([(cn, open_date, text)], [failures]).

    A 404 yields an empty text rather than a failure — the page is gone and
    retrying won't bring it back.
    """
    session = new_session()
    lock = threading.Lock()
    rows: list[tuple[str, str, str]] = []
    failures: list[str] = []

    def fetch(cn: str) -> None:
        text = None
        for attempt in range(4):
            try:
                r = session.get(f"https://www.usajobs.gov/job/{cn}", timeout=45)
                if r.status_code == 404:
                    text = ""
                    break
                r.raise_for_status()
                text = page_text(r.text)
                if len(text) < 500:
                    raise ValueError(f"short page ({len(text)} chars)")
                break
            except Exception as e:
                if attempt == 3:
                    with lock:
                        failures.append(f"{cn},{e}")
                    return
                time.sleep(3 * (attempt + 1))
        time.sleep(pause * (0.5 + random.random()))
        with lock:
            rows.append((cn, controls[cn], text))
            if on_progress and len(rows) % 1000 == 0:
                on_progress(len(rows), len(failures))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(fetch, list(controls)))
    return rows, failures
