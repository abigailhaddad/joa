#!/usr/bin/env python3
"""
Scrape the announcement page for every 2026 USAJOBS posting.

Why scrape at all: the USAJOBS API is the wrong source for full text twice over.
It only lists jobs that are open right now, so ~3% of 2026 postings were never
captured. And for postings it did capture, MatchedObjectDescriptor drops content
the page shows -- Federal Highway has 60 announcements naming the "Rule of Many"
and the API record contains the phrase in none of them. usajobs.gov serves closed
announcements indefinitely, so the page is the complete source.

The posting list comes from work/controls_2026.csv (dataset.py writes it).
Output is monthly parquet shards under cache/scraped/, zstd-compressed -- about
250MB for all of 2026, versus 1.3GB as individual gzipped files.

Resumable and crash-safe: shards are immutable part files tagged with a per-run
id, and a rerun skips every control number already in one. Kill it whenever.

    python3 dataset.py            # write the posting list first
    python3 scrape.py             # everything not yet fetched, ~2h for a year
    python3 scrape.py --workers 8 # gentler
    python3 scrape.py --limit 500 # a taste

update_daily.py imports scrape() from here for its nightly top-up.
"""

import argparse
import csv
import random
import re
import sys
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

from dataset import CONTROLS, SCRAPE_SCHEMA, SHARDS

FAILURES = Path("results/scrape_failures.csv")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
BATCH = 2000                          # rows per part file
WS = re.compile(r"\s+")

# Part files must not collide across runs -- a plain counter restarts every
# process, so without a per-run tag a rerun overwrites an earlier run's parts.
_RUN = uuid.uuid4().hex[:8]
_write_lock = threading.Lock()
_part_no = 0


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

    A 404 yields empty text rather than a failure -- the page is gone and
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


def already_have() -> set[str]:
    SHARDS.mkdir(parents=True, exist_ok=True)
    have = set()
    for p in SHARDS.glob("*.parquet"):
        try:
            have.update(pq.read_table(p, columns=["usajobsControlNumber"])
                          .column(0).to_pylist())
        except Exception as e:
            print(f"  unreadable shard {p.name} ({e}), ignoring", flush=True)
    return have


def write_batch(batch: list[tuple[str, str, str]]) -> None:
    global _part_no
    by_month = defaultdict(list)
    for cn, od, text in batch:
        by_month[od[:7].replace("-", "_")].append((cn, od, text))
    with _write_lock:
        _part_no += 1
        n = _part_no
    for month, rows in by_month.items():
        t = pa.Table.from_arrays(
            [pa.array([r[0] for r in rows]), pa.array([r[1] for r in rows]),
             pa.array([r[2] for r in rows])], schema=SCRAPE_SCHEMA)
        pq.write_table(t, SHARDS / f"{month}-part-{_RUN}-{n:05d}.parquet",
                       compression="zstd", compression_level=19)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.25,
                    help="per-request pause, seconds (jittered)")
    args = ap.parse_args()

    if not CONTROLS.exists():
        sys.exit(f"{CONTROLS} is missing — run dataset.py first.")
    with CONTROLS.open() as f:
        rows = {r["cn"]: r["od"] for r in csv.DictReader(f)}
    have = already_have()
    todo = [cn for cn in rows if cn not in have]
    if args.limit:
        todo = todo[:args.limit]
    print(f"{len(rows):,} postings in 2026, {len(have):,} cached, {len(todo):,} to fetch",
          flush=True)
    if not todo:
        return

    started = time.time()
    batch: list[tuple[str, str, str]] = []

    def progress(done, failed):
        rate = done / (time.time() - started) * 60
        left = (len(todo) - done) / rate if rate else 0
        print(f"  {done:,}/{len(todo):,}  {rate:,.0f}/min  ~{left:,.0f} min left  "
              f"{failed} failed", flush=True)

    fetched, failures = scrape({cn: rows[cn] for cn in todo},
                               workers=args.workers, pause=args.sleep,
                               on_progress=progress)
    for i in range(0, len(fetched), BATCH):
        write_batch(fetched[i:i + BATCH])

    if failures:
        FAILURES.parent.mkdir(exist_ok=True)
        FAILURES.write_text("usajobsControlNumber,error\n" + "\n".join(failures) + "\n")
        print(f"{len(failures):,} failed, listed in {FAILURES}. Rerun to retry them.")
    print(f"done: {len(fetched):,} fetched in {(time.time()-started)/60:.1f} min")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
