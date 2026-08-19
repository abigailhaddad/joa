#!/usr/bin/env python3
"""
Scrape the announcement page for every 2026 USAJOBS posting.

Why scrape at all: the USAJOBS API is the wrong source for full text twice over.
It only lists jobs that are open right now, so ~3% of 2026 postings were never
captured. And for postings it did capture, MatchedObjectDescriptor drops content
the page shows -- we have confirmed cases where the page says "Rule of Many" and
the API record doesn't. usajobs.gov serves closed announcements indefinitely, so
the page is the complete source.

Input is reference/controls_2026.csv (see find_controls.sh). Output is monthly
parquet shards under cache/scraped/, zstd-compressed -- about 210MB for all of
2026, versus 1.3GB as individual gzipped files.

Resumable and crash-safe: shards are written as immutable part files, and a
rerun skips every control number already present in one. Kill it whenever.

    python3 scrape_2026.py                 # everything not yet fetched
    python3 scrape_2026.py --workers 8     # gentler
    python3 scrape_2026.py --limit 500     # a taste
"""

import argparse
import csv
import gzip
import random
import re
import sys
import threading
import time
import uuid
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

CONTROLS = Path("reference/controls_2026.csv")
SHARDS = Path("cache/scraped")
LEGACY = Path("cache/pages")          # earlier per-file gzip cache, imported once
FAILURES = Path("results/scrape_failures.csv")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
BATCH = 2000                          # rows per part file
WS = re.compile(r"\s+")

SCHEMA = pa.schema([("usajobsControlNumber", pa.string()),
                    ("open_date", pa.string()),
                    ("text", pa.string())])


def page_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return WS.sub(" ", soup.get_text(" ")).strip()


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


def import_legacy(rows: dict[str, str]) -> int:
    """Fold the old cache/pages/*.txt.gz files into the shards, once."""
    files = list(LEGACY.glob("*.txt.gz"))
    if not files:
        return 0
    batch = []
    for f in files:
        cn = f.name.split(".")[0]
        if cn not in rows:
            continue
        text = gzip.open(f, "rb").read().decode()
        if text:
            batch.append((cn, rows[cn], text))
    if batch:
        write_batch(batch, tag="legacy")
    return len(batch)


_write_lock = threading.Lock()
_part_no = 0
# Part files must not collide across runs -- the counter restarts every process,
# so without a per-run tag a rerun silently overwrites an earlier run's parts.
_RUN = uuid.uuid4().hex[:8]


def write_batch(batch: list[tuple[str, str, str]], tag: str = "") -> None:
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
             pa.array([r[2] for r in rows])], schema=SCHEMA)
        name = f"{month}-{tag or 'part'}-{_RUN}-{n:05d}.parquet"
        pq.write_table(t, SHARDS / name, compression="zstd", compression_level=19)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.15,
                    help="per-request pause, seconds (jittered)")
    args = ap.parse_args()

    with CONTROLS.open() as f:
        rows = {r["cn"]: r["od"] for r in csv.DictReader(f)}
    print(f"{len(rows):,} postings in 2026", flush=True)

    have = already_have()
    moved = import_legacy({k: v for k, v in rows.items() if k not in have})
    if moved:
        print(f"imported {moved:,} pages from the old {LEGACY}/ cache", flush=True)
        have = already_have()

    todo = [cn for cn in rows if cn not in have]
    if args.limit:
        todo = todo[:args.limit]
    print(f"{len(have):,} already cached, {len(todo):,} to fetch", flush=True)
    if not todo:
        return

    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    lock = threading.Lock()
    batch: list[tuple[str, str, str]] = []
    failures: list[str] = []
    done = 0
    started = time.time()

    def fetch(cn: str) -> None:
        nonlocal done, batch
        text = None
        for attempt in range(4):
            try:
                r = session.get(f"https://www.usajobs.gov/job/{cn}", timeout=45)
                if r.status_code == 404:
                    text = ""            # page is gone; record the miss, don't retry
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
        time.sleep(args.sleep * (0.5 + random.random()))
        with lock:
            batch.append((cn, rows[cn], text))
            done += 1
            if len(batch) >= BATCH:
                write_batch(batch)
                batch = []
            if done % 1000 == 0:
                rate = done / (time.time() - started) * 60
                left = (len(todo) - done) / rate if rate else 0
                print(f"  {done:,}/{len(todo):,}  {rate:,.0f}/min  "
                      f"~{left:,.0f} min left  {len(failures)} failed", flush=True)

    try:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            list(pool.map(fetch, todo))
    finally:
        if batch:
            write_batch(batch)

    if failures:
        FAILURES.parent.mkdir(exist_ok=True)
        FAILURES.write_text("usajobsControlNumber,error\n" + "\n".join(failures) + "\n")
        print(f"{len(failures):,} failed, listed in {FAILURES}. Rerun to retry them.")
    print(f"done: {done:,} fetched in {(time.time()-started)/60:.1f} min")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
