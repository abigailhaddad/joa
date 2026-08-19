#!/usr/bin/env python3
"""
Daily top-up for the HuggingFace dataset.

Diffs the USAJOBS Historical data against the dataset's manifest, scrapes only
the pages that are new, and pushes the months that changed. Downloads a few MB
rather than the whole dataset.

Built to run as a step inside the usajobs_historical pipeline, after its own R2
download and collection have finished: it reads data/historical_jobs_2026.parquet
straight off disk, so it needs no R2 credentials and makes no second fetch. Run
standalone and it falls back to the public R2 URL.

The Historical API reports closed announcements, so a job that opened and closed
inside a single day still shows up — which is the gap this dataset exists to
close.

Metadata (close dates, opening status) keeps changing after a posting appears,
so the daily run also refreshes the months it touches. --refresh-all rewrites
every month with current metadata; run it on a slower schedule, since it
re-uploads the whole dataset.

    python3 update_daily.py
    python3 update_daily.py --dry-run       # report what's new, scrape nothing
    python3 update_daily.py --refresh-all   # plus rewrite every month's metadata
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_download

from dataset import BUILD, METADATA, REPO_ID, build_metadata
from scrape import scrape


def month_of(open_date: str) -> str:
    return open_date[:7].replace("-", "_")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--refresh-all", action="store_true")
    ap.add_argument("--repo", default=REPO_ID)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--source", help="historical parquet path or URL "
                                     "(default: the pipeline's local data/ copy)")
    args = ap.parse_args()

    token = os.environ.get("HF_TOKEN")
    api = HfApi(token=token)
    BUILD.mkdir(exist_ok=True)

    have = set(pd.read_csv(
        hf_hub_download(args.repo, "manifest.csv", repo_type="dataset", token=token),
        dtype=str)["usajobsControlNumber"])
    print(f"dataset holds {len(have):,} announcements")

    build_metadata(args.source)
    meta = pq.read_table(METADATA).to_pandas().drop_duplicates("usajobsControlNumber")
    todo = {r.usajobsControlNumber: r.positionOpenDate
            for r in meta.itertuples() if r.usajobsControlNumber not in have}
    print(f"historical mirror has {len(meta):,}; {len(todo):,} are new")

    if args.dry_run:
        for cn, od in sorted(todo.items(), key=lambda kv: kv[1])[:20]:
            print(f"  {od}  {cn}")
        return
    if not todo and not args.refresh_all:
        print("nothing to do")
        return

    rows = []
    failures = []
    if todo:
        rows, failures = scrape(
            todo, workers=args.workers,
            on_progress=lambda n, f: print(f"  {n:,} fetched, {f} failed", flush=True))
        rows = [r for r in rows if r[2]]
        print(f"scraped {len(rows):,}, {len(failures)} failed")

    new = pd.DataFrame(rows, columns=["usajobsControlNumber", "open_date", "text"])
    touched = set(new.open_date.map(month_of)) if len(new) else set()
    if args.refresh_all:
        touched |= {month_of(d) for d in meta.positionOpenDate.dropna()}

    changed = []
    for month in sorted(touched):
        name = f"data/{month}.parquet"
        try:
            existing = pq.read_table(
                hf_hub_download(args.repo, name, repo_type="dataset", token=token)
            ).to_pandas()
        except Exception:
            existing = pd.DataFrame(columns=["usajobsControlNumber", "text"])

        text = pd.concat([
            existing[["usajobsControlNumber", "text"]],
            new.loc[new.open_date.map(month_of) == month, ["usajobsControlNumber", "text"]],
        ]).drop_duplicates("usajobsControlNumber")

        # Metadata is re-joined rather than carried forward, so close dates and
        # opening status stay current for every month this run touches.
        merged = (meta.merge(text, on="usajobsControlNumber", how="inner")
                      .sort_values("usajobsControlNumber"))
        if len(merged) == len(existing) and not args.refresh_all:
            continue
        dest = BUILD / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(merged, preserve_index=False),
                       dest, compression="zstd", compression_level=19)
        changed.append(name)
        print(f"  {name}: {len(existing):,} -> {len(merged):,}")

    if not changed:
        print("no month changed")
        return

    manifest = sorted(have | {r[0] for r in rows})
    (BUILD / "manifest.csv").write_text(
        "usajobsControlNumber\n" + "\n".join(manifest) + "\n")

    # One commit for everything. Uploading file by file would mean a commit per
    # month plus another for the manifest, and would leave the dataset briefly
    # inconsistent between them.
    ops = [CommitOperationAdd(path_in_repo=n, path_or_fileobj=str(BUILD / n))
           for n in changed + ["manifest.csv"]]
    api.create_commit(repo_id=args.repo, repo_type="dataset", operations=ops,
                      commit_message=f"+{len(rows):,} announcements, "
                                     f"{len(manifest):,} total")
    print(f"pushed {len(rows):,} new announcements to {args.repo}")
    if failures:
        print(f"{len(failures)} failed; tomorrow's run retries them")
        sys.exit(1)


if __name__ == "__main__":
    main()
