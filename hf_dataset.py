#!/usr/bin/env python3
"""
Shared bits for the HuggingFace dataset: repo id, layout, and the compaction
step that turns scrape_2026.py's part files into one parquet per month.

Each monthly file carries the scraped announcement text plus the structured
fields from the usajobs_historical mirror, so the dataset stands on its own —
nobody needs the R2 bucket to use it.

Layout in the repo:
    data/2026_01.parquet ... data/2026_08.parquet
    manifest.csv    every control number already scraped, so the daily job can
                    work out what's new without downloading the dataset
    README.md       dataset card
"""

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ID = os.environ.get("HF_DATASET_REPO", "abigailhaddad/usajobs-scraping")
SHARDS = Path("cache/scraped")
BUILD = Path("build")
METADATA = Path("work/metadata.parquet")   # intermediate; never uploaded

SCRAPE_SCHEMA = pa.schema([("usajobsControlNumber", pa.string()),
                           ("open_date", pa.string()),
                           ("text", pa.string())])


def _metadata():
    if not METADATA.exists():
        raise SystemExit(f"{METADATA} is missing — run build_metadata.py first.")
    df = pq.read_table(METADATA).to_pandas()
    return df.drop_duplicates("usajobsControlNumber")


def compact(shard_dir: Path = SHARDS, out_dir: Path = BUILD / "data") -> dict[str, int]:
    """One parquet per month: text joined to metadata, deduplicated, sorted."""
    import pandas as pd

    out_dir.mkdir(parents=True, exist_ok=True)
    meta = _metadata()

    months: dict[str, list[Path]] = {}
    for p in sorted(shard_dir.glob("*.parquet")):
        months.setdefault(p.name.split("-")[0], []).append(p)

    counts = {}
    for month, parts in sorted(months.items()):
        df = pa.concat_tables([pq.read_table(p, schema=SCRAPE_SCHEMA)
                               for p in parts]).to_pandas()
        df = df[df["text"].str.len() > 0]                    # drop 404s
        df = df.drop_duplicates("usajobsControlNumber")
        merged = (meta.merge(df[["usajobsControlNumber", "text"]],
                             on="usajobsControlNumber", how="inner")
                      .sort_values("usajobsControlNumber"))
        out = out_dir / f"{month}.parquet"
        pq.write_table(pa.Table.from_pandas(merged, preserve_index=False),
                       out, compression="zstd", compression_level=19)
        counts[month] = len(merged)
        print(f"  {out.name}: {len(merged):,} rows, {out.stat().st_size/1e6:.1f} MB",
              flush=True)
    return counts


def write_manifest(data_dir: Path = BUILD / "data",
                   dest: Path = BUILD / "manifest.csv") -> int:
    cns = []
    for p in sorted(data_dir.glob("*.parquet")):
        cns += pq.read_table(p, columns=["usajobsControlNumber"]).column(0).to_pylist()
    dest.write_text("usajobsControlNumber\n" + "\n".join(sorted(cns)) + "\n")
    return len(cns)


if __name__ == "__main__":
    counts = compact()
    n = write_manifest()
    print(f"total {sum(counts.values()):,} announcements, manifest {n:,}")
