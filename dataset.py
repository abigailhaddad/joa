#!/usr/bin/env python3
"""
Where the published dataset comes from: the posting list, the structured fields,
and the compaction that turns scraped shards into one parquet per month.

Layout in the HuggingFace repo:
    data/2026_01.parquet ... data/2026_08.parquet
    manifest.csv    every control number already scraped, so the daily job can
                    work out what's new by downloading 2MB instead of 250
    README.md       dataset card

Run it directly to write reference/controls_2026.csv and work/metadata.parquet:

    python3 dataset.py
"""

import argparse
import os
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ID = os.environ.get("HF_DATASET_REPO", "abigailhaddad/usajobs-scraping")
SHARDS = Path("cache/scraped")
BUILD = Path("build")
WORK = Path("work")
METADATA = WORK / "metadata.parquet"        # intermediate; never uploaded
CONTROLS = WORK / "controls_2026.csv"

SCRAPE_SCHEMA = pa.schema([("usajobsControlNumber", pa.string()),
                           ("open_date", pa.string()),
                           ("text", pa.string())])

# The usajobs_historical mirror. Metadata only -- no announcement text -- but it
# reports closed postings, so it's the complete list of what exists.
PUBLIC_URL = ("https://pub-317c58882ec04f329b63842c1eb65b0c.r2.dev/data/"
              "historical_jobs_2026.parquet")
LOCAL_CANDIDATES = [
    Path("data/historical_jobs_2026.parquet"),              # inside the pipeline
    Path("../usajobs_historical/data/historical_jobs_2026.parquet"),
]

# Dropped from the mirror's 46 columns: usajobs_control_number (duplicates
# usajobsControlNumber), HiringPaths / JobCategories / PositionLocations (empty
# integer columns superseded by the *_1 varchars below), and inserted_at /
# last_seen (bookkeeping for the pipeline's own collection runs).
FIELDS = """
    usajobsControlNumber::varchar AS usajobsControlNumber,
    announcementNumber,
    hiringAgencyCode, hiringAgencyName,
    hiringDepartmentCode, hiringDepartmentName,
    agencyLevel, agencyLevelSort,
    appointmentType, workSchedule, serviceType, whoMayApply,
    payScale, salaryType, minimumSalary, maximumSalary,
    minimumGrade, maximumGrade, promotionPotential, supervisoryStatus,
    totalOpenings, positionOpeningStatus,
    announcementClosingTypeCode, announcementClosingTypeDescription,
    substr(positionOpenDate, 1, 10)   AS positionOpenDate,
    substr(positionCloseDate, 1, 10)  AS positionCloseDate,
    substr(positionExpireDate, 1, 10) AS positionExpireDate,
    travelRequirement, teleworkEligible, relocationExpensesReimbursed,
    securityClearanceRequired, securityClearance, drugTestRequired,
    disableApplyOnline, vendor,
    hiringpaths_1        AS hiringPaths,
    jobcategories_1      AS jobCategories,
    positionlocations_1  AS positionLocations,
    array_to_string(regexp_extract_all(coalesce(jobcategories_1, ''), '[0-9]{4}'), ' | ')
        AS occupationalSeries
"""


def source(explicit: str | None = None) -> str:
    """Prefer the pipeline's local copy: running inside usajobs_historical picks
    up the file that run just refreshed, no R2 credentials and no second fetch."""
    if explicit or os.environ.get("USAJOBS_HISTORICAL_2026"):
        return explicit or os.environ["USAJOBS_HISTORICAL_2026"]
    for p in LOCAL_CANDIDATES:
        if p.exists():
            return str(p)
    return PUBLIC_URL


def build_metadata(src: str | None = None) -> str:
    """Structured fields for every 2026 posting, plus the control-number list."""
    src = source(src)
    print(f"metadata source: {src}", flush=True)
    WORK.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; SET http_retries=5;")
    con.execute(f"""
        COPY (SELECT {FIELDS} FROM read_parquet('{src}')
              WHERE substr(positionOpenDate, 1, 4) = '2026')
        TO '{METADATA}' (FORMAT parquet, COMPRESSION zstd);
    """)
    con.execute(f"""
        COPY (SELECT usajobsControlNumber AS cn, positionOpenDate AS od,
                     hiringAgencyName AS agency, announcementNumber AS title
              FROM read_parquet('{METADATA}') ORDER BY od, cn)
        TO '{CONTROLS}' (HEADER);
    """)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{METADATA}')").fetchone()[0]
    print(f"{METADATA}: {n:,} postings, {METADATA.stat().st_size/1e6:.1f} MB")
    return src


def _metadata_frame():
    if not METADATA.exists():
        raise SystemExit(f"{METADATA} is missing — run dataset.py first.")
    return pq.read_table(METADATA).to_pandas().drop_duplicates("usajobsControlNumber")


def compact(shard_dir: Path = SHARDS, out_dir: Path = BUILD / "data") -> dict[str, int]:
    """One parquet per month: scraped text joined to metadata, deduped, sorted."""
    out_dir.mkdir(parents=True, exist_ok=True)
    meta = _metadata_frame()

    months: dict[str, list[Path]] = {}
    for p in sorted(shard_dir.glob("*.parquet")):
        months.setdefault(p.name.split("-")[0], []).append(p)

    counts = {}
    for month, parts in sorted(months.items()):
        df = pa.concat_tables([pq.read_table(p, schema=SCRAPE_SCHEMA)
                               for p in parts]).to_pandas()
        df = df[df["text"].str.len() > 0].drop_duplicates("usajobsControlNumber")
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", help="parquet path or URL; overrides the search order")
    build_metadata(ap.parse_args().source)
