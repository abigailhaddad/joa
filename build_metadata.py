#!/usr/bin/env python3
"""
Pull the structured fields for every 2026 posting out of the usajobs_historical
data, so the published dataset is self-contained. Anyone using it gets agency,
series, grade, salary, and location without needing the R2 bucket.

Source resolution, in order:

  1. --source, or the USAJOBS_HISTORICAL_2026 environment variable
  2. data/historical_jobs_2026.parquet — where the usajobs_historical pipeline
     puts it after its own R2 download step. Running inside that pipeline picks
     up the file it just refreshed, no second fetch and no R2 credentials here.
  3. ../usajobs_historical/data/historical_jobs_2026.parquet, for a local
     checkout sitting next to this one
  4. the public R2 URL, so this works for anyone without the bucket

Writes build/metadata.parquet, which hf_dataset.compact() joins onto the
scraped text by usajobsControlNumber.
"""

import argparse
import os
from pathlib import Path

import duckdb

PUBLIC_URL = ("https://pub-317c58882ec04f329b63842c1eb65b0c.r2.dev/data/"
              "historical_jobs_2026.parquet")
LOCAL_CANDIDATES = [
    Path("data/historical_jobs_2026.parquet"),
    Path("../usajobs_historical/data/historical_jobs_2026.parquet"),
]
OUT = Path("work/metadata.parquet")

# Dropped from the mirror's 46 columns: usajobs_control_number (duplicates
# usajobsControlNumber), HiringPaths / JobCategories / PositionLocations (empty
# integer columns superseded by the *_1 varchars below), and inserted_at /
# last_seen (bookkeeping for the pipeline's own collection runs).
SELECT = """
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
    array_to_string(regexp_extract_all(coalesce(jobcategories_1, ''), '[0-9]{4}'), ' | ') AS occupationalSeries
"""


def resolve_source(explicit: str | None = None) -> str:
    src = explicit or os.environ.get("USAJOBS_HISTORICAL_2026")
    if src:
        return src
    for p in LOCAL_CANDIDATES:
        if p.exists():
            return str(p)
    return PUBLIC_URL


def main(source: str | None = None) -> str:
    src = resolve_source(source)
    print(f"metadata source: {src}", flush=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("LOAD httpfs; SET http_retries=5;")
    con.execute(f"""
        COPY (
          SELECT {SELECT}
          FROM read_parquet('{src}')
          WHERE substr(positionOpenDate, 1, 4) = '2026'
        ) TO '{OUT}' (FORMAT parquet, COMPRESSION zstd);
    """)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{OUT}')").fetchone()[0]
    print(f"{OUT}: {n:,} postings, {OUT.stat().st_size/1e6:.1f} MB")
    return src


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", help="parquet path or URL; overrides the search order")
    main(ap.parse_args().source)
