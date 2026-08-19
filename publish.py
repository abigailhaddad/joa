#!/usr/bin/env python3
"""
Compact the scraped shards and push them to HuggingFace as a dataset.

Needs a write token: `huggingface-cli login`, or HF_TOKEN in the environment.
Override the destination with HF_DATASET_REPO.

    python3 publish.py            # compact, then upload
    python3 publish.py --dry-run  # compact and write build/, upload nothing
    python3 publish.py --no-build # upload build/ as it stands
"""

import argparse
import os
import sys
from pathlib import Path

from huggingface_hub import HfApi

from dataset import BUILD, REPO_ID, build_metadata, compact, write_manifest

CARD = """---
license: cc0-1.0
task_categories:
  - text-classification
  - feature-extraction
language:
  - en
tags:
  - usajobs
  - federal-hiring
  - government
pretty_name: USAJOBS Announcements 2026
size_categories:
  - 100K<n<1M
configs:
  - config_name: default
    data_files: data/*.parquet
---

# USAJOBS announcements, 2026

The full text of every federal job announcement opened in 2026, scraped from
usajobs.gov. {n:,} announcements, updated daily.

## Why this exists

The USAJOBS API is a poor source for announcement text, in two ways.

The Search API only lists jobs that are open right now, so anything that opens
and closes between two collection runs is never captured. Measured against the
Historical API, which does report closed postings, that's about 3% of 2026 —
5,274 announcements — and it isn't spread evenly. OPM had text for 159 of its
724 postings. SSA for 200 of 422.

For postings the API *does* return, `MatchedObjectDescriptor` drops content the
announcement page shows. Confirmed cases exist where the page says "Rule of
Many" and the API record does not.

Neither gap can be backfilled from the API afterward: the Historical API returns
metadata only, and Search only returns open jobs. So this scrapes the page,
which usajobs.gov serves indefinitely after an announcement closes.

## What's in it

One parquet file per month under `data/`, one row per announcement. Self
contained — you don't need any other source to use it.

The scraped column:

| column | |
|---|---|
| `text` | the announcement page as plain text, tags stripped and whitespace collapsed |

Everything else comes from the USAJOBS Historical API: `usajobsControlNumber`,
`announcementNumber`, `hiringAgencyCode`, `hiringAgencyName`,
`hiringDepartmentCode`, `hiringDepartmentName`, `agencyLevel`, `agencyLevelSort`,
`appointmentType`, `workSchedule`, `serviceType`, `whoMayApply`, `payScale`,
`salaryType`, `minimumSalary`, `maximumSalary`, `minimumGrade`, `maximumGrade`,
`promotionPotential`, `supervisoryStatus`, `totalOpenings`,
`positionOpeningStatus`, `announcementClosingTypeCode`,
`announcementClosingTypeDescription`, `positionOpenDate`, `positionCloseDate`,
`positionExpireDate`, `travelRequirement`, `teleworkEligible`,
`relocationExpensesReimbursed`, `securityClearanceRequired`,
`securityClearance`, `drugTestRequired`, `disableApplyOnline`, `vendor`,
`hiringPaths`, `jobCategories`, `positionLocations`, and `occupationalSeries`
(the four-digit series pulled out of `jobCategories`).

`hiringPaths`, `jobCategories`, and `positionLocations` are JSON strings, since
one posting can carry several of each.

## How it's built

The posting list comes from the USAJOBS Historical API, mirrored by
[usajobs_historical](https://github.com/abigailhaddad/usajobs_historical), which
reports closed announcements and is therefore the complete universe. Each
`usajobs.gov/job/{{controlNumber}}` page is fetched, `script`/`style` stripped,
and the remaining text collapsed to single spaces. The structured fields are
joined on `usajobsControlNumber` from the same Historical API.

A daily job scrapes whatever is in the Historical mirror and not yet here, and
re-joins metadata for the months it touches. Close dates and opening status
change after a posting first appears, so a monthly job refreshes every month's
metadata.

## Caveats

The text is the rendered page, so it includes site chrome — the nav bar, the
footer, the cookie language. Anchor your searches accordingly.

A handful of announcements are agency test postings with placeholder content
("There will be duties to perform for this position").

Announcements can be amended after posting. Each page is fetched once, so the
text is whatever it said when scraped, not necessarily the final version. The
structured fields are refreshed on a schedule, the text isn't.

This is not an official USAJOBS project.

## License

Work of the U.S. federal government, not subject to copyright.
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--repo", default=REPO_ID)
    ap.add_argument("--no-build", action="store_true",
                    help="upload build/ as it stands, skipping compaction")
    args = ap.parse_args()

    if args.no_build:
        import pyarrow.parquet as pq
        counts = {p.stem: pq.read_metadata(p).num_rows
                  for p in sorted((BUILD / "data").glob("*.parquet"))}
        n = sum(1 for _ in (BUILD / "manifest.csv").open()) - 1
    else:
        build_metadata()
        print("compacting shards ...", flush=True)
        counts = compact()
        n = write_manifest()
    print(f"{sum(counts.values()):,} announcements across {len(counts)} months")

    (BUILD / "README.md").write_text(CARD.format(n=n, repo=args.repo))
    size = sum(p.stat().st_size for p in BUILD.rglob("*") if p.is_file())
    print(f"build/ is {size/1e6:.0f} MB")

    if args.dry_run:
        print("dry run, nothing uploaded")
        return

    token = os.environ.get("HF_TOKEN")
    api = HfApi(token=token)
    try:
        api.whoami()
    except Exception:
        sys.exit("No HuggingFace credentials. Run `huggingface-cli login` or set HF_TOKEN.")

    api.create_repo(args.repo, repo_type="dataset", exist_ok=True)
    api.upload_folder(repo_id=args.repo, repo_type="dataset",
                      folder_path=str(BUILD),
                      commit_message=f"{sum(counts.values()):,} announcements, 2026")
    print(f"https://huggingface.co/datasets/{args.repo}")


if __name__ == "__main__":
    main()
