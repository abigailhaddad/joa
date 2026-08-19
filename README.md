# joa

Which 2026 USAJOBS announcements use "Rule of Many" — the referral method where
everyone tied at a cut score goes to the selecting official, instead of only the
top three names.

Two things live here: a scraper that publishes every 2026 announcement to
HuggingFace, and the analysis that runs against it.

## What we found

659 announcements printed the phrase. **405 use the method. 254 name it to say
it doesn't apply.**

That second group is almost all direct-hire-authority announcements listing what
they're exempt from:

> Under the provisions of the Direct Hire Authority, Veterans Preference and the
> "Rule of Many" do not apply.

A raw phrase match counts those as adopters, and they're 39% of the matches.
Every single one of SSA's 79 mentions is a negation, as are all 60 at Federal
Highway, all 38 at the National Gallery of Art, and all 28 at Federal Transit.
None of those four agencies uses the method at all.

Among the 405 that do, it climbed hard through the spring:

| month | uses it | says it doesn't apply | announcements |
|-------|----|----|--------|
| Jan | 1 | 3 | 16,173 |
| Feb | 7 | 6 | 18,837 |
| Mar | 27 | 27 | 23,124 |
| Apr | 41 | 62 | 23,845 |
| May | 116 | 39 | 21,901 |
| Jun | 82 | 20 | 21,965 |
| Jul | 87 | 75 | 22,597 |
| Aug | 44 | 22 | 13,588 |

August runs through the 19th. "Rule of three" held flat at 136–258 a month the
whole time.

NLRB accounts for 138 of the 405, then FCC (43), NARA (20), Western Area Power
(19), DFC (18), EEOC (17), Fiscal Service (15), Treasury Departmental Offices
(14). The earliest is a DOJ paralegal announcement from January
([855359900](https://www.usajobs.gov/job/855359900)):

> Your application will be evaluated and rated under using the Rule of Many
> method under 5 CFR § 332.402. All applicants tied at the score of 85 will be
> referred.

## Why there's a scraper

The USAJOBS API is the wrong source for announcement text, in two ways.

The Search API only lists jobs open right now, so anything that opens and closes
between collection runs is never captured. Measured against the Historical API,
which does report closed postings, that's 5,274 of 2026's 162,030 announcements —
and it isn't spread evenly. OPM had text for 159 of its 724 postings.

For postings the API does return, `MatchedObjectDescriptor` drops content the
page shows. There are confirmed cases where the page says "Rule of Many" and the
API record doesn't.

Together those cost a lot. The API text finds 502 mentions. The scraped pages
find 659 — a 31% undercount, and it hides whole agencies: Federal Highway and
Federal Transit don't appear in the API results at all.

Neither gap can be backfilled from the API afterward. The Historical API returns
metadata only, and Search returns open jobs only. usajobs.gov serves closed
announcements indefinitely, so the page is the source.

## The dataset

[`abigailhaddad/usajobs-scraping`](https://huggingface.co/datasets/abigailhaddad/usajobs-scraping) —
every 2026 announcement, page text plus 39 structured fields from the Historical
API, one parquet per month, updated daily. 162,030 rows, 241 MB.

```bash
./find_controls.sh    # the posting list, from the historical mirror
python3 scrape_2026.py  # ~2 hours for a full year at 1,350 pages/min
python3 publish_hf.py --dry-run   # build/, uploading nothing
python3 publish_hf.py             # create the repo and push, one commit
```

`scrape_2026.py` is resumable and crash-safe. Shards are immutable part files
tagged with a per-run id, and a rerun skips every control number already stored,
so you can kill it whenever.

Daily updates run inside the
[usajobs_historical](https://github.com/abigailhaddad/usajobs_historical)
pipeline rather than reaching for R2 themselves — see `PIPELINE.md`.

## The analysis

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/abigailhaddad/joa/blob/main/rule_of_many.ipynb)

`rule_of_many.ipynb` is the readable version — the query, the two kinds of
mention side by side, the monthly chart, and the agencies that only ever
disclaim the rule. It runs top to bottom in about a minute.

The same thing from the command line:

```bash
python3 rule_of_many.py            # query, classify, write results/
python3 rule_of_many.py --cached   # reclassify results/ without re-querying
```

Both go through `rule_of_many.py`, which queries
`hf://datasets/abigailhaddad/usajobs-scraping/data/*.parquet` and downloads
nothing but the matches. Pass `--dataset` (or `rom.fetch(dataset=...)`) to point
somewhere else.

Every rule for reading a mention is in `patterns.yaml`. Edit it and reclassify
off the saved contexts — nothing is re-queried. A posting counts as `applied` if
any occurrence reads that way, `negated` if all of them read as negations,
`unclear` if no rule fires. Currently nothing is unclear, and no posting contains
both readings.

## Output

- `results/rule_of_many.csv` — one row per announcement, with `status`,
  occurrence count, agency, series, dates, and a link
- `results/contexts.csv` — every occurrence with ±300 characters, what the
  classifier reads
- `results/hits.csv` — the raw matches before classification
- `results/monthly_counts.csv` — postings, "rule of many", "rule of three" by month

## Gotchas

The phrase has to be printed. Announcements describe how they'll refer
applicants without naming the method all the time, so a zero for an agency means
it doesn't use the words.

The scraped text is the rendered page, so it carries site chrome — nav, footer,
cookie language. Anchor searches accordingly.

A handful of announcements are agency test postings with placeholder content
("There will be duties to perform for this position").

## Layout

- `find_controls.sh` — the 2026 posting list from the historical mirror
- `scrape_2026.py`, `scrape_lib.py` — the scrape
- `build_metadata.py` — structured fields, local pipeline copy first, public URL as fallback
- `hf_dataset.py` — compaction into monthly parquets plus the manifest
- `publish_hf.py` — build and push
- `update_daily.py` — the daily top-up, one commit per run
- `rule_of_many.ipynb` — the analysis, with charts
- `rule_of_many.py`, `patterns.yaml` — the query and the classification rules
- `PIPELINE.md` — wiring the daily update into usajobs_historical
