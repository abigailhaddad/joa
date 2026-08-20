# joa

Which 2026 USAJOBS announcements use "Rule of Many" — the referral method where
everyone tied at a cut score goes to the selecting official, instead of only the
top three names.

Two things live here: a scraper that publishes every 2026 announcement to
HuggingFace, and the analysis that runs against it.

## What we found

721 announcements named it. **467 use the method. 254 name it to say it doesn't
apply.** That's 0.29% of the 162,030 announcements posted in 2026, peaking at
0.58% in May.

Agencies don't call it one thing. Some use "Rule of Many" as an umbrella term,
others name the specific OPM mechanism — "the Cut Score method under 5 CFR §
332.402", "Mechanism C - Set number of highest-ranked eligibles". Same
regulation, same referral. Searching only the umbrella term misses 62
announcements, 34 of them at Veterans Health, which by that reading never uses
the method and is in fact the third heaviest user. Both families are in
`patterns.yaml`.

That second group is almost all direct-hire-authority announcements listing what
they're exempt from:

> Under the provisions of the Direct Hire Authority, Veterans Preference and the
> "Rule of Many" do not apply.

A raw phrase match counts those as adopters, and they're 39% of the matches.
Every one of SSA's 79 mentions is a negation, as are all 60 at Federal Highway,
all 38 at the National Gallery of Art, and all 28 at Federal Transit. None of
those four agencies uses the method at all.

Among the 405 that do, it climbed hard through the spring:

| month | uses it | says it doesn't apply | announcements | % using it |
|-------|----|----|--------|------|
| Jan | 1 | 3 | 16,173 | 0.006% |
| Feb | 7 | 6 | 18,837 | 0.037% |
| Mar | 29 | 27 | 23,124 | 0.125% |
| Apr | 69 | 62 | 23,845 | 0.289% |
| May | 127 | 39 | 21,901 | 0.580% |
| Jun | 86 | 20 | 21,965 | 0.392% |
| Jul | 100 | 75 | 22,597 | 0.443% |
| Aug | 48 | 22 | 13,588 | 0.353% |

August runs through the 19th. "Rule of three" held flat at 136–258 a month the
whole time.

NLRB accounts for 138 of the 467, then FCC (43), Veterans Health (34), NARA (21),
Western Area Power (19), DFC (18), EEOC (17). The earliest is a DOJ paralegal announcement from January
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

Together those cost a lot. The API text finds 556 announcements. The scraped
pages find 721 — a 23% undercount, and it hides whole agencies. All 60 of Federal
Highway's announcements naming the rule are in the API, with the phrase absent
from the text of every one. Read the API and the agency looks like it has never
encountered it.

Neither gap can be backfilled from the API afterward. The Historical API returns
metadata only, and Search returns open jobs only. usajobs.gov serves closed
announcements indefinitely, so the page is the source.

## The dataset

[`abigailhaddad/usajobs-scraping`](https://huggingface.co/datasets/abigailhaddad/usajobs-scraping) —
every 2026 announcement, page text plus 39 structured fields from the Historical
API, one parquet per month, updated daily. 162,030 rows, 241 MB.

```bash
python3 dataset.py               # posting list and metadata, from the mirror
python3 scrape.py                # ~2 hours for a year at 1,350 pages/min
python3 publish.py --dry-run     # build/, uploading nothing
python3 publish.py               # create the repo and push, one commit
```

`scrape.py` is resumable and crash-safe. Shards are immutable part files tagged
with a per-run id, and a rerun skips every control number already stored, so you
can kill it whenever.

Daily updates run as a step inside the
[usajobs_historical](https://github.com/abigailhaddad/usajobs_historical)
pipeline, which clones this repo and runs `update_daily.py` after its own
collection. That step reads `data/historical_jobs_2026.parquet` off disk — the
copy that run just refreshed — so nothing here needs R2 credentials. About 800
new postings a day, roughly 40 seconds. On the first of the month it passes
`--refresh-all` to re-join metadata across every month, since close dates and
opening status keep changing after a posting appears.

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

- `dataset.py` — the posting list, the structured fields, and compaction into
  monthly parquets. Prefers the pipeline's local mirror copy, falls back to the
  public URL.
- `scrape.py` — the scrape, and the fetch `update_daily.py` reuses
- `publish.py` — build `build/` and push it in one commit
- `update_daily.py` — the nightly top-up (named for the pipeline that calls it)
- `rule_of_many.ipynb` — the analysis, with charts
- `rule_of_many.py`, `patterns.yaml` — the query and the classification rules
