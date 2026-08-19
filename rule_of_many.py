#!/usr/bin/env python3
"""
Find "rule of many" in 2026 USAJOBS announcements and tell the two kinds of
mention apart.

659 announcements print the phrase, but only 405 use the method. The other 254
name it to say it doesn't apply -- almost all direct-hire announcements listing
their exemptions ("Veterans Preference and the 'Rule of Many' do not apply").
A raw phrase match counts those as adopters.

The query runs server-side against the published dataset, so only matches come
back. The rules for reading a mention live in patterns.yaml; edit them and
reclassify without re-querying.

    python3 rule_of_many.py              # query, classify, write results/
    python3 rule_of_many.py --cached     # reclassify what's already in results/

Or import it, which is what rule_of_many.ipynb does:

    import rule_of_many as rom
    hits, contexts = rom.fetch()
    status = rom.classify(contexts)
"""

import argparse
import re
from collections import Counter
from pathlib import Path

import duckdb
import pandas as pd
import yaml

DATASET = "hf://datasets/abigailhaddad/usajobs-scraping/data/*.parquet"
PHRASE = r"rule[ -]of[ -](?:the[ -])?many"
RESULTS = Path("results")

HITS = RESULTS / "hits.csv"
CONTEXTS = RESULTS / "contexts.csv"
MONTHLY = RESULTS / "monthly_counts.csv"
CLASSIFIED = RESULTS / "rule_of_many.csv"


def _rules() -> dict:
    return yaml.safe_load(Path("patterns.yaml").read_text())


def _compile(group: str) -> list[re.Pattern]:
    return [re.compile(p.replace("many", PHRASE)) for p in _rules().get(group, [])]


def fetch(dataset: str = DATASET, save: bool = True):
    """Query the dataset for matches. Returns (hits, contexts) as DataFrames."""
    con = duckdb.connect()
    con.execute("LOAD httpfs; SET http_retries=5;")
    con.execute(f"""
        CREATE TABLE hits AS
        SELECT usajobsControlNumber, positionOpenDate, positionCloseDate,
               hiringAgencyName, hiringDepartmentName, occupationalSeries,
               announcementNumber, text
        FROM read_parquet('{dataset}')
        WHERE regexp_matches(text, '(?i){PHRASE}')
    """)
    hits = con.execute("""
        SELECT usajobsControlNumber, positionOpenDate, positionCloseDate,
               hiringAgencyName, hiringDepartmentName, occupationalSeries,
               announcementNumber,
               'https://www.usajobs.gov/job/' || usajobsControlNumber AS link
        FROM hits ORDER BY positionOpenDate, hiringAgencyName
    """).df()
    contexts = con.execute(f"""
        SELECT usajobsControlNumber,
               unnest(regexp_extract_all(text, '.{{0,300}}(?i){PHRASE}.{{0,300}}')) AS ctx
        FROM hits
    """).df()
    if save:
        RESULTS.mkdir(exist_ok=True)
        hits.to_csv(HITS, index=False)
        contexts.to_csv(CONTEXTS, index=False)
    return hits, contexts


def monthly(dataset: str = DATASET, save: bool = True) -> pd.DataFrame:
    """Per month: every posting, plus mentions of each rule. The denominator."""
    con = duckdb.connect()
    con.execute("LOAD httpfs; SET http_retries=5;")
    df = con.execute(f"""
        SELECT replace(substr(positionOpenDate,1,7),'-','_') AS month,
               count(*) AS postings,
               count(*) FILTER (regexp_matches(text, '(?i){PHRASE}')) AS rule_of_many,
               count(*) FILTER (regexp_matches(text, '(?i)rule[ -]of[ -](the[ -])?three'))
                   AS rule_of_three
        FROM read_parquet('{dataset}')
        GROUP BY 1 ORDER BY 1
    """).df()
    if save:
        RESULTS.mkdir(exist_ok=True)
        df.to_csv(MONTHLY, index=False)
    return df


def read_occurrence(ctx: str) -> str:
    """One mention: applied, negated, or unclear. Negation wins."""
    t = " ".join(str(ctx).lower().split())
    if any(p.search(t) for p in _compile("negated")):
        return "negated"
    if any(p.search(t) for p in _compile("applied")):
        return "applied"
    return "unclear"


def classify(contexts: pd.DataFrame) -> pd.Series:
    """Per posting: applied if any mention applies, negated if all negate."""
    overrides = {str(k): v for k, v in (_rules().get("overrides") or {}).items()}
    reads = contexts.assign(read=contexts.ctx.map(read_occurrence))
    grouped = reads.groupby("usajobsControlNumber").read.agg(set)

    def decide(cn, s):
        if str(cn) in overrides:
            return overrides[str(cn)]
        return "applied" if "applied" in s else ("negated" if "negated" in s else "unclear")

    return pd.Series({cn: decide(cn, s) for cn, s in grouped.items()}, name="status")


def load_cached():
    return pd.read_csv(HITS), pd.read_csv(CONTEXTS)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default=DATASET)
    ap.add_argument("--cached", action="store_true",
                    help="reclassify results/ without re-querying")
    args = ap.parse_args()

    if args.cached:
        hits, contexts = load_cached()
    else:
        print(f"source: {args.dataset}", flush=True)
        hits, contexts = fetch(args.dataset)
        monthly(args.dataset)

    status = classify(contexts)
    out = hits.assign(
        status=hits.usajobsControlNumber.map(status).fillna("unclear"),
        occurrences=hits.usajobsControlNumber.map(contexts.usajobsControlNumber.value_counts()),
    )
    out = out[["status", "occurrences"] + [c for c in hits.columns]]
    out.to_csv(CLASSIFIED, index=False)

    counts = Counter(out.status)
    total = len(out)
    print(f"{total:,} postings mention the phrase, {len(contexts):,} occurrences")
    for k in ("applied", "negated", "unclear"):
        print(f"  {k:8} {counts[k]:6,}  {counts[k]/total*100:4.1f}%")
    print(f"\nwrote {CLASSIFIED}")


if __name__ == "__main__":
    main()
