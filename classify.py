#!/usr/bin/env python3
"""
Sort "rule of many" mentions into announcements that use the method and
announcements that name it to say it doesn't apply.

The distinction matters: 39% of the postings that contain the phrase are
direct-hire-authority announcements listing what they're exempt from
("veterans preference and the 'Rule of Many' do not apply"). A raw phrase
match counts those as adopters.

Reads results/contexts.csv and results/hits.csv from analyze.sh, plus the rules
in patterns.yaml. Writes results/rule_of_many.csv and a summary.

A posting is `applied` if any occurrence reads as applied, `negated` if every
occurrence reads as negated, and `unclear` if nothing matched -- read those by
hand and add a rule or an override.
"""

import csv
import re
import sys
from collections import Counter
from pathlib import Path

import yaml

PHRASE = r"rule[ -]of[ -](?:the[ -])?many"
RULES = yaml.safe_load(Path("patterns.yaml").read_text())
OVERRIDES = {str(k): v for k, v in (RULES.get("overrides") or {}).items()}


def compile_group(name: str) -> list[re.Pattern]:
    return [re.compile(p.replace("many", PHRASE)) for p in RULES.get(name, [])]


NEGATED = compile_group("negated")
APPLIED = compile_group("applied")


def read_occurrence(ctx: str) -> str:
    t = " ".join(ctx.lower().split())
    if any(p.search(t) for p in NEGATED):
        return "negated"
    if any(p.search(t) for p in APPLIED):
        return "applied"
    return "unclear"


def main() -> None:
    contexts = Path("results/contexts.csv")
    hits = Path("results/hits.csv")
    if not contexts.exists():
        sys.exit("results/contexts.csv is missing -- run ./analyze.sh first.")

    per_posting: dict[str, list[str]] = {}
    with contexts.open() as f:
        for row in csv.DictReader(f):
            per_posting.setdefault(row["usajobsControlNumber"], []).append(
                read_occurrence(row["ctx"]))

    status = {}
    for cn, reads in per_posting.items():
        if cn in OVERRIDES:
            status[cn] = OVERRIDES[cn]
        elif "applied" in reads:
            status[cn] = "applied"
        elif "negated" in reads:
            status[cn] = "negated"
        else:
            status[cn] = "unclear"

    with hits.open() as f:
        rows = list(csv.DictReader(f))
    out = Path("results/rule_of_many.csv")
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["status", "occurrences"] + list(rows[0]))
        w.writeheader()
        for r in rows:
            cn = r["usajobsControlNumber"]
            w.writerow({"status": status.get(cn, "unclear"),
                        "occurrences": len(per_posting.get(cn, [])), **r})

    counts = Counter(status.values())
    total = sum(counts.values())
    print(f"{total:,} postings mention the phrase")
    for k in ("applied", "negated", "unclear"):
        print(f"  {k:8} {counts[k]:6,}  {counts[k]/total*100:4.1f}%")
    if counts["unclear"]:
        print(f"\nread the unclear ones and add a rule or an override:")
        print(f"  duckdb -c \"SELECT c.* FROM read_csv('results/contexts.csv', header=true) c "
              f"JOIN read_csv('{out}', header=true) r USING (usajobsControlNumber) "
              f"WHERE r.status='unclear' LIMIT 20\"")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
