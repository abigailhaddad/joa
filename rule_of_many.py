#!/usr/bin/env python3
"""
Find "rule of many" in USAJOBS announcements and tell the two kinds of
mention apart.

In 2026 through mid-August, 659 announcements printed the phrase, but only 405
used the method. The other 254
name it to say it doesn't apply -- almost all direct-hire announcements listing
their exemptions ("Veterans Preference and the 'Rule of Many' do not apply").
A raw phrase match counts those as adopters.

Each file of the published dataset is downloaded, checked, scanned and deleted
straight away; only the per-file results are kept, in cache/scan/, so a rerun
reads just the files that changed. The rules for reading a mention live in
patterns.yaml; edit them and reclassify without re-scanning.

    python3 rule_of_many.py              # query, classify, write results/
    python3 rule_of_many.py --cached     # reclassify what's already in results/

Or import it, which is what rule_of_many.ipynb does:

    import rule_of_many as rom
    hits, contexts = rom.fetch()
    status = rom.classify(contexts)
"""

import argparse
import hashlib
import re
import tempfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
import pandas as pd
import requests
import yaml
from huggingface_hub import HfApi, hf_hub_url
from huggingface_hub.utils import build_hf_headers

REPO = "abigailhaddad/usajobs-scraping"
PHRASE = r"rule[ -]*of[ -]*(?:the[ -]*)?many"       # the umbrella term

# The scraper collapses whitespace, but these survive it and break a literal
# match: soft hyphens and zero-width spaces inside words, non-breaking spaces,
# the dash family, and entities left unescaped in the text. All are matched
# case-insensitively and normalized before any pattern runs.
INVISIBLE = r"[\x{00ad}\x{200b}-\x{200d}\x{2060}\x{feff}]|&shy;|&#173;|&#8203;"
SPACES = r"\x{00a0}|&nbsp;|&#160;|&#xa0;"
DASHES = r"[\x{2010}-\x{2015}]|&#(820[89]|821[0-3]);|&#x201[0-5];|&[mn]dash;"
RESULTS = Path("results")

HITS = RESULTS / "hits.csv"
CONTEXTS = RESULTS / "contexts.csv"
MONTHLY = RESULTS / "monthly_counts.csv"
CLASSIFIED = RESULTS / "rule_of_many.csv"
REVISION = RESULTS / "revision.txt"
CACHE = Path("cache/scan")       # per-file results; never announcement files
WORKERS = 3
COLS = ("usajobsControlNumber, positionOpenDate, positionCloseDate, "
        "hiringAgencyName, hiringDepartmentName, occupationalSeries, announcementNumber")


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")     # a widget in notebooks otherwise
    return con


def _clean(col: str = "text") -> str:
    """SQL for `col` with INVISIBLE dropped, SPACES and DASHES made plain.

    Most rows have none of it, so one match picks out the ones that do and
    only those pay for the four rewrites. Output is the same either way.
    """
    e = f"regexp_replace({col}, '(?i){INVISIBLE}', '', 'g')"
    e = f"regexp_replace({e}, '(?i){SPACES}', ' ', 'g')"
    e = f"regexp_replace({e}, '(?i){DASHES}', '-', 'g')"
    e = f"regexp_replace({e}, '\\s+', ' ', 'g')"
    dirty = f"(?i){INVISIBLE}|{SPACES}|{DASHES}|\\s\\s|[\\t\\n\\r\\f]"
    return f"CASE WHEN regexp_matches({col}, '{dirty}') THEN {e} ELSE {col} END"


def _files(revision: str | None) -> tuple[str, list]:
    """The dataset's commit and its parquet files, pinned so a commit landing
    mid-run (the daily update, a re-fetch pass) can't mix two versions."""
    api = HfApi()
    sha = api.dataset_info(REPO, revision=revision).sha
    files = [f for f in api.list_repo_tree(REPO, repo_type="dataset", path_in_repo="data",
                                           revision=sha) if f.path.endswith(".parquet")]
    if not files:
        raise RuntimeError(f"no parquet files in {REPO}@{sha}")
    return sha, sorted(files, key=lambda f: f.path)


def _cache_key(f) -> Path:
    """Results for one file hold until its content or the search changes."""
    h = hashlib.sha256("\n".join([f.blob_id, search_regex(), PHRASE, _clean()]).encode())
    return CACHE / f"{Path(f.path).stem}_{h.hexdigest()[:16]}.parquet"


def _download(sha: str, f, dest: Path, tries: int = 5) -> None:
    """Fetch one file and check it against the sha256 HF lists for it.

    Streaming files straight into DuckDB failed twice on full-history runs
    with ZSTD decompression errors on files that read fine on retry; checking
    the hash catches a bad download before it's read.
    """
    url = hf_hub_url(REPO, f.path, repo_type="dataset", revision=sha)
    for attempt in range(tries):
        try:
            digest = hashlib.sha256()
            with requests.get(url, stream=True, timeout=60, headers=build_hf_headers()) as r:
                r.raise_for_status()
                with open(dest, "wb") as out:
                    for chunk in r.iter_content(1 << 20):
                        out.write(chunk)
                        digest.update(chunk)
            if f.lfs and digest.hexdigest() != f.lfs.sha256:
                raise OSError(f"sha256 mismatch")
            return
        except (requests.RequestException, OSError) as e:
            if attempt == tries - 1:
                raise RuntimeError(f"{f.path} failed {tries} times: {e}") from e


def _scan_file(sha: str, f) -> pd.DataFrame:
    """One row per posting in the file: which patterns it matches, and its
    cleaned text if it names the method. The download is deleted as soon as
    it's read; only these results are kept, in CACHE."""
    key = _cache_key(f)
    if key.exists():
        return pd.read_parquet(key)
    rx = search_regex()
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / Path(f.path).name
        try:
            _download(sha, f, local)
            df = _connect().execute(f"""
                SELECT {COLS}, rule_of_many, any_name, rule_of_three,
                       CASE WHEN any_name THEN text END AS text
                FROM (SELECT *,
                             regexp_matches(text, '(?i){PHRASE}') AS rule_of_many,
                             regexp_matches(text, '(?i){rx}') AS any_name,
                             regexp_matches(text, '(?i)rule[ -]*of[ -]*(the[ -]*)?three')
                                 AS rule_of_three
                      FROM (SELECT {COLS}, {_clean()} AS text
                            FROM read_parquet('{local}')))
            """).df()
        finally:
            local.unlink(missing_ok=True)
    CACHE.mkdir(parents=True, exist_ok=True)
    df.to_parquet(key, index=False)
    return df


def scan(revision: str | None = None, workers: int = WORKERS) -> pd.DataFrame:
    """Every posting in the dataset, one row each, with its match flags.

    Files are scanned in parallel and each one's results cached, so a rerun
    only reads files that changed. The commit scanned goes in REVISION.
    """
    sha, files = _files(revision)
    print(f"{REPO}@{sha[:12]}: {len(files)} files", flush=True)
    with ThreadPoolExecutor(workers) as pool:
        parts = list(pool.map(lambda f: _scan_file(sha, f), files))
    RESULTS.mkdir(exist_ok=True)
    REVISION.write_text(sha + "\n")
    return pd.concat(parts, ignore_index=True)


def _rules() -> dict:
    return yaml.safe_load(Path("patterns.yaml").read_text())


def _compile(group: str) -> list[re.Pattern]:
    return [re.compile(p.replace("many", PHRASE)) for p in _rules().get(group, [])]


def search_families() -> dict[str, str]:
    """{label: regex} for each way announcements name this method."""
    return _rules()["search"]


def search_regex() -> str:
    """One regex matching any of them, for the server-side query."""
    return "|".join(f"({r})" for r in search_families().values())


def fetch(revision: str | None = None, save: bool = True):
    """Every posting that names the method. Returns (hits, contexts)."""
    rx = search_regex()
    con = _connect()
    found = scan(revision).query("any_name")
    con.register("found", found)
    hits = con.execute(f"""
        SELECT {COLS},
               'https://www.usajobs.gov/job/' || usajobsControlNumber AS link
        FROM found ORDER BY positionOpenDate, hiringAgencyName
    """).df()
    contexts = con.execute(f"""
        SELECT usajobsControlNumber,
               unnest(regexp_extract_all(text, '.{{0,300}}(?i)({rx}).{{0,300}}')) AS ctx
        FROM found
    """).df()
    if save:
        RESULTS.mkdir(exist_ok=True)
        hits.to_csv(HITS, index=False)
        contexts.to_csv(CONTEXTS, index=False)
    return hits, contexts


def monthly(revision: str | None = None, save: bool = True) -> pd.DataFrame:
    """Per month: every posting, plus mentions of each rule. The denominator."""
    df = (scan(revision)
          .assign(month=lambda d: d.positionOpenDate.str[:7].str.replace("-", "_"))
          .groupby("month", as_index=False)
          .agg(postings=("usajobsControlNumber", "size"),
               rule_of_many=("rule_of_many", "sum"),
               any_name=("any_name", "sum"),
               rule_of_three=("rule_of_three", "sum")))
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
    ap.add_argument("--revision", help="dataset commit to scan (default: latest)")
    ap.add_argument("--cached", action="store_true",
                    help="reclassify results/ without re-querying")
    args = ap.parse_args()

    if args.cached:
        hits, contexts = load_cached()
    else:
        hits, contexts = fetch(args.revision)
        monthly(REVISION.read_text().strip())

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
