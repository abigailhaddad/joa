# Running the daily update inside usajobs_historical

The scrape has no R2 access of its own. It reads
`data/historical_jobs_2026.parquet` off disk, which the pipeline has already
downloaded and refreshed by the time this runs — so no credentials here, no
second fetch, and the posting list is that day's.

Add these steps to `.github/workflows/daily-data-update.yml`, after **Run data
integrity tests** and before **Commit and push to data-updates branch**. The
scrape doesn't touch the parquet files, so it stays out of the commit and PR.

```yaml
    - name: Check usajobs.gov is reachable from this runner
      run: |
        pip install beautifulsoup4 requests
        python3 - <<'EOF'
        import sys, re, requests
        from bs4 import BeautifulSoup
        UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
        r = requests.get("https://www.usajobs.gov/job/855359900",
                         headers={"User-Agent": UA}, timeout=45)
        soup = BeautifulSoup(r.text, "html.parser")
        for t in soup(["script", "style", "noscript"]):
            t.decompose()
        text = re.sub(r"\s+", " ", soup.get_text(" ")).strip()
        print(f"status {r.status_code}, {len(text):,} chars")
        if not r.ok or len(text) < 5000:
            sys.exit("usajobs.gov is not serving this runner. Move the scrape off "
                     "GitHub Actions, or route it through a proxy.")
        EOF

    - name: Scrape new announcements and push to HuggingFace
      env:
        HF_TOKEN: ${{ secrets.HF_TOKEN }}
      run: |
        git clone --depth 1 https://github.com/abigailhaddad/joa.git /tmp/joa
        pip install -r /tmp/joa/requirements.txt
        cd /tmp/joa
        ln -s "$GITHUB_WORKSPACE/data" data
        REFRESH=""
        [ "$(date -u +%d)" = "01" ] && REFRESH="--refresh-all"
        python3 update_daily.py $REFRESH
```

usajobs.gov sits behind Akamai and 403s anything that doesn't look like a
browser. Whether it also objects to GitHub's runner IPs is the one thing that
can't be tested from a laptop, so the check runs first and fails in seconds
rather than halfway through a scrape.

The monthly `--refresh-all` re-joins metadata across every month. Close dates
and opening status keep changing after a posting first appears, and the daily
run only refreshes the months it touched.

## What it needs

- `HF_TOKEN` as a repository secret, with write scope
- `HF_DATASET_REPO` as a repository variable if the dataset ever moves;
  otherwise it defaults to `abigailhaddad/usajobs-scraping`

## If the runner is blocked

Nothing about the scrape changes, only where it runs. `update_daily.py` works
anywhere it can reach both usajobs.gov and the historical parquet — a launchd
job on your laptop pointed at the public R2 URL does the same work:

```bash
python3 update_daily.py   # falls back to the public URL when data/ isn't there
```
