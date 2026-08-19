#!/bin/bash
# Every 2026 posting the Historical API knows about, which is the universe the
# scrape covers. The historical mirror is metadata only -- no announcement text --
# so it can't be searched, but it's the most complete list of what exists.
cd "$(dirname "$0")"
mkdir -p reference
HIST=$(python3 -c "print('[' + ', '.join(chr(39)+u.strip()+chr(39) for u in open('reference/r2_historical_urls.txt')) + ']')")
duckdb -c "
LOAD httpfs; SET http_retries=5; SET memory_limit='6GB';
COPY (
  SELECT usajobsControlNumber::varchar AS cn,
         substr(positionOpenDate, 1, 10) AS od,
         coalesce(hiringAgencyName, '') AS agency,
         coalesce(hiringDepartmentName, '') AS department,
         positionTitle AS title
  FROM read_parquet($HIST, union_by_name=true)
  WHERE substr(positionOpenDate, 1, 4) = '2026'
  ORDER BY od, cn
) TO 'reference/controls_2026.csv' (HEADER);
"
echo "2026 postings: $(($(wc -l < reference/controls_2026.csv) - 1))"
