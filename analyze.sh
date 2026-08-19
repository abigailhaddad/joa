#!/bin/bash
# Pull every "rule of many" mention out of the published dataset, server-side.
# Nothing is downloaded except the hits.
#
# Set DATASET to point somewhere else -- build/data/*.parquet reads a local
# build before it's been pushed.
cd "$(dirname "$0")"
DATASET="${DATASET:-hf://datasets/abigailhaddad/usajobs-scraping/data/*.parquet}"
RX='(?i)rule[ -]of[ -](the[ -])?many'
mkdir -p results
echo "source: $DATASET"

duckdb -c "
LOAD httpfs;
SET http_retries=5;

CREATE TABLE hits AS
SELECT usajobsControlNumber, positionOpenDate, positionCloseDate,
       hiringAgencyName, hiringDepartmentName, occupationalSeries,
       announcementNumber, whoMayApply, text
FROM read_parquet('$DATASET')
WHERE regexp_matches(text, '$RX');

-- One row per occurrence, with enough either side to tell an applied mention
-- from a negated one. classify.py reads this.
COPY (
  SELECT usajobsControlNumber,
         unnest(regexp_extract_all(text, '.{0,300}$RX.{0,300}')) AS ctx
  FROM hits
) TO 'results/contexts.csv' (HEADER);

COPY (
  SELECT usajobsControlNumber, positionOpenDate, positionCloseDate,
         hiringAgencyName, hiringDepartmentName, occupationalSeries,
         announcementNumber,
         'https://www.usajobs.gov/job/' || usajobsControlNumber AS link
  FROM hits ORDER BY positionOpenDate, hiringAgencyName
) TO 'results/hits.csv' (HEADER);

-- Monthly denominator: every posting, not only the ones that match.
COPY (
  SELECT replace(substr(positionOpenDate,1,7),'-','_') AS month,
         count(*) AS postings,
         count(*) FILTER (regexp_matches(text, '$RX'))                       AS rule_of_many,
         count(*) FILTER (regexp_matches(text, '(?i)rule[ -]of[ -](the[ -])?three')) AS rule_of_three
  FROM read_parquet('$DATASET')
  GROUP BY 1 ORDER BY 1
) TO 'results/monthly_counts.csv' (HEADER);
"
echo "postings mentioning the phrase: $(($(wc -l < results/hits.csv) - 1))"
echo "occurrences: $(($(wc -l < results/contexts.csv) - 1))"
echo "now run: python3 classify.py"
