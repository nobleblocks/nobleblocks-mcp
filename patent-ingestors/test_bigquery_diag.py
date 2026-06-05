#!/usr/bin/env python3
"""Quick diagnostic: check BigQuery result volume and sample data."""
import sys
import time
sys.path.insert(0, '.')

from google.cloud import bigquery

client = bigquery.Client(project='gen-lang-client-0004533848')

# Count query
print("Running count query...")
q = """
SELECT COUNT(*) as total
FROM `patents-public-data.patents.publications` pub,
     UNNEST(pub.citation) AS cit
WHERE pub.country_code = 'US'
  AND cit.npl_text IS NOT NULL
  AND LENGTH(cit.npl_text) > 20
  AND REGEXP_CONTAINS(cit.npl_text, r'10\\.\\d{4,9}/')
"""
start = time.time()
result = client.query(q).result()
for row in result:
    print(f"Total US NPL citations with DOIs: {row.total:,}")
print(f"Took {time.time()-start:.1f}s")

# Sample 5 rows
print("\nSample rows:")
q2 = """
SELECT
    pub.publication_number AS patent_id,
    cit.npl_text,
    cit.category AS citation_category
FROM `patents-public-data.patents.publications` pub,
     UNNEST(pub.citation) AS cit
WHERE pub.country_code = 'US'
  AND cit.npl_text IS NOT NULL
  AND LENGTH(cit.npl_text) > 20
  AND REGEXP_CONTAINS(cit.npl_text, r'10\\.\\d{4,9}/')
LIMIT 5
"""
result2 = client.query(q2).result()
for row in result2:
    print(f"  {row.patent_id}: {row.npl_text[:120]}")
    print(f"    category: {row.citation_category}")

# Test iterating through 100 rows to measure speed
print("\nSpeed test: iterate 100 rows...")
q3 = """
SELECT
    pub.publication_number AS patent_id,
    cit.npl_text
FROM `patents-public-data.patents.publications` pub,
     UNNEST(pub.citation) AS cit
WHERE pub.country_code = 'US'
  AND cit.npl_text IS NOT NULL
  AND LENGTH(cit.npl_text) > 20
  AND REGEXP_CONTAINS(cit.npl_text, r'10\\.\\d{4,9}/')
LIMIT 100
"""
start = time.time()
result3 = client.query(q3).result()
count = 0
for row in result3:
    count += 1
elapsed = time.time() - start
print(f"  Iterated {count} rows in {elapsed:.1f}s ({count/elapsed:.0f} rows/s)")
