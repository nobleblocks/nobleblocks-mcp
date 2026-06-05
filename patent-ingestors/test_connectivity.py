#!/usr/bin/env python3
"""Test DNS resolution and API connectivity from this server."""
import socket
import sys
sys.path.insert(0, "/opt/nobleblocks/paper-db/patent-ingestors")
exec(open("/opt/nobleblocks/paper-db/patent-ingestors/ssl_wrapper.py").read())
import requests

print("=== DNS Resolution Test ===")
domains = [
    "api.semanticscholar.org",
    "api.lens.org", 
    "api.openalex.org",
    "search.patentsview.org",
    "api.patentsview.org",
    "bulkdata.uspto.gov",
    "eutils.ncbi.nlm.nih.gov",
]
for d in domains:
    try:
        ip = socket.getaddrinfo(d, 443)[0][4][0]
        print(f"  OK   {d} -> {ip}")
    except Exception as e:
        print(f"  FAIL {d}: {e}")

print("\n=== API Connectivity Test ===")
tests = [
    ("Semantic Scholar", "https://api.semanticscholar.org/graph/v1/paper/search?query=CRISPR&fields=title,citationCount&limit=2"),
    ("OpenAlex works", "https://api.openalex.org/works?filter=cited_by_count:>100&per_page=1&select=id,doi,title"),
    ("NCBI GenBank", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=nucleotide&term=patent[Properties]&retmax=1&retmode=json"),
]
for name, url in tests:
    try:
        r = requests.get(url, timeout=15)
        print(f"  {name}: {r.status_code} - {r.text[:150]}")
    except Exception as e:
        print(f"  {name}: ERROR - {e}")

print("\nDone.")
