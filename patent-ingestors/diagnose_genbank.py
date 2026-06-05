#!/usr/bin/env python3
"""Diagnostic: Fetch 3 GenBank patent sequences and dump XML structure."""
import sys
sys.path.insert(0, "/opt/nobleblocks/paper-db/patent-ingestors")
exec(open("/opt/nobleblocks/paper-db/patent-ingestors/ssl_wrapper.py").read())

import requests
import xml.etree.ElementTree as ET

NCBI_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
EMAIL = "admin@nobleblocks.com"

# Step 1: Get 5 IDs
print("Fetching 5 patent sequence IDs...")
params = {
    "db": "nuccore",
    "term": '"patent"[Properties]',
    "retmax": 5,
    "retmode": "json",
    "email": EMAIL,
}
resp = requests.get(f"{NCBI_BASE}/esearch.fcgi", params=params, timeout=15)
data = resp.json()
ids = data["esearchresult"]["idlist"]
print(f"IDs: {ids}")

import time
time.sleep(0.5)

# Step 2: Fetch full records
print("\nFetching full GenBank XML...")
params2 = {
    "db": "nuccore",
    "id": ",".join(ids[:3]),
    "rettype": "gb",
    "retmode": "xml",
    "email": EMAIL,
}
resp2 = requests.get(f"{NCBI_BASE}/efetch.fcgi", params=params2, timeout=30)
xml_text = resp2.text

# Step 3: Parse and show structure
print(f"\nXML length: {len(xml_text)} chars")
print(f"First 500 chars: {xml_text[:500]}")
print("\n--- Parsing sequences ---")

root = ET.fromstring(xml_text)
for i, seq in enumerate(root.findall(".//GBSeq")):
    print(f"\n{'='*60}")
    print(f"SEQUENCE {i+1}")
    print(f"{'='*60}")
    
    acc = seq.find("GBSeq_primary-accession")
    print(f"  Accession: {acc.text if acc is not None else 'NONE'}")
    
    moltype = seq.find("GBSeq_moltype")
    print(f"  MolType: {moltype.text if moltype is not None else 'NONE'}")
    
    organism = seq.find("GBSeq_organism")
    print(f"  Organism: {organism.text if organism is not None else 'NONE'}")
    
    definition = seq.find("GBSeq_definition")
    print(f"  Definition: {definition.text[:100] if definition is not None and definition.text else 'NONE'}")
    
    length = seq.find("GBSeq_length")
    print(f"  Length: {length.text if length is not None else 'NONE'}")
    
    keywords = seq.find("GBSeq_keywords")
    if keywords is not None:
        kws = [kw.text for kw in keywords.findall("GBKeyword")]
        print(f"  Keywords: {kws[:5]}")
    
    comment = seq.find("GBSeq_comment")
    if comment is not None:
        print(f"  Comment: {comment.text[:200] if comment.text else 'NONE'}")
    
    # References
    print(f"  References:")
    for ref in seq.findall(".//GBReference"):
        journal = ref.find("GBReference_journal")
        title = ref.find("GBReference_title")
        authors = ref.find("GBReference_authors")
        print(f"    Journal: {journal.text[:200] if journal is not None and journal.text else 'NONE'}")
        print(f"    Title: {title.text[:100] if title is not None and title.text else 'NONE'}")
        author_names = [a.find("GBAuthor").text for a in (authors.findall("GBAuthor") if authors is not None else []) ] if authors is not None else []
        print(f"    Authors: {author_names[:3]}")
        print()

    # Features summary
    features = seq.findall(".//GBFeature")
    feature_keys = [f.find("GBFeature_key").text for f in features if f.find("GBFeature_key") is not None]
    print(f"  Features: {feature_keys[:10]}")

print("\n\nDone.")
