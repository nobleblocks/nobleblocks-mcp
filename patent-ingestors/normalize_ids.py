#!/usr/bin/env python3
"""
Canonical ID Normalization for Paper & Patent Identifiers

ALL ingestors must use these functions before inserting/matching IDs.
This prevents format mismatches like:
  - DOI: "10.1038/Nature14539" vs "10.1038/nature14539"
  - Patent: "US-12224364-B2" vs "US-12224364"
  - PMID: "PMID:23903748" vs "23903748"
  - arXiv: "arXiv:2301.12345" vs "2301.12345"
  - OpenAlex: "https://openalex.org/W2935714482" vs "W2935714482"
  - S2: "CorpusId:12345" vs "12345"

Database trigger (normalize_paper_ids_trigger) enforces same rules at DB level.
"""

import re
from typing import Optional

# Patterns
_DOI_PREFIX_RE = re.compile(r'^https?://(dx\.)?doi\.org/', re.IGNORECASE)
_DOI_TRAILING_RE = re.compile(r'[.,;:)\]}>]+$')
_PMID_PREFIX_RE = re.compile(r'^(pmid[:\s]*|pubmed[:\s]*)', re.IGNORECASE)
_ARXIV_PREFIX_RE = re.compile(r'^(arxiv[:\s]*|https?://arxiv\.org/(abs|pdf)/)', re.IGNORECASE)
_OPENALEX_PREFIX_RE = re.compile(r'^https?://openalex\.org/', re.IGNORECASE)
_S2_PREFIX_RE = re.compile(r'^(corpusid[:\s]*|s2[:\s]*)', re.IGNORECASE)


def normalize_doi(doi: Optional[str]) -> Optional[str]:
    """
    Canonical DOI format: lowercase, no URL prefix, no trailing punctuation.
    
    Examples:
        "https://doi.org/10.1038/Nature14539." → "10.1038/nature14539"
        "10.1038/NATURE14539"                 → "10.1038/nature14539"
        " 10.1234/foo.bar; "                  → "10.1234/foo.bar"
    """
    if not doi:
        return None
    doi = doi.strip()
    if not doi:
        return None
    # Strip URL prefix
    doi = _DOI_PREFIX_RE.sub('', doi)
    # Strip trailing punctuation (common in citation text)
    doi = _DOI_TRAILING_RE.sub('', doi)
    # Lowercase (DOIs are case-insensitive per spec)
    doi = doi.lower()
    # Basic validity check
    if not doi.startswith('10.') or '/' not in doi or len(doi) < 8:
        return None
    return doi


def normalize_pmid(pmid: Optional[str]) -> Optional[str]:
    """
    Canonical PMID format: bare numeric string (no prefix).
    
    Examples:
        "PMID:23903748"  → "23903748"
        "PubMed 12345"   → "12345"
        "23903748"       → "23903748"
    """
    if not pmid:
        return None
    pmid = str(pmid).strip()
    if not pmid:
        return None
    # Strip prefix
    pmid = _PMID_PREFIX_RE.sub('', pmid).strip()
    # Must be numeric
    if not pmid.isdigit() or len(pmid) > 9:
        return None
    return pmid


def normalize_arxiv_id(arxiv_id: Optional[str]) -> Optional[str]:
    """
    Canonical arXiv format: bare ID without prefix, preserves version.
    
    Examples:
        "arXiv:2301.12345v2"                  → "2301.12345v2"
        "https://arxiv.org/abs/2301.12345"    → "2301.12345"
        "hep-th/9905111"                      → "hep-th/9905111" (old format)
    """
    if not arxiv_id:
        return None
    arxiv_id = str(arxiv_id).strip()
    if not arxiv_id:
        return None
    # Strip prefix
    arxiv_id = _ARXIV_PREFIX_RE.sub('', arxiv_id).strip()
    # Strip trailing .pdf if present
    if arxiv_id.endswith('.pdf'):
        arxiv_id = arxiv_id[:-4]
    # Validate: new format (YYMM.NNNNN) or old format (archive/NNNNNNN)
    if not (re.match(r'^\d{4}\.\d{4,5}(v\d+)?$', arxiv_id) or
            re.match(r'^[a-z-]+/\d{7}(v\d+)?$', arxiv_id)):
        return None
    return arxiv_id


def normalize_openalex_id(oaid: Optional[str]) -> Optional[str]:
    """
    Canonical OpenAlex format: "W" + digits (no URL prefix).
    
    Examples:
        "https://openalex.org/W2935714482" → "W2935714482"
        "W2935714482"                      → "W2935714482"
    """
    if not oaid:
        return None
    oaid = str(oaid).strip()
    if not oaid:
        return None
    # Strip URL prefix
    oaid = _OPENALEX_PREFIX_RE.sub('', oaid).strip()
    # Validate
    if not re.match(r'^W\d+$', oaid):
        return None
    return oaid


def normalize_s2_id(s2_id: Optional[str]) -> Optional[str]:
    """
    Canonical Semantic Scholar format: bare 40-char hex hash.
    
    Examples:
        "CorpusId:12345"  → "12345" (numeric corpus IDs stored as-is)
        "abc123...def"    → "abc123...def" (40-char hex)
    """
    if not s2_id:
        return None
    s2_id = str(s2_id).strip()
    if not s2_id:
        return None
    # Strip prefix
    s2_id = _S2_PREFIX_RE.sub('', s2_id).strip()
    # Accept either 40-char hex or numeric corpus ID
    if re.match(r'^[0-9a-f]{40}$', s2_id):
        return s2_id
    if s2_id.isdigit():
        return s2_id
    return None


def normalize_patent_id(patent_id: Optional[str], strip_kind_code: bool = True) -> Optional[str]:
    """
    Canonical patent_id format: JURISDICTION-NUMBER (no kind code by default).
    
    Our DB stores patents as "US-12224364" (no kind code).
    BigQuery/EPO use "US-12224364-B2" (with kind code).
    
    Examples:
        "US-12224364-B2"  → "US-12224364"
        "EP-3456789-A1"   → "EP-3456789"
        "US-12224364"     → "US-12224364" (already clean)
    """
    if not patent_id:
        return None
    patent_id = str(patent_id).strip().upper()
    if not patent_id:
        return None
    if strip_kind_code:
        parts = patent_id.split('-')
        if len(parts) >= 3:
            # Jurisdiction-Number (drop kind code)
            patent_id = f"{parts[0]}-{parts[1]}"
        elif len(parts) == 2:
            patent_id = patent_id  # Already clean
        else:
            return None
    return patent_id


# ─── Convenience: normalize a dict of IDs ───

def normalize_paper_ids(record: dict) -> dict:
    """
    Normalize all paper IDs in a record dict (in-place + return).
    
    Usage:
        record = normalize_paper_ids({
            "doi": "HTTPS://DOI.ORG/10.1038/Nature14539.",
            "pmid": "PMID:23903748",
            "arxiv_id": "arXiv:2301.12345v2",
            "openalex_id": "https://openalex.org/W2935714482",
        })
        # → {"doi": "10.1038/nature14539", "pmid": "23903748", ...}
    """
    if 'doi' in record:
        record['doi'] = normalize_doi(record['doi'])
    if 'pmid' in record:
        record['pmid'] = normalize_pmid(record['pmid'])
    if 'arxiv_id' in record:
        record['arxiv_id'] = normalize_arxiv_id(record['arxiv_id'])
    if 'openalex_id' in record:
        record['openalex_id'] = normalize_openalex_id(record['openalex_id'])
    if 's2_id' in record:
        record['s2_id'] = normalize_s2_id(record['s2_id'])
    if 'patent_id' in record:
        record['patent_id'] = normalize_patent_id(record['patent_id'])
    return record
