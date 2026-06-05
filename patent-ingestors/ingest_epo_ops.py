#!/usr/bin/env python3
"""
EPO Open Patent Services (OPS) Ingestor

Fetches European patent data including:
- Full claims text
- Legal status (granted, lapsed, opposed)
- Citation data (patent and non-patent literature)
- Patent family information (INPADOC)

Source: https://ops.epo.org/
Auth: OAuth2 (consumer key + secret from developers.epo.org)
Rate limit: 4GB/week for free registered users

This ingestor focuses on:
1. Patents that cite academic papers (NPL citations)
2. Full claims text for semantic search
3. Legal status for investment signals
"""

import requests
import psycopg2
import psycopg2.extras
import xml.etree.ElementTree as ET
import time
import json
import os
import sys
import re
import signal
import base64
from datetime import datetime, timedelta

# Config
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

EPO_CONSUMER_KEY = os.environ.get("EPO_CONSUMER_KEY", "")
EPO_CONSUMER_SECRET = os.environ.get("EPO_CONSUMER_SECRET", "")
EPO_BASE = "https://ops.epo.org/3.2/rest-services"
EPO_AUTH_URL = "https://ops.epo.org/3.2/auth/accesstoken"

BATCH_SIZE = 100
PROGRESS_FILE = "/tmp/epo_ingest_progress.json"
DOI_PATTERN = re.compile(r'10\.\d{4,9}/[^\s,;"\'>]+')

# Namespaces for EPO XML
NS = {
    "ops": "http://ops.epo.org",
    "epo": "http://www.epo.org/exchange",
    "ft": "http://www.epo.org/fulltext",
}

# Graceful shutdown
shutdown_requested = False
def handle_signal(sig, frame):
    global shutdown_requested
    shutdown_requested = True
    print("\n⚠ Shutdown requested, finishing current batch...")

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"last_date": "2024-01-01", "total_patents": 0, "total_links": 0, "offset": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


class EPOClient:
    """EPO OPS API client with OAuth2."""

    def __init__(self, consumer_key, consumer_secret):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = None
        self.token_expires = 0

    def authenticate(self):
        """Get OAuth2 access token."""
        if not self.consumer_key or not self.consumer_secret:
            raise ValueError("EPO_CONSUMER_KEY and EPO_CONSUMER_SECRET must be set")

        credentials = base64.b64encode(
            f"{self.consumer_key}:{self.consumer_secret}".encode()
        ).decode()

        resp = requests.post(
            EPO_AUTH_URL,
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data="grant_type=client_credentials",
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        self.access_token = data["access_token"]
        self.token_expires = time.time() + int(data.get("expires_in", 1200)) - 60

    def get_token(self):
        """Get valid access token, refreshing if needed."""
        if not self.access_token or time.time() > self.token_expires:
            self.authenticate()
        return self.access_token

    def search(self, query, start=1, end=100):
        """Search published patents."""
        token = self.get_token()
        url = f"{EPO_BASE}/published-data/search"
        params = {"q": query, "Range": f"{start}-{end}"}
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/xml"}

        resp = requests.get(url, params=params, headers=headers, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.text

    def get_biblio(self, patent_ref):
        """Get bibliographic data for a patent."""
        token = self.get_token()
        url = f"{EPO_BASE}/published-data/publication/epodoc/{patent_ref}/biblio"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/xml"}

        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.text

    def get_claims(self, patent_ref):
        """Get claims text for a patent."""
        token = self.get_token()
        url = f"{EPO_BASE}/published-data/publication/epodoc/{patent_ref}/claims"
        headers = {"Authorization": f"Bearer {token}", "Accept": "text/plain"}

        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code in (404, 403):
            return None
        resp.raise_for_status()
        return resp.text

    def get_citations(self, patent_ref):
        """Get citation data (forward and backward)."""
        token = self.get_token()
        url = f"{EPO_BASE}/published-data/publication/epodoc/{patent_ref}/references"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/xml"}

        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.text

    def get_legal_status(self, patent_ref):
        """Get legal status events."""
        token = self.get_token()
        url = f"{EPO_BASE}/legal/{patent_ref}"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/xml"}

        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.text


def parse_search_results(xml_text):
    """Parse EPO search results to get patent references."""
    if not xml_text:
        return [], 0

    root = ET.fromstring(xml_text)
    refs = []

    # Total results
    total = 0
    total_el = root.find(".//{http://ops.epo.org}biblio-search")
    if total_el is not None:
        total = int(total_el.get("total-result-count", 0))

    for doc in root.findall(".//{http://www.epo.org/exchange}document-id"):
        doc_type = doc.get("document-id-type")
        if doc_type == "epodoc":
            num = doc.find("{http://www.epo.org/exchange}doc-number")
            if num is not None and num.text:
                refs.append(num.text)

    return refs, total


def parse_biblio(xml_text, patent_ref):
    """Parse bibliographic data."""
    if not xml_text:
        return None

    patent = {
        "patent_id": patent_ref,
        "jurisdiction": "EP",
        "source": "epo",
    }

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None

    # Title
    for title in root.findall(".//{http://www.epo.org/exchange}invention-title"):
        lang = title.get("lang", "")
        if lang == "en" or not patent.get("title"):
            patent["title"] = title.text

    # Abstract
    for abstract in root.findall(".//{http://www.epo.org/exchange}abstract"):
        lang = abstract.get("lang", "")
        if lang == "en" or not patent.get("abstract"):
            text = " ".join(abstract.itertext()).strip()
            patent["abstract"] = text

    # Filing date
    for app_ref in root.findall(".//{http://www.epo.org/exchange}application-reference"):
        date_el = app_ref.find(".//{http://www.epo.org/exchange}date")
        if date_el is not None and date_el.text:
            d = date_el.text
            try:
                patent["filing_date"] = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            except (ValueError, IndexError):
                pass

    # Publication date
    for pub_ref in root.findall(".//{http://www.epo.org/exchange}publication-reference"):
        date_el = pub_ref.find(".//{http://www.epo.org/exchange}date")
        if date_el is not None and date_el.text:
            d = date_el.text
            try:
                patent["grant_date"] = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            except (ValueError, IndexError):
                pass

    # Applicants (assignees)
    applicants = []
    for app in root.findall(".//{http://www.epo.org/exchange}applicant"):
        name = app.find(".//{http://www.epo.org/exchange}name")
        if name is not None and name.text:
            applicants.append(name.text)
    patent["assignee"] = applicants[0] if applicants else None
    patent["assignee_type"] = "corporate" if applicants else None

    # Inventors
    inventors = []
    for inv in root.findall(".//{http://www.epo.org/exchange}inventor"):
        name = inv.find(".//{http://www.epo.org/exchange}name")
        if name is not None and name.text:
            inventors.append(name.text)
    patent["inventors"] = inventors

    # IPC codes
    ipc_codes = []
    for ipc in root.findall(".//{http://www.epo.org/exchange}classification-ipcr"):
        text = ipc.find("{http://www.epo.org/exchange}text")
        if text is not None and text.text:
            ipc_codes.append(text.text.strip()[:10])
    patent["ipc_codes"] = ipc_codes[:20]

    return patent


def parse_citations_xml(xml_text, patent_ref):
    """Parse citation data, extract NPL (academic paper) citations."""
    if not xml_text:
        return []

    citations = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    for citation in root.findall(".//{http://www.epo.org/exchange}citation"):
        cat = citation.get("cited-phase", "")

        # Non-patent literature
        npl = citation.find("{http://www.epo.org/exchange}nplcit")
        if npl is not None:
            text_el = npl.find("{http://www.epo.org/exchange}text")
            if text_el is not None and text_el.text:
                npl_text = text_el.text
                doi_match = DOI_PATTERN.search(npl_text)
                raw_doi = doi_match.group(0).rstrip(".),").lower() if doi_match else None
                citations.append({
                    "patent_id": patent_ref,
                    "paper_doi": raw_doi,
                    "citation_context": npl_text[:500],
                    "citation_type": "npl",
                    "source": "epo",
                })

    return citations


def insert_patent(conn, patent):
    """Insert or update a single patent."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO patents (patent_id, title, abstract, claims_text, filing_date,
                            grant_date, assignee, assignee_type, inventors, ipc_codes,
                            jurisdiction, legal_status, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (patent_id) DO UPDATE SET
            claims_text = COALESCE(EXCLUDED.claims_text, patents.claims_text),
            legal_status = COALESCE(EXCLUDED.legal_status, patents.legal_status),
            updated_at = NOW()
    """, (
        patent.get("patent_id"), patent.get("title"), patent.get("abstract"),
        patent.get("claims_text"), patent.get("filing_date"), patent.get("grant_date"),
        patent.get("assignee"), patent.get("assignee_type"),
        patent.get("inventors", []), patent.get("ipc_codes", []),
        patent.get("jurisdiction"), patent.get("legal_status"), patent.get("source")
    ))
    conn.commit()
    cur.close()


def insert_citations(conn, citations):
    """Insert citation links."""
    if not citations:
        return 0

    doi_citations = [c for c in citations if c.get("paper_doi")]
    if not doi_citations:
        return 0

    sql = """
        INSERT INTO patent_paper_citations (patent_id, paper_doi, citation_context, citation_type, source)
        VALUES %s
        ON CONFLICT (patent_id, COALESCE(paper_doi, ''), COALESCE(paper_openalex_id, ''))
        DO NOTHING
    """
    values = [(c["patent_id"], c["paper_doi"], c.get("citation_context"),
               c["citation_type"], c["source"]) for c in doi_citations]
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, sql, values, template="(%s, %s, %s, %s, %s)")
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    return inserted


def main():
    print("=" * 60)
    print("  EPO Open Patent Services Ingestor")
    print("=" * 60)
    print(f"  DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  EPO credentials: {'SET' if EPO_CONSUMER_KEY else '⚠ NOT SET'}")
    print()

    if not EPO_CONSUMER_KEY or not EPO_CONSUMER_SECRET:
        print("  ❌ EPO_CONSUMER_KEY and EPO_CONSUMER_SECRET required!")
        print("  Register at: https://developers.epo.org/")
        print("  Then set environment variables and re-run.")
        sys.exit(1)

    conn = get_db_connection()
    client = EPOClient(EPO_CONSUMER_KEY, EPO_CONSUMER_SECRET)
    progress = load_progress()

    start_date = progress.get("last_date", "2024-01-01")
    total_patents = progress.get("total_patents", 0)
    total_links = progress.get("total_links", 0)

    print(f"  Starting from date: {start_date}")
    print()

    start_time = time.time()

    # Search by publication date ranges (weekly chunks)
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.now()

    while current_date < end_date and not shutdown_requested:
        week_end = current_date + timedelta(days=7)
        date_range = f"{current_date.strftime('%Y%m%d')}-{week_end.strftime('%Y%m%d')}"
        query = f'pd="{date_range}"'

        print(f"  Week {current_date.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}:")

        offset = progress.get("offset", 0) if current_date.strftime("%Y-%m-%d") == start_date else 0
        page_start = offset + 1

        while not shutdown_requested:
            try:
                xml = client.search(query, start=page_start, end=page_start + 99)
            except requests.exceptions.HTTPError as e:
                if "404" in str(e):
                    break
                print(f"    ⚠ Search error: {e}, retrying...")
                time.sleep(5)
                continue
            except Exception as e:
                print(f"    ⚠ Error: {e}, retrying...")
                time.sleep(5)
                continue

            refs, total = parse_search_results(xml)
            if not refs:
                break

            time.sleep(0.5)  # Rate limit

            # Process each patent
            for ref in refs:
                if shutdown_requested:
                    break

                try:
                    # Get biblio
                    biblio_xml = client.get_biblio(ref)
                    patent = parse_biblio(biblio_xml, ref)
                    if not patent:
                        continue

                    time.sleep(0.3)

                    # Get claims (optional, may not be available)
                    claims = client.get_claims(ref)
                    if claims:
                        patent["claims_text"] = claims[:50000]
                    time.sleep(0.3)

                    # Get citations
                    cit_xml = client.get_citations(ref)
                    citations = parse_citations_xml(cit_xml, ref)
                    time.sleep(0.3)

                    # Insert
                    insert_patent(conn, patent)
                    links_inserted = insert_citations(conn, citations)

                    total_patents += 1
                    total_links += links_inserted

                except Exception as e:
                    print(f"    ⚠ Error on {ref}: {e}")
                    time.sleep(2)
                    continue

            page_start += 100
            if page_start > total:
                break

            elapsed = time.time() - start_time
            print(f"    {page_start-1}/{total} patents | "
                  f"Total: {total_patents:,} patents, {total_links:,} links")

        # Move to next week
        current_date = week_end
        progress["last_date"] = current_date.strftime("%Y-%m-%d")
        progress["offset"] = 0
        progress["total_patents"] = total_patents
        progress["total_links"] = total_links
        save_progress(progress)

    # Resolve DOIs
    print("\n  Resolving DOIs to paper records...")
    cur = conn.cursor()
    cur.execute("""
        UPDATE patent_paper_citations ppc
        SET paper_id = p.id, paper_title = p.title
        FROM papers p
        WHERE ppc.paper_doi = p.doi
        AND ppc.paper_id IS NULL
        AND ppc.source = 'epo'
    """)
    resolved = cur.rowcount
    conn.commit()
    cur.close()
    print(f"  ✓ Resolved {resolved:,} DOI→paper links")

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  DONE in {elapsed:.0f}s")
    print(f"  Patents processed: {total_patents:,}")
    print(f"  Citation links: {total_links:,}")
    print(f"{'=' * 60}")

    conn.close()


if __name__ == "__main__":
    main()
