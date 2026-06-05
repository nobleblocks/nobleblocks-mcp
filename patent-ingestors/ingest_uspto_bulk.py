#!/usr/bin/env python3
"""
USPTO Bulk Full-Text Patent Ingestor

Downloads and parses USPTO weekly patent grant and application XML dumps.
Source: https://bulkdata.uspto.gov/

Grant files: Patent Grant Full Text (XML 4.x)
Application files: Patent Application Full Text (XML 4.x)

Each weekly file is ~200-400MB compressed, ~2-4GB uncompressed.
Total historical: ~12M US patents.

This ingestor:
1. Downloads weekly XML ZIP files from USPTO bulk data
2. Parses patent XML to extract: title, abstract, claims, citations, assignees
3. Extracts cited non-patent literature (NPL = academic paper citations)
4. Inserts patents and patent→paper citation links into Paper DB
"""

import requests
import psycopg2
import psycopg2.extras
import xml.etree.ElementTree as ET
import zipfile
import gzip
import os
import sys
import json
import re
import time
import signal
from datetime import datetime, date
from io import BytesIO
from pathlib import Path

# Config
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

USPTO_GRANTS_BASE = "https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext"
USPTO_APPS_BASE = "https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext"
DOWNLOAD_DIR = "/tmp/uspto_downloads"
BATCH_SIZE = 1000
PROGRESS_FILE = "/tmp/uspto_ingest_progress.json"

# DOI pattern for extracting DOIs from NPL citations
DOI_PATTERN = re.compile(r'10\.\d{4,9}/[^\s,;"\'>]+')

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
    return {"processed_files": [], "total_patents": 0, "total_links": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def list_grant_files(year):
    """List available grant XML files for a given year."""
    url = f"{USPTO_GRANTS_BASE}/{year}/"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            return []
        # Parse the directory listing for ZIP files
        files = re.findall(r'href="(ipg\d+\.zip)"', resp.text)
        return [f"{url}{f}" for f in files]
    except Exception as e:
        print(f"  ⚠ Error listing {year}: {e}")
        return []


def list_application_files(year):
    """List available application XML files for a given year."""
    url = f"{USPTO_APPS_BASE}/{year}/"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            return []
        files = re.findall(r'href="(ipa\d+\.zip)"', resp.text)
        return [f"{url}{f}" for f in files]
    except Exception as e:
        print(f"  ⚠ Error listing {year}: {e}")
        return []


def download_file(url, dest_dir):
    """Download a file to local disk."""
    os.makedirs(dest_dir, exist_ok=True)
    filename = url.split("/")[-1]
    filepath = os.path.join(dest_dir, filename)

    if os.path.exists(filepath):
        return filepath

    print(f"    Downloading {filename}...")
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()

    with open(filepath, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024*1024):
            f.write(chunk)

    return filepath


def parse_patent_xml(xml_text):
    """Parse a single patent XML document."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None, []

    patent = {}
    citations = []

    # Patent number
    doc_number = root.find(".//publication-reference/document-id/doc-number")
    kind = root.find(".//publication-reference/document-id/kind")
    country = root.find(".//publication-reference/document-id/country")

    if doc_number is not None:
        patent_num = doc_number.text or ""
        kind_code = kind.text if kind is not None else "A1"
        country_code = country.text if country is not None else "US"
        patent["patent_id"] = f"{country_code}-{patent_num}-{kind_code}"
        patent["jurisdiction"] = country_code
    else:
        return None, []

    # Title
    title_el = root.find(".//invention-title")
    patent["title"] = title_el.text if title_el is not None else ""

    # Abstract
    abstract_el = root.find(".//abstract")
    if abstract_el is not None:
        patent["abstract"] = " ".join(abstract_el.itertext()).strip()
    else:
        patent["abstract"] = ""

    # Claims
    claims_el = root.find(".//claims")
    if claims_el is not None:
        patent["claims_text"] = " ".join(claims_el.itertext()).strip()[:50000]  # Cap at 50k chars
    else:
        patent["claims_text"] = ""

    # Filing date
    filing_date = root.find(".//application-reference/document-id/date")
    if filing_date is not None and filing_date.text:
        try:
            d = filing_date.text
            patent["filing_date"] = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        except (ValueError, IndexError):
            patent["filing_date"] = None
    else:
        patent["filing_date"] = None

    # Grant date
    grant_date = root.find(".//publication-reference/document-id/date")
    if grant_date is not None and grant_date.text:
        try:
            d = grant_date.text
            patent["grant_date"] = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        except (ValueError, IndexError):
            patent["grant_date"] = None
    else:
        patent["grant_date"] = None

    # Assignee
    assignee_el = root.find(".//assignees/assignee/addressbook/orgname")
    if assignee_el is not None:
        patent["assignee"] = assignee_el.text
        patent["assignee_type"] = "corporate"
    else:
        assignee_el = root.find(".//assignees/assignee/addressbook/last-name")
        if assignee_el is not None:
            first = root.find(".//assignees/assignee/addressbook/first-name")
            patent["assignee"] = f"{first.text} {assignee_el.text}" if first is not None else assignee_el.text
            patent["assignee_type"] = "individual"
        else:
            patent["assignee"] = None
            patent["assignee_type"] = None

    # Inventors
    inventors = []
    for inv in root.findall(".//inventors/inventor/addressbook"):
        last = inv.find("last-name")
        first = inv.find("first-name")
        if last is not None:
            name = f"{first.text} {last.text}" if first is not None else last.text
            inventors.append(name)
    patent["inventors"] = inventors

    # IPC codes
    ipc_codes = []
    for ipc in root.findall(".//classifications-ipcr/classification-ipcr"):
        section = ipc.find("section")
        cls = ipc.find("class")
        subclass = ipc.find("subclass")
        if section is not None and cls is not None:
            code = f"{section.text}{cls.text}"
            if subclass is not None:
                code += subclass.text
            ipc_codes.append(code)
    patent["ipc_codes"] = ipc_codes[:20]

    # CPC codes
    cpc_codes = []
    for cpc in root.findall(".//us-bibliographic-data-grant/us-field-of-classification-search/classification-cpc-text"):
        if cpc.text:
            cpc_codes.append(cpc.text.strip())
    patent["cpc_codes"] = cpc_codes[:20]

    patent["source"] = "uspto"

    # Non-patent literature citations (NPL) → these cite academic papers!
    for npl in root.findall(".//references-cited/citation"):
        npl_el = npl.find("nplcit/othercit")
        if npl_el is not None and npl_el.text:
            npl_text = npl_el.text
            # Try to extract DOI
            doi_match = DOI_PATTERN.search(npl_text)
            if doi_match:
                citations.append({
                    "patent_id": patent["patent_id"],
                    "paper_doi": doi_match.group(0).rstrip(".),"),
                    "citation_context": npl_text[:500],
                    "citation_type": "npl",
                    "source": "uspto",
                })
            else:
                # Store the NPL text even without DOI — can resolve later
                citations.append({
                    "patent_id": patent["patent_id"],
                    "paper_doi": None,
                    "citation_context": npl_text[:500],
                    "citation_type": "npl",
                    "source": "uspto",
                })

    return patent, citations


def process_zip_file(filepath, conn):
    """Process a USPTO ZIP file containing multiple patent XMLs."""
    patents = []
    all_citations = []
    count = 0

    try:
        with zipfile.ZipFile(filepath) as zf:
            for name in zf.namelist():
                if not name.endswith(".xml"):
                    continue
                with zf.open(name) as f:
                    content = f.read().decode("utf-8", errors="replace")

                # USPTO XML files contain multiple patents separated by <?xml...?>
                # Split on XML declaration
                docs = re.split(r'<\?xml[^?]*\?>', content)

                for doc in docs:
                    if not doc.strip() or "<us-patent-grant" not in doc and "<us-patent-application" not in doc:
                        continue

                    # Ensure proper XML
                    if not doc.strip().startswith("<"):
                        continue

                    patent, citations = parse_patent_xml(doc)
                    if patent and patent.get("patent_id"):
                        patents.append(patent)
                        all_citations.extend(citations)
                        count += 1

                    if len(patents) >= BATCH_SIZE:
                        insert_patents_batch(conn, patents)
                        insert_citations_batch(conn, all_citations)
                        patents = []
                        all_citations = []

                    if shutdown_requested:
                        break

                if shutdown_requested:
                    break

    except zipfile.BadZipFile:
        print(f"    ⚠ Bad ZIP file: {filepath}")
        return 0, 0

    # Flush remaining
    if patents:
        insert_patents_batch(conn, patents)
    if all_citations:
        insert_citations_batch(conn, all_citations)

    return count, len(all_citations)


def insert_patents_batch(conn, patents):
    """Bulk insert patents."""
    if not patents:
        return

    sql = """
        INSERT INTO patents (patent_id, title, abstract, claims_text, filing_date,
                            grant_date, assignee, assignee_type, inventors, ipc_codes,
                            cpc_codes, jurisdiction, source)
        VALUES %s
        ON CONFLICT (patent_id) DO UPDATE SET
            claims_text = COALESCE(EXCLUDED.claims_text, patents.claims_text),
            updated_at = NOW()
    """
    values = [
        (p["patent_id"], p.get("title"), p.get("abstract"), p.get("claims_text"),
         p.get("filing_date"), p.get("grant_date"), p.get("assignee"),
         p.get("assignee_type"), p.get("inventors", []), p.get("ipc_codes", []),
         p.get("cpc_codes", []), p.get("jurisdiction"), p["source"])
        for p in patents
    ]
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, sql, values,
        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    conn.commit()
    cur.close()


def insert_citations_batch(conn, citations):
    """Bulk insert patent→paper citation links."""
    if not citations:
        return

    # Only insert citations that have a DOI (can resolve others later)
    doi_citations = [c for c in citations if c.get("paper_doi")]
    context_citations = [c for c in citations if not c.get("paper_doi") and c.get("citation_context")]

    if doi_citations:
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
        conn.commit()
        cur.close()


def main():
    print("=" * 60)
    print("  USPTO Bulk Full-Text Patent Ingestor")
    print("=" * 60)
    print(f"  DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  Download dir: {DOWNLOAD_DIR}")
    print()

    conn = get_db_connection()
    progress = load_progress()
    processed_files = set(progress.get("processed_files", []))
    total_patents = progress.get("total_patents", 0)
    total_links = progress.get("total_links", 0)

    # Process recent years (most relevant for VC intelligence)
    years = list(range(2024, 2027))  # Start with recent, can go back to 2005+ later

    start_time = time.time()

    for year in years:
        if shutdown_requested:
            break

        print(f"\n  Year {year}:")
        print(f"  {'─' * 40}")

        # Get grant files
        grant_files = list_grant_files(year)
        app_files = list_application_files(year)
        all_files = grant_files + app_files

        print(f"    Found {len(grant_files)} grant files, {len(app_files)} application files")

        for file_url in all_files:
            if shutdown_requested:
                break

            filename = file_url.split("/")[-1]
            if filename in processed_files:
                continue

            try:
                filepath = download_file(file_url, DOWNLOAD_DIR)
                patents_count, links_count = process_zip_file(filepath, conn)
                total_patents += patents_count
                total_links += links_count

                processed_files.add(filename)
                progress["processed_files"] = list(processed_files)
                progress["total_patents"] = total_patents
                progress["total_links"] = total_links
                save_progress(progress)

                print(f"    ✓ {filename}: {patents_count:,} patents, {links_count:,} NPL citations")

                # Clean up downloaded file to save disk
                os.remove(filepath)

            except Exception as e:
                print(f"    ⚠ Error processing {filename}: {e}")
                continue

    # Resolve DOIs to paper IDs
    print("\n  Resolving DOIs to paper records...")
    cur = conn.cursor()
    cur.execute("""
        UPDATE patent_paper_citations ppc
        SET paper_id = p.id,
            paper_title = p.title
        FROM papers p
        WHERE ppc.paper_doi = p.doi
        AND ppc.paper_id IS NULL
        AND ppc.source = 'uspto'
    """)
    resolved = cur.rowcount
    conn.commit()
    cur.close()
    print(f"  ✓ Resolved {resolved:,} DOI→paper links")

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  DONE in {elapsed:.0f}s")
    print(f"  Patents processed: {total_patents:,}")
    print(f"  NPL citation links: {total_links:,}")
    print(f"  DOIs resolved: {resolved:,}")
    print(f"{'=' * 60}")

    conn.close()


if __name__ == "__main__":
    main()
