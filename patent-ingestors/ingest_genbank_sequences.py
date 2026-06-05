#!/usr/bin/env python3
"""
GenBank Patent Sequence Ingestor

Fetches patent-associated sequences from NCBI GenBank.
These are DNA/RNA/protein sequences that are referenced in patents.

Source: NCBI Entrez API (E-utilities)
Database: nuccore (nucleotide) + protein
Query: "patent"[Properties]

Pharma/biotech VCs want to know:
- Which gene sequences just got patented?
- Which patents cover a specific gene/protein?
- What organisms/diseases are being targeted?

Rate limit: 3 req/sec without API key, 10 req/sec with key.
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
from datetime import datetime

# Config
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

NCBI_API_KEY = os.environ.get("NCBI_API_KEY", "")  # Optional but recommended
NCBI_EMAIL = os.environ.get("NCBI_EMAIL", "admin@nobleblocks.com")
NCBI_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

BATCH_SIZE = 500
RETMAX = 500  # Max records per NCBI request
PROGRESS_FILE = "/tmp/genbank_patent_progress.json"

# Rate limiting
DELAY = 0.11 if NCBI_API_KEY else 0.34  # 10/s with key, 3/s without

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
    return {"retstart": 0, "total_sequences": 0, "total_patents_linked": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def esearch_patent_sequences(retstart=0, db="nuccore"):
    """Search for patent-associated sequences."""
    params = {
        "db": db,
        "term": '"patent"[Properties]',
        "retstart": retstart,
        "retmax": RETMAX,
        "rettype": "count" if retstart == 0 else "",
        "usehistory": "y",
        "email": NCBI_EMAIL,
        "tool": "nobleblocks_patent_ingestor",
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY

    resp = requests.get(f"{NCBI_BASE}/esearch.fcgi", params=params, timeout=30)
    resp.raise_for_status()
    return resp.text


def efetch_sequences(ids, db="nuccore"):
    """Fetch sequence details for a batch of IDs."""
    params = {
        "db": db,
        "id": ",".join(ids),
        "rettype": "gb",
        "retmode": "xml",
        "email": NCBI_EMAIL,
        "tool": "nobleblocks_patent_ingestor",
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY

    resp = requests.get(f"{NCBI_BASE}/efetch.fcgi", params=params, timeout=60)
    resp.raise_for_status()
    return resp.text


def parse_genbank_xml(xml_text):
    """Parse GenBank XML to extract patent sequence info."""
    sequences = []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    for seq_entry in root.findall(".//GBSeq"):
        seq_data = {}

        # Accession
        accession = seq_entry.find("GBSeq_primary-accession")
        seq_data["sequence_id"] = accession.text if accession is not None else None
        if not seq_data["sequence_id"]:
            continue

        # Sequence type
        moltype = seq_entry.find("GBSeq_moltype")
        if moltype is not None:
            mt = moltype.text.lower()
            if "dna" in mt:
                seq_data["sequence_type"] = "DNA"
            elif "rna" in mt:
                seq_data["sequence_type"] = "RNA"
            elif "protein" in mt or "aa" in mt:
                seq_data["sequence_type"] = "protein"
            else:
                seq_data["sequence_type"] = mt
        else:
            seq_data["sequence_type"] = "unknown"

        # Organism
        organism = seq_entry.find("GBSeq_organism")
        seq_data["organism"] = organism.text if organism is not None else None

        # Length
        length = seq_entry.find("GBSeq_length")
        seq_data["sequence_length"] = int(length.text) if length is not None and length.text else None

        # Definition/description
        definition = seq_entry.find("GBSeq_definition")
        seq_data["description"] = definition.text if definition is not None else None

        # Extract patent number from multiple sources
        patent_id = None
        gene_name = None

        # Method 1: Parse from Comment field (~PN tag)
        # Format: "...~PN WO 2025244108-A/104~PD 27-NOV-2025..."
        comment = seq_entry.find("GBSeq_comment")
        if comment is not None and comment.text:
            pn_match = re.search(r'~PN\s+([A-Z]{2})\s*(\d+)[-\s]*([A-Z]\d?)?', comment.text)
            if pn_match:
                country = pn_match.group(1)
                number = pn_match.group(2)
                kind = pn_match.group(3) or "A"
                patent_id = f"{country}-{number}-{kind}"

        # Method 2: Parse from Keywords
        if not patent_id:
            keywords_el = seq_entry.find("GBSeq_keywords")
            if keywords_el is not None:
                for kw in keywords_el.findall("GBKeyword"):
                    if kw.text:
                        kw_match = re.search(r'([A-Z]{2})\s*(\d{5,})[-\s]*([A-Z]\d?)?', kw.text)
                        if kw_match:
                            country = kw_match.group(1)
                            number = kw_match.group(2)
                            kind = kw_match.group(3) or "A"
                            patent_id = f"{country}-{number}-{kind}"
                            break

        # Method 3: Parse from Reference Journal
        # Format: "WO2025244108-A 104 27-NOV-2025 The University of Tokyo"
        if not patent_id:
            for ref in seq_entry.findall(".//GBReference"):
                journal = ref.find("GBReference_journal")
                if journal is not None and journal.text:
                    # Try "Patent: XX NNNNN" format (older records)
                    pat_match = re.search(r'Patent:\s*(\w+)\s+(\d+)\s*(\w*)', journal.text)
                    if pat_match:
                        patent_id = f"{pat_match.group(1)}-{pat_match.group(2)}-{pat_match.group(3) or 'A'}"
                        break
                    # Try direct patent number format: "WO2025244108-A ..."
                    direct_match = re.search(r'^([A-Z]{2})(\d{5,})[-\s]*([A-Z]\d?)?', journal.text)
                    if direct_match:
                        patent_id = f"{direct_match.group(1)}-{direct_match.group(2)}-{direct_match.group(3) or 'A'}"
                        break

        # Method 4: Parse from Definition
        if not patent_id:
            if definition is not None and definition.text:
                def_match = re.search(r'([A-Z]{2})\s*(\d{7,})[-\s]*([A-Z]\d?)?', definition.text)
                if def_match:
                    patent_id = f"{def_match.group(1)}-{def_match.group(2)}-{def_match.group(3) or 'A'}"

        # Extract gene name from features
        for ref in seq_entry.findall(".//GBReference"):
            title = ref.find("GBReference_title")
            if title is not None and title.text:
                gene_match = re.search(r'\b([A-Z][A-Z0-9]{2,}[a-z]?\d*)\b', title.text)
                if gene_match:
                    gene_name = gene_match.group(1)

        # Also check features for gene annotation
        for feature in seq_entry.findall(".//GBFeature"):
            feature_key = feature.find("GBFeature_key")
            if feature_key is not None and feature_key.text == "gene":
                for qual in feature.findall(".//GBQualifier"):
                    qual_name = qual.find("GBQualifier_name")
                    qual_value = qual.find("GBQualifier_value")
                    if qual_name is not None and qual_name.text == "gene" and qual_value is not None:
                        gene_name = qual_value.text
                        break

        seq_data["patent_id"] = patent_id
        seq_data["gene_name"] = gene_name

        if patent_id:  # Only store sequences with patent links
            sequences.append(seq_data)

    return sequences


def insert_sequences_batch(conn, sequences):
    """Bulk insert patent sequences."""
    if not sequences:
        return 0

    # First ensure patents exist
    patent_ids = set(s["patent_id"] for s in sequences if s.get("patent_id"))
    if patent_ids:
        cur = conn.cursor()
        for pid in patent_ids:
            cur.execute("""
                INSERT INTO patents (patent_id, source, jurisdiction)
                VALUES (%s, 'genbank', %s)
                ON CONFLICT (patent_id) DO NOTHING
            """, (pid, pid.split("-")[0] if "-" in pid else "US"))
        conn.commit()
        cur.close()

    # Insert sequences
    sql = """
        INSERT INTO patent_sequences (patent_id, sequence_id, sequence_type,
                                     organism, gene_name, sequence_length, description)
        VALUES %s
        ON CONFLICT (patent_id, sequence_id) DO NOTHING
    """
    values = [
        (s["patent_id"], s["sequence_id"], s["sequence_type"],
         s["organism"], s["gene_name"], s["sequence_length"], s.get("description", "")[:500])
        for s in sequences
    ]
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, sql, values,
        template="(%s, %s, %s, %s, %s, %s, %s)")
    conn.commit()
    inserted = cur.rowcount
    cur.close()
    return inserted


def main():
    print("=" * 60)
    print("  GenBank Patent Sequence Ingestor")
    print("=" * 60)
    print(f"  DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  NCBI API key: {'SET' if NCBI_API_KEY else 'NOT SET (3 req/s limit)'}")
    print()

    conn = get_db_connection()
    progress = load_progress()
    retstart = progress.get("retstart", 0)
    total_sequences = progress.get("total_sequences", 0)

    # First, get total count
    print("  Checking total patent sequences available...")
    search_xml = esearch_patent_sequences(retstart=0)
    root = ET.fromstring(search_xml)
    total_count = int(root.find(".//Count").text)
    print(f"  Total patent sequences in GenBank: {total_count:,}")
    time.sleep(DELAY)

    if retstart > 0:
        print(f"  Resuming from position {retstart:,}")

    start_time = time.time()
    batch_count = 0

    while retstart < total_count and not shutdown_requested:
        # Search for IDs
        params = {
            "db": "nuccore",
            "term": '"patent"[Properties]',
            "retstart": retstart,
            "retmax": RETMAX,
            "rettype": "uilist",
            "email": NCBI_EMAIL,
            "tool": "nobleblocks_patent_ingestor",
        }
        if NCBI_API_KEY:
            params["api_key"] = NCBI_API_KEY

        try:
            resp = requests.get(f"{NCBI_BASE}/esearch.fcgi", params=params, timeout=30)
            resp.raise_for_status()
            search_root = ET.fromstring(resp.text)
            ids = [id_el.text for id_el in search_root.findall(".//Id")]
        except Exception as e:
            print(f"  ⚠ Search error at {retstart}: {e}, retrying...")
            time.sleep(5)
            continue

        if not ids:
            break

        time.sleep(DELAY)

        # Fetch details in sub-batches of 100
        for i in range(0, len(ids), 100):
            if shutdown_requested:
                break

            sub_ids = ids[i:i+100]
            try:
                fetch_xml = efetch_sequences(sub_ids)
                sequences = parse_genbank_xml(fetch_xml)

                if sequences:
                    inserted = insert_sequences_batch(conn, sequences)
                    total_sequences += inserted
            except Exception as e:
                print(f"  ⚠ Fetch error: {e}")
                time.sleep(5)
                continue

            time.sleep(DELAY)

        retstart += RETMAX
        batch_count += 1

        if batch_count % 10 == 0:
            elapsed = time.time() - start_time
            rate = total_sequences / elapsed if elapsed > 0 else 0
            print(f"  Progress: {retstart:,}/{total_count:,} "
                  f"({100*retstart/total_count:.1f}%) | "
                  f"{total_sequences:,} sequences stored ({rate:.0f}/s)")

            progress["retstart"] = retstart
            progress["total_sequences"] = total_sequences
            save_progress(progress)

    # Final save
    progress["retstart"] = retstart
    progress["total_sequences"] = total_sequences
    progress["completed"] = retstart >= total_count
    progress["last_run"] = datetime.now().isoformat()
    save_progress(progress)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  DONE in {elapsed:.0f}s")
    print(f"  Sequences processed: {retstart:,}/{total_count:,}")
    print(f"  Patent sequences stored: {total_sequences:,}")
    print(f"{'=' * 60}")

    conn.close()


if __name__ == "__main__":
    main()
