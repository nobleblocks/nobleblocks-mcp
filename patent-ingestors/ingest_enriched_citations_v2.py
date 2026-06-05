#!/usr/bin/env python3
"""
USPTO Enriched Citations Ingestor v2 — With Solr 10K Limit Workaround

The Developer Hub API has a 10,000 result window limit (standard Solr default).
This version partitions queries by techCenter to keep each partition under 10K.

For techCenters with > 10K records, further splits by examinerCitedReferenceIndicator.

Data: ~170K NPL records (academic papers cited in patents)
API: POST developer.uspto.gov/ds-api/enriched_cited_reference_metadata/v3/records

WARNING: Legacy Developer Hub being decommissioned May 29, 2026!

Run: DB_PASS=nb_papers_2026_prod python3 ingest_enriched_citations_v2.py
"""

import gzip
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests

# Config
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

API_BASE = "https://developer.uspto.gov/ds-api/enriched_cited_reference_metadata/v3/records"
PAGE_SIZE = 100
MAX_OFFSET = 9900  # Solr limit: can't go past 10000
DB_BATCH_SIZE = 500
PROGRESS_FILE = "/tmp/enriched_citations_v2_progress.json"

# Tech centers — partitions for NPL
TECH_CENTERS = ["1600", "1700", "2100", "2400", "2600", "2800", "3600", "3700"]

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
    return {
        "completed_partitions": [],
        "current_partition": None,
        "current_offset": 0,
        "total_ingested": 0,
        "phase": "npl",  # npl → patent → link
        "patent_completed_partitions": [],
        "patent_current_partition": None,
        "patent_current_offset": 0,
        "total_patent_ingested": 0,
    }


def save_progress(progress):
    progress["last_updated"] = datetime.now(timezone.utc).isoformat()
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def get_db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def fetch_page(criteria, start=0, rows=100, max_retries=3):
    """Fetch a page from the Enriched Citations API."""
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                API_BASE,
                data={"criteria": criteria, "start": start, "rows": rows},
                timeout=60,
            )
            resp.raise_for_status()

            try:
                content = gzip.decompress(resp.content).decode("utf-8")
            except (gzip.BadGzipFile, OSError):
                content = resp.text

            data = json.loads(content)

            if "response" in data:
                return data["response"].get("numFound", 0), data["response"].get("docs", [])
            elif "recordTotalCount" in data:
                return data["recordTotalCount"], data.get("results", [])
            else:
                return 0, []

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
            else:
                print(f"    ✗ API failed: {e}")
                return 0, []
        except json.JSONDecodeError:
            return 0, []

    return 0, []


def get_partitions(base_criteria):
    """Generate partitions that each have < 10K results."""
    partitions = []

    for tc in TECH_CENTERS:
        criteria = f"{base_criteria} AND techCenter:{tc}"
        total, _ = fetch_page(criteria, 0, 1)
        time.sleep(0.3)

        if total <= MAX_OFFSET + PAGE_SIZE:
            # Fits in one partition
            partitions.append({"criteria": criteria, "total": total, "label": f"TC={tc}"})
        else:
            # Split by examiner/applicant cited
            for examiner_val in ["true", "false"]:
                sub_criteria = f"{criteria} AND examinerCitedReferenceIndicator:{examiner_val}"
                sub_total, _ = fetch_page(sub_criteria, 0, 1)
                time.sleep(0.3)

                if sub_total <= MAX_OFFSET + PAGE_SIZE:
                    partitions.append({
                        "criteria": sub_criteria,
                        "total": sub_total,
                        "label": f"TC={tc},examiner={examiner_val}",
                    })
                else:
                    # Further split by citation category
                    for cat in ["X", "Y", "A", "E", "D", "L", "R"]:
                        cat_criteria = f"{sub_criteria} AND citationCategoryCode:{cat}"
                        cat_total, _ = fetch_page(cat_criteria, 0, 1)
                        time.sleep(0.2)
                        if cat_total > 0:
                            partitions.append({
                                "criteria": cat_criteria,
                                "total": cat_total,
                                "label": f"TC={tc},examiner={examiner_val},cat={cat}",
                            })

    return partitions


def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        return None


def insert_batch(conn, batch):
    """Bulk insert records."""
    if not batch:
        return

    sql = """
        INSERT INTO enriched_citation_context (
            patent_application_number, publication_number, cited_document_identifier,
            npl_indicator, office_action_date, office_action_category,
            citation_category_code, related_claims, passage_locations,
            examiner_cited, applicant_cited, tech_center, group_art_unit,
            quality_summary
        ) VALUES %s
        ON CONFLICT (patent_application_number, cited_document_identifier, office_action_date)
        DO NOTHING
    """
    values = [
        (
            r["patent_application_number"],
            r["publication_number"],
            r["cited_document_identifier"] or "",
            r["npl_indicator"],
            r["office_action_date"],
            r["office_action_category"],
            r["citation_category_code"],
            r["related_claims"],
            r["passage_locations"] if r["passage_locations"] else None,
            r["examiner_cited"],
            r["applicant_cited"],
            r["tech_center"],
            r["group_art_unit"],
            r["quality_summary"],
        )
        for r in batch
    ]

    cur = conn.cursor()
    try:
        psycopg2.extras.execute_values(
            cur, sql, values,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        # Fallback: individual inserts
        inserted = 0
        for v in values:
            try:
                cur.execute(
                    """INSERT INTO enriched_citation_context (
                        patent_application_number, publication_number, cited_document_identifier,
                        npl_indicator, office_action_date, office_action_category,
                        citation_category_code, related_claims, passage_locations,
                        examiner_cited, applicant_cited, tech_center, group_art_unit,
                        quality_summary
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (patent_application_number, cited_document_identifier, office_action_date)
                    DO NOTHING""",
                    v,
                )
                conn.commit()
                inserted += 1
            except Exception:
                conn.rollback()
        print(f"    ⚠ Fallback: {inserted}/{len(values)}")
    cur.close()


def ingest_partitions(conn, partitions, progress, is_npl=True):
    """Ingest all records from a list of partitions."""
    phase_key = "" if is_npl else "patent_"
    completed_key = f"{phase_key}completed_partitions"
    current_key = f"{phase_key}current_partition"
    offset_key = f"{phase_key}current_offset"
    total_key = "total_ingested" if is_npl else "total_patent_ingested"

    completed = set(progress.get(completed_key, []))
    current = progress.get(current_key)
    offset = progress.get(offset_key, 0)
    total_ingested = progress.get(total_key, 0)

    grand_total = sum(p["total"] for p in partitions)
    print(f"  {len(partitions)} partitions, {grand_total:,} total records")

    for i, partition in enumerate(partitions):
        if shutdown_requested:
            break

        label = partition["label"]
        criteria = partition["criteria"]
        part_total = partition["total"]

        if label in completed:
            continue

        # Resume from current partition's offset
        if current and current != label:
            continue
        if not current:
            offset = 0

        progress[current_key] = label
        print(f"\n  [{i+1}/{len(partitions)}] {label} ({part_total:,} records, offset={offset})")

        batch = []

        while offset <= min(part_total, MAX_OFFSET) and not shutdown_requested:
            _, docs = fetch_page(criteria, offset, PAGE_SIZE)

            if not docs:
                break

            for doc in docs:
                batch.append({
                    "patent_application_number": doc.get("patentApplicationNumber", ""),
                    "publication_number": doc.get("publicationNumber", ""),
                    "cited_document_identifier": doc.get("citedDocumentIdentifier", ""),
                    "npl_indicator": is_npl,
                    "office_action_date": parse_date(doc.get("officeActionDate")),
                    "office_action_category": doc.get("officeActionCategory", ""),
                    "citation_category_code": doc.get("citationCategoryCode", ""),
                    "related_claims": doc.get("relatedClaimNumberText", ""),
                    "passage_locations": doc.get("passageLocationText", []),
                    "examiner_cited": doc.get("examinerCitedReferenceIndicator", False),
                    "applicant_cited": doc.get("applicantCitedExaminerReferenceIndicator", False),
                    "tech_center": doc.get("techCenter", ""),
                    "group_art_unit": doc.get("groupArtUnitNumber", ""),
                    "quality_summary": doc.get("qualitySummaryText", ""),
                })

            if len(batch) >= DB_BATCH_SIZE:
                insert_batch(conn, batch)
                total_ingested += len(batch)
                batch = []

            offset += PAGE_SIZE

            # Progress save every 10 pages
            if (offset // PAGE_SIZE) % 10 == 0:
                progress[offset_key] = offset
                progress[total_key] = total_ingested
                save_progress(progress)

            time.sleep(0.4)  # Rate limit

        # Flush remaining batch
        if batch:
            insert_batch(conn, batch)
            total_ingested += len(batch)
            batch = []

        # Mark partition complete
        completed.add(label)
        progress[completed_key] = list(completed)
        progress[current_key] = None
        progress[offset_key] = 0
        progress[total_key] = total_ingested
        save_progress(progress)
        print(f"    ✓ {label} done — total so far: {total_ingested:,}")

    return total_ingested


def link_to_patents(conn):
    """Link enriched citations to patents table."""
    print("\n  Linking enriched citations to patent records...")
    cur = conn.cursor()
    cur.execute("""
        UPDATE enriched_citation_context ecc
        SET patent_id = p.patent_id
        FROM patents p
        WHERE ecc.patent_id IS NULL
          AND ecc.publication_number != ''
          AND p.patent_id = 'US-' || ecc.publication_number
    """)
    linked = cur.rowcount
    conn.commit()
    cur.close()
    print(f"  ✓ Linked {linked:,} citations to patents")


def main():
    print("=" * 60)
    print("  USPTO Enriched Citations Ingestor v2")
    print("  Partition-based (workaround for 10K Solr limit)")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    conn = get_db()
    progress = load_progress()
    start_time = time.time()

    # Phase 1: NPL citations
    if progress.get("phase", "npl") == "npl":
        print("\n═══ Phase 1: NPL Citations (academic papers cited in patents) ═══")
        print("  Computing partitions...")
        partitions = get_partitions("nplIndicator:true")
        total = ingest_partitions(conn, partitions, progress, is_npl=True)
        print(f"\n  ═══ NPL Phase Complete: {total:,} records ═══")

        if not shutdown_requested:
            progress["phase"] = "patent"
            save_progress(progress)

    # Phase 2: Patent citations with category codes (X/Y = most valuable)
    if progress.get("phase") == "patent" and not shutdown_requested:
        print("\n═══ Phase 2: Patent Citations with Context (X/Y categories) ═══")
        print("  Computing partitions...")
        partitions = get_partitions("nplIndicator:false AND citationCategoryCode:(X OR Y)")
        total = ingest_partitions(conn, partitions, progress, is_npl=False)
        print(f"\n  ═══ Patent Phase Complete: {total:,} records ═══")

        if not shutdown_requested:
            progress["phase"] = "link"
            save_progress(progress)

    # Phase 3: Link to patents
    if progress.get("phase") == "link" and not shutdown_requested:
        link_to_patents(conn)
        progress["phase"] = "done"
        save_progress(progress)

    elapsed = (time.time() - start_time) / 60
    print(f"\n{'═'*60}")
    print(f"  COMPLETE in {elapsed:.1f} minutes")
    print(f"  NPL ingested: {progress.get('total_ingested', 0):,}")
    print(f"  Patent ingested: {progress.get('total_patent_ingested', 0):,}")
    print(f"{'═'*60}")

    conn.close()


if __name__ == "__main__":
    main()
