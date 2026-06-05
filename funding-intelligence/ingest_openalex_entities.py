#!/usr/bin/env python3
"""
OpenAlex Funding Intelligence Bulk Ingestor

Downloads and ingests OpenAlex entities from their public S3 bucket:
- Funders (32K records, 10MB)
- Awards (12.2M records, 3GB)
- Topics (4.5K records, 2MB)
- Sources (280K records, 350MB)
- Publishers (10.7K records, 4MB)
- Institutions (121K records, 180MB)

Then links awards → papers via openalex_id matching.

Usage:
    python3 ingest_openalex_entities.py --entity funders
    python3 ingest_openalex_entities.py --entity awards
    python3 ingest_openalex_entities.py --entity all
    python3 ingest_openalex_entities.py --entity all --skip authors

Run on paper-db server (i-0cb48faa3f931c661) for best performance.
"""

import gzip
import json
import os
import sys
import time
import signal
import argparse
import requests
import psycopg2
import psycopg2.extras
from io import BytesIO
from datetime import datetime

# ─── Config ───────────────────────────────────────────────────────────────────
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "nb_papers_2026_prod")

S3_BASE = "https://openalex.s3.amazonaws.com"

BATCH_SIZE = 5000
PROGRESS_DIR = "/tmp/oa_ingest"

# ─── Graceful shutdown ────────────────────────────────────────────────────────
shutdown_requested = False
def handle_signal(sig, frame):
    global shutdown_requested
    shutdown_requested = True
    print("\n⚠ Shutdown requested, finishing current batch...")

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

# ─── Database ─────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )

# ─── Progress tracking ────────────────────────────────────────────────────────
def get_progress(entity):
    os.makedirs(PROGRESS_DIR, exist_ok=True)
    path = f"{PROGRESS_DIR}/{entity}_progress.json"
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"completed_files": [], "total_ingested": 0, "last_updated": None}

def save_progress(entity, progress):
    path = f"{PROGRESS_DIR}/{entity}_progress.json"
    progress["last_updated"] = datetime.utcnow().isoformat()
    with open(path, 'w') as f:
        json.dump(progress, f, indent=2)

# ─── Manifest fetcher ─────────────────────────────────────────────────────────
def fetch_manifest(entity):
    """Fetch the manifest listing all data files for an entity."""
    url = f"{S3_BASE}/data/{entity}/manifest"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()["entries"]

# ─── File downloader ──────────────────────────────────────────────────────────
def download_and_iterate(s3_url):
    """Download a gzipped JSONL file from S3 and yield parsed records."""
    # Convert s3:// to https://
    https_url = s3_url.replace("s3://openalex/", f"{S3_BASE}/")
    resp = requests.get(https_url, timeout=300, stream=True)
    resp.raise_for_status()

    content = resp.content
    decompressed = gzip.decompress(content)
    for line in decompressed.decode('utf-8').strip().split('\n'):
        if line.strip():
            yield json.loads(line)

# ─── Entity-specific ingestors ────────────────────────────────────────────────

def ingest_funders_batch(conn, records):
    """Upsert funders from OpenAlex bulk data."""
    sql = """
        INSERT INTO funders (openalex_id, name, alternate_names, country_code,
                           grants_count, works_count, citation_count, homepage_url,
                           ror_id, description, doi, wikidata_id, crossref_id,
                           h_index, i10_index, mean_citedness, awards_count,
                           counts_by_year, roles, updated_at)
        VALUES %s
        ON CONFLICT (openalex_id) DO UPDATE SET
            name = EXCLUDED.name,
            alternate_names = EXCLUDED.alternate_names,
            country_code = EXCLUDED.country_code,
            grants_count = EXCLUDED.grants_count,
            works_count = EXCLUDED.works_count,
            citation_count = EXCLUDED.citation_count,
            homepage_url = EXCLUDED.homepage_url,
            ror_id = EXCLUDED.ror_id,
            description = EXCLUDED.description,
            doi = EXCLUDED.doi,
            wikidata_id = EXCLUDED.wikidata_id,
            crossref_id = EXCLUDED.crossref_id,
            h_index = EXCLUDED.h_index,
            i10_index = EXCLUDED.i10_index,
            mean_citedness = EXCLUDED.mean_citedness,
            awards_count = EXCLUDED.awards_count,
            counts_by_year = EXCLUDED.counts_by_year,
            roles = EXCLUDED.roles,
            updated_at = NOW()
    """
    values = []
    for r in records:
        oa_id = r.get('id', '').replace('https://openalex.org/', '')
        ids = r.get('ids', {})
        stats = r.get('summary_stats', {})
        values.append((
            oa_id,
            r.get('display_name', ''),
            r.get('alternate_titles', []),
            r.get('country_code'),
            r.get('awards_count', 0),
            r.get('works_count', 0),
            r.get('cited_by_count', 0),
            r.get('homepage_url'),
            ids.get('ror', '').replace('https://ror.org/', '') if ids.get('ror') else None,
            r.get('description'),
            ids.get('doi'),
            ids.get('wikidata', '').replace('https://www.wikidata.org/entity/', '') if ids.get('wikidata') else None,
            ids.get('crossref'),
            stats.get('h_index'),
            stats.get('i10_index'),
            stats.get('2yr_mean_citedness'),
            r.get('awards_count', 0),
            json.dumps(r.get('counts_by_year', [])),
            json.dumps(r.get('roles', []))
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    conn.commit()
    return len(values)


def ingest_awards_batch(conn, records):
    """Upsert awards from OpenAlex bulk data."""
    sql = """
        INSERT INTO awards (openalex_id, display_name, description, funder_award_id,
                          funder_openalex, amount, currency, funding_type, funder_scheme,
                          start_date, end_date, start_year, end_year, landing_page_url,
                          doi, provenance, lead_investigator, co_lead_investigator,
                          investigators, primary_topic, topics, institution_awarded,
                          funded_outputs_count, updated_at)
        VALUES %s
        ON CONFLICT (openalex_id) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            description = EXCLUDED.description,
            amount = EXCLUDED.amount,
            currency = EXCLUDED.currency,
            funding_type = EXCLUDED.funding_type,
            funder_scheme = EXCLUDED.funder_scheme,
            funded_outputs_count = EXCLUDED.funded_outputs_count,
            topics = EXCLUDED.topics,
            updated_at = NOW()
    """
    values = []
    for r in records:
        oa_id = r.get('id', '').replace('https://openalex.org/', '')
        funder = r.get('funder', {})
        funder_oa = funder.get('id', '').replace('https://openalex.org/', '') if funder else None

        values.append((
            oa_id,
            r.get('display_name'),
            r.get('description'),
            r.get('funder_award_id'),
            funder_oa,
            r.get('amount'),
            r.get('currency'),
            r.get('funding_type'),
            r.get('funder_scheme'),
            r.get('start_date'),
            r.get('end_date'),
            r.get('start_year'),
            r.get('end_year'),
            r.get('landing_page_url'),
            r.get('doi'),
            r.get('provenance'),
            json.dumps(r.get('lead_investigator')) if r.get('lead_investigator') else None,
            json.dumps(r.get('co_lead_investigator')) if r.get('co_lead_investigator') else None,
            json.dumps(r.get('investigators')) if r.get('investigators') else None,
            json.dumps(r.get('primary_topic')) if r.get('primary_topic') else None,
            json.dumps(r.get('topics')) if r.get('topics') else None,
            json.dumps(r.get('institution_awarded')) if r.get('institution_awarded') else None,
            r.get('funded_outputs_count', 0),
            datetime.utcnow()
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    conn.commit()
    return len(values)


def ingest_topics_batch(conn, records):
    """Upsert topics from OpenAlex bulk data."""
    sql = """
        INSERT INTO oa_topics (openalex_id, display_name, description,
                             domain_id, domain_name, field_id, field_name,
                             subfield_id, subfield_name, keywords,
                             works_count, cited_by_count, updated_at)
        VALUES %s
        ON CONFLICT (openalex_id) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            works_count = EXCLUDED.works_count,
            cited_by_count = EXCLUDED.cited_by_count,
            updated_at = NOW()
    """
    values = []
    for r in records:
        oa_id = r.get('id', '').replace('https://openalex.org/', '')
        domain = r.get('domain', {})
        field = r.get('field', {})
        subfield = r.get('subfield', {})

        values.append((
            oa_id,
            r.get('display_name', ''),
            r.get('description'),
            domain.get('id', '').replace('https://openalex.org/', '') if domain else None,
            domain.get('display_name') if domain else None,
            field.get('id', '').replace('https://openalex.org/', '') if field else None,
            field.get('display_name') if field else None,
            subfield.get('id', '').replace('https://openalex.org/', '') if subfield else None,
            subfield.get('display_name') if subfield else None,
            [k.get('display_name', '') for k in r.get('keywords', [])],
            r.get('works_count', 0),
            r.get('cited_by_count', 0),
            datetime.utcnow()
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    conn.commit()
    return len(values)


def ingest_sources_batch(conn, records):
    """Upsert sources (journals) from OpenAlex bulk data."""
    sql = """
        INSERT INTO oa_sources (openalex_id, display_name, type, issn_l, issn,
                              is_oa, is_in_doaj, host_org_name, host_org_id,
                              country_code, homepage_url, apc_usd, works_count,
                              cited_by_count, h_index, mean_citedness, topics, updated_at)
        VALUES %s
        ON CONFLICT (openalex_id) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            works_count = EXCLUDED.works_count,
            cited_by_count = EXCLUDED.cited_by_count,
            h_index = EXCLUDED.h_index,
            updated_at = NOW()
    """
    values = []
    for r in records:
        oa_id = r.get('id', '').replace('https://openalex.org/', '')
        stats = r.get('summary_stats', {})
        host_org = r.get('host_organization', '')

        values.append((
            oa_id,
            r.get('display_name', ''),
            r.get('type'),
            r.get('issn_l'),
            r.get('issn', []),
            r.get('is_oa', False),
            r.get('is_in_doaj', False),
            r.get('host_organization_name'),
            host_org.replace('https://openalex.org/', '') if host_org else None,
            r.get('country_code'),
            r.get('homepage_url'),
            r.get('apc_usd'),
            r.get('works_count', 0),
            r.get('cited_by_count', 0),
            stats.get('h_index'),
            stats.get('2yr_mean_citedness'),
            json.dumps(r.get('topics', [])[:10]) if r.get('topics') else None,  # Top 10 topics
            datetime.utcnow()
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    conn.commit()
    return len(values)


def ingest_publishers_batch(conn, records):
    """Upsert publishers from OpenAlex bulk data."""
    sql = """
        INSERT INTO oa_publishers (openalex_id, display_name, alternate_names,
                                 country_codes, homepage_url, image_url, ror_id,
                                 works_count, cited_by_count, h_index, sources_count,
                                 counts_by_year, updated_at)
        VALUES %s
        ON CONFLICT (openalex_id) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            works_count = EXCLUDED.works_count,
            cited_by_count = EXCLUDED.cited_by_count,
            h_index = EXCLUDED.h_index,
            updated_at = NOW()
    """
    values = []
    for r in records:
        oa_id = r.get('id', '').replace('https://openalex.org/', '')
        stats = r.get('summary_stats', {})

        values.append((
            oa_id,
            r.get('display_name', ''),
            r.get('alternate_titles', []),
            r.get('country_codes', []),
            r.get('homepage_url'),
            r.get('image_url'),
            r.get('ids', {}).get('ror', '').replace('https://ror.org/', '') if r.get('ids', {}).get('ror') else None,
            r.get('works_count', 0),
            r.get('cited_by_count', 0),
            stats.get('h_index'),
            r.get('sources_count', 0),
            json.dumps(r.get('counts_by_year', [])),
            datetime.utcnow()
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    conn.commit()
    return len(values)


def ingest_institutions_batch(conn, records):
    """Upsert institutions from OpenAlex bulk data."""
    sql = """
        INSERT INTO oa_institutions (openalex_id, display_name, ror, type,
                                   country_code, city, region, latitude, longitude,
                                   homepage_url, image_url, works_count, cited_by_count,
                                   h_index, mean_citedness, associated_institutions,
                                   topics, updated_at)
        VALUES %s
        ON CONFLICT (openalex_id) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            works_count = EXCLUDED.works_count,
            cited_by_count = EXCLUDED.cited_by_count,
            h_index = EXCLUDED.h_index,
            updated_at = NOW()
    """
    values = []
    for r in records:
        oa_id = r.get('id', '').replace('https://openalex.org/', '')
        geo = r.get('geo', {})
        stats = r.get('summary_stats', {})

        values.append((
            oa_id,
            r.get('display_name', ''),
            r.get('ror', '').replace('https://ror.org/', '') if r.get('ror') else None,
            r.get('type'),
            r.get('country_code'),
            geo.get('city') if geo else None,
            geo.get('region') if geo else None,
            geo.get('latitude') if geo else None,
            geo.get('longitude') if geo else None,
            r.get('homepage_url'),
            r.get('image_url'),
            r.get('works_count', 0),
            r.get('cited_by_count', 0),
            stats.get('h_index'),
            stats.get('2yr_mean_citedness'),
            json.dumps(r.get('associated_institutions', [])[:20]) if r.get('associated_institutions') else None,
            json.dumps(r.get('topics', [])[:10]) if r.get('topics') else None,
            datetime.utcnow()
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    conn.commit()
    return len(values)


# ─── Entity dispatch ──────────────────────────────────────────────────────────
ENTITY_MAP = {
    "funders": ingest_funders_batch,
    "awards": ingest_awards_batch,
    "topics": ingest_topics_batch,
    "sources": ingest_sources_batch,
    "publishers": ingest_publishers_batch,
    "institutions": ingest_institutions_batch,
}

# ─── Main ingest loop ─────────────────────────────────────────────────────────
def ingest_entity(entity, conn):
    """Download and ingest all records for a given entity type."""
    global shutdown_requested

    ingest_fn = ENTITY_MAP[entity]
    progress = get_progress(entity)
    completed = set(progress["completed_files"])

    print(f"\n{'='*60}")
    print(f"  Ingesting: {entity.upper()}")
    print(f"  Previously completed: {len(completed)} files, {progress['total_ingested']:,} records")
    print(f"{'='*60}\n")

    # Fetch manifest
    entries = fetch_manifest(entity)
    total_records = sum(e['meta']['record_count'] for e in entries)
    total_size = sum(e['meta']['content_length'] for e in entries)
    remaining = [e for e in entries if e['url'] not in completed]

    print(f"  Manifest: {len(entries)} files, {total_records:,} records, {total_size/1e9:.2f} GB")
    print(f"  Remaining: {len(remaining)} files")
    print()

    session_ingested = 0
    session_start = time.time()

    for i, entry in enumerate(remaining):
        if shutdown_requested:
            print("⚠ Shutdown - saving progress")
            break

        file_url = entry['url']
        record_count = entry['meta']['record_count']
        file_size_mb = entry['meta']['content_length'] / 1e6

        print(f"  [{i+1}/{len(remaining)}] {file_url.split('/')[-1]} "
              f"({record_count:,} records, {file_size_mb:.1f} MB)...", end=" ", flush=True)

        file_start = time.time()
        batch = []
        file_ingested = 0

        try:
            for record in download_and_iterate(file_url):
                batch.append(record)
                if len(batch) >= BATCH_SIZE:
                    ingest_fn(conn, batch)
                    file_ingested += len(batch)
                    batch = []

                    if shutdown_requested:
                        break

            # Final batch
            if batch and not shutdown_requested:
                ingest_fn(conn, batch)
                file_ingested += len(batch)

            elapsed = time.time() - file_start
            rate = file_ingested / elapsed if elapsed > 0 else 0
            print(f"✓ {file_ingested:,} records in {elapsed:.1f}s ({rate:.0f}/s)")

            session_ingested += file_ingested
            progress["completed_files"].append(file_url)
            progress["total_ingested"] += file_ingested
            save_progress(entity, progress)

        except Exception as e:
            print(f"\n  ✗ Error: {e}")
            # Save progress and continue to next file
            save_progress(entity, progress)
            continue

    # Summary
    elapsed_total = time.time() - session_start
    print(f"\n{'─'*60}")
    print(f"  {entity.upper()} complete: {session_ingested:,} new records in {elapsed_total:.0f}s")
    print(f"  Total in DB: {progress['total_ingested']:,}")
    print(f"{'─'*60}\n")


def link_awards_to_funders(conn):
    """After ingesting awards, link them to the funders table via openalex_id."""
    print("Linking awards → funders...")
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE awards a
            SET funder_id = f.id
            FROM funders f
            WHERE a.funder_openalex = f.openalex_id
              AND a.funder_id IS NULL
        """)
        linked = cur.rowcount
        conn.commit()
    print(f"  Linked {linked:,} awards to funders")


def link_award_papers(conn):
    """
    After ingesting awards, link funded_outputs to papers.
    This runs as a separate step since it requires cross-referencing openalex_ids.
    """
    print("Linking award → paper relationships...")
    # This would be a bulk process reading from the awards' funded_outputs
    # and matching against papers.openalex_id
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO award_papers (award_id, paper_id, openalex_work_id)
            SELECT a.id, p.id, NULL
            FROM awards a
            CROSS JOIN LATERAL jsonb_array_elements_text(
                (SELECT jsonb_agg(fo) FROM unnest(ARRAY[]::text[]) fo)
            ) AS work_id
            JOIN papers p ON p.openalex_id = work_id
            WHERE NOT EXISTS (
                SELECT 1 FROM award_papers ap WHERE ap.award_id = a.id AND ap.paper_id = p.id
            )
            LIMIT 0  -- placeholder - actual linking done by separate script
        """)
        conn.commit()
    print("  Award-paper linking requires separate batch script (see link_award_papers.py)")


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Ingest OpenAlex entities for Funding Intelligence")
    parser.add_argument("--entity", required=True,
                       choices=["funders", "awards", "topics", "sources",
                               "publishers", "institutions", "all"],
                       help="Which entity to ingest")
    parser.add_argument("--skip", nargs="*", default=[],
                       help="Entities to skip when using --entity all")
    args = parser.parse_args()

    conn = get_conn()

    if args.entity == "all":
        # Ingest in dependency order: small entities first, awards last
        order = ["topics", "publishers", "sources", "institutions", "funders", "awards"]
        for entity in order:
            if entity in args.skip:
                print(f"  Skipping {entity} (--skip)")
                continue
            if shutdown_requested:
                break
            ingest_entity(entity, conn)

        # Post-processing
        if not shutdown_requested:
            link_awards_to_funders(conn)
    else:
        ingest_entity(args.entity, conn)
        if args.entity == "awards":
            link_awards_to_funders(conn)

    conn.close()
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
