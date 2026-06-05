#!/usr/bin/env python3
"""
Export Author Emails
====================
Exports author contact information from the author_email_lookup table
for researcher outreach, newsletter invitations, and platform notifications.

Usage:
    python3 export_author_emails.py [--format csv|jsonl] [--output FILE]
    python3 export_author_emails.py --field bio   # Only life sciences
    python3 export_emails.py --min-papers 3       # Prolific authors only
    python3 export_emails.py --since 2024-01-01   # Recent papers only

Outputs columns: email, author_name, orcid, institution, department, paper_count, recent_doi

Requires:
    - psycopg2 (pip install psycopg2-binary)
    - Access to paper_search database
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

# Database configuration — same as other ingestors
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "nb_papers_2026_prod")

OUTPUT_DIR = Path("/tmp/exports")


def get_connection():
    import psycopg2
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def export_emails(args):
    """Export deduplicated author emails with metadata."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    output_file = args.output or str(
        OUTPUT_DIR / f"author_emails_{time.strftime('%Y-%m-%d')}.{args.format}"
    )
    
    # Build query with optional filters
    conditions = ["e.email IS NOT NULL", "e.email != ''"]
    params = []
    
    if args.since:
        conditions.append("e.created_at >= %s")
        params.append(args.since)
    
    if args.field:
        # Filter by paper subject area (journal-based heuristic)
        field_journals = {
            "bio": ("nature", "cell", "plos", "bmc", "lancet", "nejm", "jama",
                    "biology", "biochem", "genomic", "neuro", "immun", "oncol",
                    "pharma", "medic", "clinic", "pathol", "physiol"),
            "cs": ("ieee", "acm", "comput", "arxiv", "neural", "machine",
                   "algorithm", "software", "artificial", "data"),
            "chem": ("chem", "acs", "rsc", "molecular", "polymer", "catalys",
                     "organic", "inorganic", "electro"),
        }
        if args.field in field_journals:
            journal_pats = field_journals[args.field]
            or_clauses = " OR ".join(
                f"LOWER(p.journal) LIKE %s" for _ in journal_pats
            )
            conditions.append(f"({or_clauses})")
            params.extend(f"%{pat}%" for pat in journal_pats)
    
    where_clause = " AND ".join(conditions)
    
    # Deduplicate by email, aggregate paper counts
    query = f"""
        SELECT
            e.email,
            e.author_name,
            e.orcid,
            e.institution,
            e.department,
            COUNT(DISTINCT e.paper_id) AS paper_count,
            MAX(e.doi) AS recent_doi
        FROM author_email_lookup e
        {"JOIN papers p ON p.id = e.paper_id" if args.field else ""}
        WHERE {where_clause}
        GROUP BY e.email, e.author_name, e.orcid, e.institution, e.department
        {"HAVING COUNT(DISTINCT e.paper_id) >= %s" if args.min_papers > 1 else ""}
        ORDER BY paper_count DESC, e.email
    """
    
    if args.min_papers > 1:
        params.append(args.min_papers)
    
    log.info(f"Querying author_email_lookup (filters: since={args.since}, field={args.field}, min_papers={args.min_papers})")
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query, params)
    
    columns = ["email", "author_name", "orcid", "institution", "department", "paper_count", "recent_doi"]
    
    row_count = 0
    
    if args.format == "csv":
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for row in cursor:
                writer.writerow(row)
                row_count += 1
    else:  # jsonl
        with open(output_file, 'w', encoding='utf-8') as f:
            for row in cursor:
                record = dict(zip(columns, row))
                record["paper_count"] = int(record["paper_count"])
                f.write(json.dumps(record, default=str) + '\n')
                row_count += 1
    
    cursor.close()
    conn.close()
    
    file_size = Path(output_file).stat().st_size / 1024
    log.info(f"Exported {row_count:,} unique author emails to {output_file} ({file_size:.1f} KB)")
    
    return output_file


def main():
    parser = argparse.ArgumentParser(description="Export author emails for outreach")
    parser.add_argument("--format", choices=["csv", "jsonl"], default="csv",
                        help="Output format (default: csv)")
    parser.add_argument("--output", "-o", help="Output file path (default: /tmp/exports/author_emails_DATE.FORMAT)")
    parser.add_argument("--since", help="Only include papers added since this date (YYYY-MM-DD)")
    parser.add_argument("--field", choices=["bio", "cs", "chem"],
                        help="Filter by research field (based on journal name heuristics)")
    parser.add_argument("--min-papers", type=int, default=1,
                        help="Minimum number of papers per author (default: 1)")
    
    args = parser.parse_args()
    export_emails(args)


if __name__ == "__main__":
    main()
