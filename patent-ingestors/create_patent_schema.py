#!/usr/bin/env python3
"""
Patent Database Schema — Creates tables for patent data on Paper DB.
Run on the paper-db server (i-0cb48faa3f931c661).
"""

import psycopg2
import os

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "paper_search")
DB_USER = os.environ.get("DB_USER", "nobleblocks")
DB_PASS = os.environ.get("DB_PASS", "")

SCHEMA_SQL = """
-- Patent records
CREATE TABLE IF NOT EXISTS patents (
    id BIGSERIAL PRIMARY KEY,
    patent_id TEXT UNIQUE NOT NULL,
    title TEXT,
    abstract TEXT,
    claims_text TEXT,
    filing_date DATE,
    grant_date DATE,
    assignee TEXT,
    assignee_type TEXT,
    inventors TEXT[],
    ipc_codes TEXT[],
    cpc_codes TEXT[],
    jurisdiction TEXT,
    legal_status TEXT,
    patent_family_id TEXT,
    source TEXT,
    raw_json JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    search_vector TSVECTOR
);

-- Patent <-> Paper citation links (THE KEY TABLE)
CREATE TABLE IF NOT EXISTS patent_paper_citations (
    id BIGSERIAL PRIMARY KEY,
    patent_id TEXT NOT NULL,
    paper_id BIGINT,
    paper_doi TEXT,
    paper_openalex_id TEXT,
    paper_title TEXT,
    citation_context TEXT,
    citation_type TEXT,
    source TEXT DEFAULT 'openalex',
    first_seen_at TIMESTAMPTZ DEFAULT NOW()
);

-- Unique index using COALESCE for deduplication
CREATE UNIQUE INDEX IF NOT EXISTS idx_ppc_unique_link
    ON patent_paper_citations (patent_id, COALESCE(paper_doi, ''), COALESCE(paper_openalex_id, ''));

-- Citation velocity signals
CREATE TABLE IF NOT EXISTS patent_citation_signals (
    id BIGSERIAL PRIMARY KEY,
    paper_id BIGINT,
    paper_doi TEXT,
    paper_openalex_id TEXT,
    window_start DATE,
    window_end DATE,
    patent_citations_count INT,
    velocity_score FLOAT,
    is_spike BOOLEAN DEFAULT FALSE,
    computed_at TIMESTAMPTZ DEFAULT NOW()
);

-- Patent sequences (GenBank)
CREATE TABLE IF NOT EXISTS patent_sequences (
    id BIGSERIAL PRIMARY KEY,
    patent_id TEXT,
    sequence_id TEXT,
    sequence_type TEXT,
    organism TEXT,
    gene_name TEXT,
    sequence_length INT,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(patent_id, sequence_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_patents_search ON patents USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS idx_patents_patent_id ON patents(patent_id);
CREATE INDEX IF NOT EXISTS idx_patents_assignee ON patents(assignee);
CREATE INDEX IF NOT EXISTS idx_patents_jurisdiction ON patents(jurisdiction);
CREATE INDEX IF NOT EXISTS idx_patents_grant_date ON patents(grant_date);
CREATE INDEX IF NOT EXISTS idx_patents_filing_date ON patents(filing_date);
CREATE INDEX IF NOT EXISTS idx_patents_source ON patents(source);

CREATE INDEX IF NOT EXISTS idx_ppc_patent_id ON patent_paper_citations(patent_id);
CREATE INDEX IF NOT EXISTS idx_ppc_paper_doi ON patent_paper_citations(paper_doi);
CREATE INDEX IF NOT EXISTS idx_ppc_paper_openalex ON patent_paper_citations(paper_openalex_id);
CREATE INDEX IF NOT EXISTS idx_ppc_paper_id ON patent_paper_citations(paper_id);
CREATE INDEX IF NOT EXISTS idx_ppc_source ON patent_paper_citations(source);

CREATE INDEX IF NOT EXISTS idx_pcs_spike ON patent_citation_signals(is_spike) WHERE is_spike = TRUE;
CREATE INDEX IF NOT EXISTS idx_pcs_velocity ON patent_citation_signals(velocity_score DESC);
CREATE INDEX IF NOT EXISTS idx_pcs_paper_doi ON patent_citation_signals(paper_doi);

CREATE INDEX IF NOT EXISTS idx_pseq_patent ON patent_sequences(patent_id);
CREATE INDEX IF NOT EXISTS idx_pseq_gene ON patent_sequences(gene_name);
CREATE INDEX IF NOT EXISTS idx_pseq_organism ON patent_sequences(organism);

-- Trigger for search_vector on patents
CREATE OR REPLACE FUNCTION patents_search_vector_update() RETURNS trigger AS $$
BEGIN
    NEW.search_vector := to_tsvector('english',
        COALESCE(NEW.title, '') || ' ' ||
        COALESCE(NEW.abstract, '') || ' ' ||
        COALESCE(NEW.assignee, '') || ' ' ||
        COALESCE(array_to_string(NEW.inventors, ' '), '')
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS patents_search_vector_trigger ON patents;
CREATE TRIGGER patents_search_vector_trigger
    BEFORE INSERT OR UPDATE ON patents
    FOR EACH ROW EXECUTE FUNCTION patents_search_vector_update();

-- Summary view
CREATE OR REPLACE VIEW patent_stats AS
SELECT
    source,
    COUNT(*) as total_patents,
    COUNT(DISTINCT patent_id) as unique_patents,
    MIN(filing_date) as earliest_filing,
    MAX(filing_date) as latest_filing,
    MAX(created_at) as last_ingested
FROM patents
GROUP BY source;

CREATE OR REPLACE VIEW citation_link_stats AS
SELECT
    source,
    COUNT(*) as total_links,
    COUNT(DISTINCT patent_id) as unique_patents,
    COUNT(DISTINCT paper_doi) as unique_papers_by_doi,
    COUNT(DISTINCT paper_openalex_id) as unique_papers_by_oaid,
    MAX(first_seen_at) as last_link
FROM patent_paper_citations
GROUP BY source;
"""

def main():
    print("Connecting to Paper DB...")
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("Creating patent schema...")
    cur.execute(SCHEMA_SQL)

    # Verify
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'patent%'")
    count = cur.fetchone()[0]
    print(f"✓ Patent tables created: {count} tables")

    cur.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'patents'")
    cols = cur.fetchone()[0]
    print(f"✓ patents table has {cols} columns")

    cur.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'patent_paper_citations'")
    cols = cur.fetchone()[0]
    print(f"✓ patent_paper_citations table has {cols} columns")

    cur.close()
    conn.close()
    print("\n✅ Patent database schema ready!")

if __name__ == "__main__":
    main()
