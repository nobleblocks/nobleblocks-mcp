-- Funding Intelligence Schema
-- Extends existing funders/funding_edges/paper_grants tables
-- Adds: awards, topics, sources, institutions, publishers
--
-- MULTI-SOURCE STRATEGY: OpenAlex + NIH Reporter + NSF + Europe PMC + CrossRef + UKRI
-- openalex_id column is reused as source_id (prefixed: oa:, nih:, nsf:, epmc:, ukri:, crossref:)
-- This avoids single-vendor lock-in if OpenAlex restricts access

-- ============================================================
-- AWARDS (OpenAlex Grants/Awards - 13.8M records)
-- The crown jewel: actual grant awards with amounts, dates, PIs
-- ============================================================
CREATE TABLE IF NOT EXISTS awards (
    id              SERIAL PRIMARY KEY,
    openalex_id     TEXT NOT NULL UNIQUE,           -- e.g. 'G6860833106'
    display_name    TEXT,                            -- Grant title
    description     TEXT,                            -- Grant abstract/description
    funder_award_id TEXT,                            -- External grant number (e.g. 'R01-CA12345')
    funder_id       INTEGER REFERENCES funders(id),  -- FK to our funders table
    funder_openalex TEXT,                            -- OpenAlex funder ID for linking
    amount          NUMERIC(15,2),                   -- Funding amount
    currency        TEXT,                            -- Currency code (USD, EUR, JPY, etc.)
    funding_type    TEXT,                            -- 'research', 'fellowship', etc.
    funder_scheme   TEXT,                            -- e.g. 'Grant-in-Aid for Scientific Research (C)'
    start_date      DATE,
    end_date        DATE,
    start_year      SMALLINT,
    end_year        SMALLINT,
    landing_page_url TEXT,
    doi             TEXT,
    provenance      TEXT,                            -- 'crossref', 'kaken', 'nsf', etc.
    lead_investigator JSONB,                         -- {given_name, family_name, orcid, affiliation}
    co_lead_investigator JSONB,
    investigators   JSONB,                           -- Array of investigators
    primary_topic   JSONB,                           -- Topic classification
    topics          JSONB,                           -- Array of topics
    institution_awarded JSONB,                       -- Institutions that received the award
    funded_outputs_count INTEGER DEFAULT 0,          -- Number of linked publications
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_awards_funder ON awards(funder_id);
CREATE INDEX IF NOT EXISTS idx_awards_funder_openalex ON awards(funder_openalex);
CREATE INDEX IF NOT EXISTS idx_awards_start_year ON awards(start_year);
CREATE INDEX IF NOT EXISTS idx_awards_amount ON awards(amount) WHERE amount IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_awards_provenance ON awards(provenance);
CREATE INDEX IF NOT EXISTS idx_awards_funding_type ON awards(funding_type) WHERE funding_type IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_awards_doi ON awards(doi) WHERE doi IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_awards_funder_award_id ON awards(funder_award_id) WHERE funder_award_id IS NOT NULL;

-- Full-text search on award titles and descriptions
CREATE INDEX IF NOT EXISTS idx_awards_title_trgm ON awards USING gin (display_name gin_trgm_ops);

-- ============================================================
-- CROSS-SOURCE DEDUPLICATION TABLE
-- Tracks which awards appear in multiple sources
-- Allows merge/enrichment without duplicating records
-- ============================================================
CREATE TABLE IF NOT EXISTS award_source_xref (
    award_id        INTEGER NOT NULL REFERENCES awards(id),
    source          TEXT NOT NULL,          -- 'openalex', 'nih_reporter', 'nsf', 'europe_pmc', 'crossref', 'ukri'
    source_id       TEXT NOT NULL,          -- Source-specific ID
    confidence      NUMERIC(3,2) DEFAULT 1.0,  -- Match confidence (1.0 = exact, <1.0 = fuzzy)
    matched_on      TEXT,                   -- What field matched: 'grant_number', 'doi', 'pi_title'
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (award_id, source)
);

CREATE INDEX IF NOT EXISTS idx_award_xref_source ON award_source_xref(source, source_id);

-- ============================================================
-- AWARD-PAPER LINKAGE (funded_outputs)
-- Links awards to the papers they produced
-- ============================================================
CREATE TABLE IF NOT EXISTS award_papers (
    award_id        INTEGER NOT NULL REFERENCES awards(id),
    paper_id        BIGINT NOT NULL,   -- FK to papers(id) - not enforced for perf
    openalex_work_id TEXT,              -- OpenAlex work ID for unresolved refs
    PRIMARY KEY (award_id, paper_id)
);

CREATE INDEX IF NOT EXISTS idx_award_papers_paper ON award_papers(paper_id);

-- ============================================================
-- ENHANCE FUNDERS TABLE (add missing fields from OpenAlex)
-- ============================================================
ALTER TABLE funders ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE funders ADD COLUMN IF NOT EXISTS image_url TEXT;
ALTER TABLE funders ADD COLUMN IF NOT EXISTS doi TEXT;
ALTER TABLE funders ADD COLUMN IF NOT EXISTS wikidata_id TEXT;
ALTER TABLE funders ADD COLUMN IF NOT EXISTS crossref_id TEXT;
ALTER TABLE funders ADD COLUMN IF NOT EXISTS h_index INTEGER;
ALTER TABLE funders ADD COLUMN IF NOT EXISTS i10_index INTEGER;
ALTER TABLE funders ADD COLUMN IF NOT EXISTS mean_citedness NUMERIC(8,4);
ALTER TABLE funders ADD COLUMN IF NOT EXISTS awards_count INTEGER DEFAULT 0;
ALTER TABLE funders ADD COLUMN IF NOT EXISTS counts_by_year JSONB;
ALTER TABLE funders ADD COLUMN IF NOT EXISTS roles JSONB;

-- ============================================================
-- TOPICS (OpenAlex topic taxonomy - 4,516 records)
-- Hierarchical: Domain > Field > Subfield > Topic
-- ============================================================
CREATE TABLE IF NOT EXISTS oa_topics (
    id              SERIAL PRIMARY KEY,
    openalex_id     TEXT NOT NULL UNIQUE,
    display_name    TEXT NOT NULL,
    description     TEXT,
    domain_id       TEXT,
    domain_name     TEXT,
    field_id        TEXT,
    field_name      TEXT,
    subfield_id     TEXT,
    subfield_name   TEXT,
    keywords        TEXT[],
    works_count     INTEGER DEFAULT 0,
    cited_by_count  BIGINT DEFAULT 0,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_oa_topics_domain ON oa_topics(domain_id);
CREATE INDEX IF NOT EXISTS idx_oa_topics_field ON oa_topics(field_id);
CREATE INDEX IF NOT EXISTS idx_oa_topics_subfield ON oa_topics(subfield_id);

-- ============================================================
-- SOURCES (Journals/Repositories - 280K records)
-- ============================================================
CREATE TABLE IF NOT EXISTS oa_sources (
    id              SERIAL PRIMARY KEY,
    openalex_id     TEXT NOT NULL UNIQUE,
    display_name    TEXT NOT NULL,
    type            TEXT,                -- 'journal', 'repository', 'conference', etc.
    issn_l          TEXT,
    issn            TEXT[],
    is_oa           BOOLEAN DEFAULT FALSE,
    is_in_doaj      BOOLEAN DEFAULT FALSE,
    host_org_name   TEXT,
    host_org_id     TEXT,
    country_code    TEXT,
    homepage_url    TEXT,
    apc_usd         INTEGER,             -- Article processing charge
    works_count     INTEGER DEFAULT 0,
    cited_by_count  BIGINT DEFAULT 0,
    h_index         INTEGER,
    mean_citedness  NUMERIC(10,4),
    topics          JSONB,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_oa_sources_issn ON oa_sources(issn_l);
CREATE INDEX IF NOT EXISTS idx_oa_sources_type ON oa_sources(type);
CREATE INDEX IF NOT EXISTS idx_oa_sources_host ON oa_sources(host_org_id);

-- ============================================================
-- PUBLISHERS (10.7K records)
-- ============================================================
CREATE TABLE IF NOT EXISTS oa_publishers (
    id              SERIAL PRIMARY KEY,
    openalex_id     TEXT NOT NULL UNIQUE,
    display_name    TEXT NOT NULL,
    alternate_names TEXT[],
    country_codes   TEXT[],
    homepage_url    TEXT,
    image_url       TEXT,
    ror_id          TEXT,
    works_count     INTEGER DEFAULT 0,
    cited_by_count  BIGINT DEFAULT 0,
    h_index         INTEGER,
    sources_count   INTEGER DEFAULT 0,
    counts_by_year  JSONB,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- INSTITUTIONS (enhanced from ror_institutions - 121K records)
-- ============================================================
CREATE TABLE IF NOT EXISTS oa_institutions (
    id              SERIAL PRIMARY KEY,
    openalex_id     TEXT NOT NULL UNIQUE,
    display_name    TEXT NOT NULL,
    ror             TEXT,
    type            TEXT,               -- 'education', 'healthcare', 'government', etc.
    country_code    TEXT,
    city            TEXT,
    region          TEXT,
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6),
    homepage_url    TEXT,
    image_url       TEXT,
    works_count     INTEGER DEFAULT 0,
    cited_by_count  BIGINT DEFAULT 0,
    h_index         INTEGER,
    mean_citedness  NUMERIC(10,4),
    associated_institutions JSONB,
    topics          JSONB,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_oa_institutions_ror ON oa_institutions(ror);
CREATE INDEX IF NOT EXISTS idx_oa_institutions_country ON oa_institutions(country_code);
CREATE INDEX IF NOT EXISTS idx_oa_institutions_type ON oa_institutions(type);

-- ============================================================
-- FUNDING ANALYTICS MATERIALIZED VIEWS
-- Pre-computed aggregations for fast funding intelligence queries
-- ============================================================

-- Top funders by total award amount (by year)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_funder_totals AS
SELECT
    f.id AS funder_id,
    f.name AS funder_name,
    f.country_code,
    a.start_year,
    COUNT(*) AS awards_count,
    SUM(CASE WHEN a.currency = 'USD' THEN a.amount
             WHEN a.currency = 'EUR' THEN a.amount * 1.08
             WHEN a.currency = 'GBP' THEN a.amount * 1.27
             WHEN a.currency = 'JPY' THEN a.amount * 0.0067
             ELSE NULL END) AS total_usd,
    AVG(CASE WHEN a.currency = 'USD' THEN a.amount
             WHEN a.currency = 'EUR' THEN a.amount * 1.08
             WHEN a.currency = 'GBP' THEN a.amount * 1.27
             WHEN a.currency = 'JPY' THEN a.amount * 0.0067
             ELSE NULL END) AS avg_award_usd,
    SUM(a.funded_outputs_count) AS total_publications
FROM funders f
JOIN awards a ON a.funder_id = f.id
WHERE a.start_year IS NOT NULL
GROUP BY f.id, f.name, f.country_code, a.start_year;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_funder_totals ON mv_funder_totals(funder_id, start_year);

-- Funding by topic (which topics get the most money)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_funding_by_topic AS
SELECT
    t.openalex_id AS topic_id,
    t.display_name AS topic_name,
    t.domain_name,
    t.field_name,
    t.subfield_name,
    COUNT(DISTINCT a.id) AS awards_count,
    SUM(CASE WHEN a.currency = 'USD' THEN a.amount
             WHEN a.currency = 'EUR' THEN a.amount * 1.08
             WHEN a.currency = 'GBP' THEN a.amount * 1.27
             WHEN a.currency = 'JPY' THEN a.amount * 0.0067
             ELSE NULL END) AS total_usd,
    COUNT(DISTINCT a.funder_id) AS funders_count
FROM awards a
CROSS JOIN LATERAL jsonb_array_elements(a.topics) AS topic_elem
JOIN oa_topics t ON t.openalex_id = topic_elem->>'id'
WHERE a.topics IS NOT NULL
GROUP BY t.openalex_id, t.display_name, t.domain_name, t.field_name, t.subfield_name;

-- Funder-Topic matrix (who funds what)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_funder_topics AS
SELECT
    f.id AS funder_id,
    f.name AS funder_name,
    topic_elem->>'id' AS topic_id,
    topic_elem->>'display_name' AS topic_name,
    COUNT(*) AS awards_count,
    SUM(a.funded_outputs_count) AS publications_count
FROM funders f
JOIN awards a ON a.funder_id = f.id
CROSS JOIN LATERAL jsonb_array_elements(a.topics) AS topic_elem
WHERE a.topics IS NOT NULL
GROUP BY f.id, f.name, topic_elem->>'id', topic_elem->>'display_name';

-- ============================================================
-- DATA SOURCE COVERAGE VIEW
-- Shows what each source contributes
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_source_coverage AS
SELECT
    provenance,
    COUNT(*) AS total_awards,
    COUNT(*) FILTER (WHERE amount IS NOT NULL) AS awards_with_amount,
    ROUND(100.0 * COUNT(*) FILTER (WHERE amount IS NOT NULL) / NULLIF(COUNT(*), 0), 1) AS pct_with_amount,
    COUNT(*) FILTER (WHERE description IS NOT NULL) AS awards_with_abstract,
    COUNT(*) FILTER (WHERE lead_investigator IS NOT NULL) AS awards_with_pi,
    MIN(start_year) AS earliest_year,
    MAX(start_year) AS latest_year,
    SUM(CASE WHEN currency = 'USD' THEN amount
             WHEN currency = 'EUR' THEN amount * 1.08
             WHEN currency = 'GBP' THEN amount * 1.27
             WHEN currency = 'JPY' THEN amount * 0.0067
             ELSE NULL END) AS total_usd
FROM awards
GROUP BY provenance
ORDER BY total_awards DESC;
