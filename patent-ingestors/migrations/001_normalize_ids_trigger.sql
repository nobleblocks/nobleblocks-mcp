-- =====================================================================
-- ID Normalization Triggers for papers & patent_paper_citations tables
-- 
-- PURPOSE: Enforce canonical ID formats at the database level regardless
-- of which application/ingestor writes the data. This prevents the
-- "US-12224364-B2" vs "US-12224364" class of bugs permanently.
--
-- DEPLOY: Run on paper_search DB via SSM or psql.
-- IDEMPOTENT: Safe to re-run (CREATE OR REPLACE).
-- =====================================================================

-- ─── Function: Normalize paper IDs on INSERT/UPDATE ───

CREATE OR REPLACE FUNCTION normalize_paper_ids_fn()
RETURNS TRIGGER AS $$
BEGIN
    -- DOI: lowercase, strip URL prefix, strip trailing punctuation
    IF NEW.doi IS NOT NULL THEN
        -- Strip https://doi.org/ or http://dx.doi.org/ prefix
        NEW.doi := regexp_replace(NEW.doi, '^https?://(dx\.)?doi\.org/', '', 'i');
        -- Strip trailing punctuation
        NEW.doi := regexp_replace(NEW.doi, '[.,;:)\]}>]+$', '');
        -- Lowercase (DOIs are case-insensitive per CrossRef spec)
        NEW.doi := lower(trim(NEW.doi));
        -- Null out invalid DOIs
        IF NEW.doi !~ '^10\.' OR position('/' in NEW.doi) = 0 OR length(NEW.doi) < 8 THEN
            NEW.doi := NULL;
        END IF;
    END IF;

    -- PMID: bare numeric, strip prefix
    IF NEW.pmid IS NOT NULL THEN
        NEW.pmid := regexp_replace(trim(NEW.pmid), '^(pmid|pubmed)[:\s]*', '', 'i');
        NEW.pmid := trim(NEW.pmid);
        IF NEW.pmid !~ '^\d{1,9}$' THEN
            NEW.pmid := NULL;
        END IF;
    END IF;

    -- arXiv ID: strip prefix, keep version
    IF NEW.arxiv_id IS NOT NULL THEN
        NEW.arxiv_id := regexp_replace(trim(NEW.arxiv_id), '^(arxiv[:\s]*|https?://arxiv\.org/(abs|pdf)/)', '', 'i');
        NEW.arxiv_id := regexp_replace(NEW.arxiv_id, '\.pdf$', '', 'i');
        NEW.arxiv_id := trim(NEW.arxiv_id);
        -- Validate format (new: YYMM.NNNNN or old: archive/NNNNNNN)
        IF NEW.arxiv_id !~ '^\d{4}\.\d{4,5}(v\d+)?$' AND 
           NEW.arxiv_id !~ '^[a-z-]+/\d{7}(v\d+)?$' THEN
            NEW.arxiv_id := NULL;
        END IF;
    END IF;

    -- OpenAlex ID: strip URL prefix, keep W+digits
    IF NEW.openalex_id IS NOT NULL THEN
        NEW.openalex_id := regexp_replace(trim(NEW.openalex_id), '^https?://openalex\.org/', '', 'i');
        NEW.openalex_id := trim(NEW.openalex_id);
        IF NEW.openalex_id !~ '^W\d+$' THEN
            NEW.openalex_id := NULL;
        END IF;
    END IF;

    -- S2 ID: strip prefix
    IF NEW.s2_id IS NOT NULL THEN
        NEW.s2_id := regexp_replace(trim(NEW.s2_id), '^(corpusid|s2)[:\s]*', '', 'i');
        NEW.s2_id := trim(NEW.s2_id);
        IF NEW.s2_id !~ '^[0-9a-f]{40}$' AND NEW.s2_id !~ '^\d+$' THEN
            NEW.s2_id := NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ─── Function: Normalize DOI in citation tables ───

CREATE OR REPLACE FUNCTION normalize_citation_doi_fn()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.paper_doi IS NOT NULL THEN
        NEW.paper_doi := regexp_replace(NEW.paper_doi, '^https?://(dx\.)?doi\.org/', '', 'i');
        NEW.paper_doi := regexp_replace(NEW.paper_doi, '[.,;:)\]}>]+$', '');
        NEW.paper_doi := lower(trim(NEW.paper_doi));
        IF NEW.paper_doi !~ '^10\.' OR position('/' in NEW.paper_doi) = 0 OR length(NEW.paper_doi) < 8 THEN
            NEW.paper_doi := NULL;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ─── Function: Normalize patent_id (strip kind code) ───

CREATE OR REPLACE FUNCTION normalize_patent_id_fn()
RETURNS TRIGGER AS $$
DECLARE
    parts TEXT[];
BEGIN
    IF NEW.patent_id IS NOT NULL THEN
        NEW.patent_id := upper(trim(NEW.patent_id));
        -- Strip kind code: "US-12224364-B2" → "US-12224364"
        parts := string_to_array(NEW.patent_id, '-');
        IF array_length(parts, 1) >= 3 THEN
            NEW.patent_id := parts[1] || '-' || parts[2];
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ─── Apply triggers ───

-- Papers table (main 332M records)
DROP TRIGGER IF EXISTS trg_normalize_paper_ids ON papers;
CREATE TRIGGER trg_normalize_paper_ids
    BEFORE INSERT OR UPDATE ON papers
    FOR EACH ROW
    EXECUTE FUNCTION normalize_paper_ids_fn();

-- Patent-paper citation links
DROP TRIGGER IF EXISTS trg_normalize_citation_doi ON patent_paper_citations;
CREATE TRIGGER trg_normalize_citation_doi
    BEFORE INSERT OR UPDATE ON patent_paper_citations
    FOR EACH ROW
    EXECUTE FUNCTION normalize_citation_doi_fn();

-- Patent table
DROP TRIGGER IF EXISTS trg_normalize_patent_id ON patents;
CREATE TRIGGER trg_normalize_patent_id
    BEFORE INSERT OR UPDATE ON patents
    FOR EACH ROW
    EXECUTE FUNCTION normalize_patent_id_fn();

-- ─── Verify ───
-- After deploying, run these to confirm triggers are active:
-- SELECT tgname, tgrelid::regclass FROM pg_trigger WHERE tgname LIKE 'trg_normalize%';

-- ─── One-time cleanup of existing data ───
-- Run AFTER triggers are deployed (triggers will normalize during UPDATE):
--
-- UPDATE papers SET doi = doi WHERE doi IS NOT NULL AND doi ~ '^https?://';
-- UPDATE papers SET doi = doi WHERE doi IS NOT NULL AND doi != lower(doi);
-- UPDATE papers SET pmid = pmid WHERE pmid IS NOT NULL AND pmid ~ '^(pmid|pubmed)';
-- UPDATE papers SET arxiv_id = arxiv_id WHERE arxiv_id IS NOT NULL AND arxiv_id ~ '^arxiv';
-- UPDATE papers SET openalex_id = openalex_id WHERE openalex_id IS NOT NULL AND openalex_id ~ '^https://';
-- UPDATE patent_paper_citations SET paper_doi = paper_doi WHERE paper_doi IS NOT NULL AND paper_doi != lower(paper_doi);
