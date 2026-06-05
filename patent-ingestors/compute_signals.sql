-- Compute patent citation signals for all resolved papers
-- Schema: patent_citation_signals(paper_id, paper_doi, paper_openalex_id, window_start, window_end, patent_citations_count, velocity_score, is_spike)

-- Delete previous all-time window signals to avoid duplicates
DELETE FROM patent_citation_signals WHERE window_start = '1900-01-01' AND window_end = CURRENT_DATE;

-- Insert all-time citation counts per paper
INSERT INTO patent_citation_signals (paper_id, paper_doi, window_start, window_end, patent_citations_count, velocity_score, is_spike)
SELECT
    ppc.paper_id,
    ppc.paper_doi,
    '1900-01-01'::date AS window_start,
    CURRENT_DATE AS window_end,
    COUNT(DISTINCT ppc.patent_id) AS patent_citations_count,
    -- Velocity = citations in last 2 years (PatentsView data goes to ~2024)
    COUNT(DISTINCT ppc.patent_id) FILTER (WHERE p.filing_date > CURRENT_DATE - interval '2 years') AS velocity_score,
    -- Spike = paper cited by 5+ patents (top 1% is roughly this threshold)
    (COUNT(DISTINCT ppc.patent_id) >= 5) AS is_spike
FROM patent_paper_citations ppc
LEFT JOIN patents p ON p.patent_id = ppc.patent_id
WHERE ppc.paper_id IS NOT NULL
GROUP BY ppc.paper_id, ppc.paper_doi;
