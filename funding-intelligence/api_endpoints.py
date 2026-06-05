#!/usr/bin/env python3
"""
Funding Intelligence API Endpoints

Add these to search_api.py on the paper-db server.
Provides the backend for the Funding Intelligence dashboard.
"""

# ────────────────────────────────────────────────────────────────────────────────
# ENDPOINTS TO ADD TO search_api.py
# ────────────────────────────────────────────────────────────────────────────────

"""
1. GET /api/v1/funding/search
   - Search awards by topic, funder, or free text
   - Params: q (text), funder_id, topic, country, year_from, year_to, min_amount, max_amount
   - Returns: paginated list of awards with funder info

2. GET /api/v1/funding/funders
   - List/search funders with stats
   - Params: q, country, sort_by (works_count|awards_count|h_index)
   - Returns: paginated funders with summary stats

3. GET /api/v1/funding/funders/{id}
   - Single funder detail with all metadata
   - Returns: funder + top topics + recent awards + top funded papers

4. GET /api/v1/funding/awards/{id}
   - Single award detail
   - Returns: award + linked papers + funder info

5. GET /api/v1/funding/analytics/by-topic
   - Funding amounts grouped by topic/field/domain
   - Params: level (domain|field|subfield|topic), year, country
   - Returns: [{topic, total_usd, awards_count, funders_count}]

6. GET /api/v1/funding/analytics/by-funder
   - Top funders ranked by amount/publications
   - Params: topic, country, year, sort_by, limit
   - Returns: [{funder, total_usd, awards_count, publications}]

7. GET /api/v1/funding/analytics/trends
   - Funding trends over time
   - Params: funder_id|topic_id, group_by (year|quarter)
   - Returns: [{period, total_usd, awards_count, new_publications}]

8. GET /api/v1/funding/paper/{paper_id}/grants
   - Get all grants/awards that funded a specific paper
   - Returns: [{award, funder, amount, dates}]

9. GET /api/v1/funding/suggest
   - AI-powered: suggest funders for a research topic
   - Params: query (research description), topic_ids[]
   - Uses embeddings to find semantically similar funded research
   - Returns: [{funder, relevance_score, example_awards, avg_amount}]
"""

# ────────────────────────────────────────────────────────────────────────────────
# IMPLEMENTATION (FastAPI-style, to add to search_api.py)
# ────────────────────────────────────────────────────────────────────────────────

FUNDING_SEARCH_SQL = """
SELECT a.id, a.openalex_id, a.display_name, a.description, a.amount, a.currency,
       a.funding_type, a.funder_scheme, a.start_year, a.end_year, a.provenance,
       a.lead_investigator, a.funded_outputs_count,
       f.name as funder_name, f.country_code as funder_country, f.openalex_id as funder_openalex_id
FROM awards a
LEFT JOIN funders f ON a.funder_id = f.id
WHERE 1=1
  {filters}
ORDER BY {sort} DESC
LIMIT %(limit)s OFFSET %(offset)s
"""

FUNDING_BY_TOPIC_SQL = """
SELECT topic_name, domain_name, field_name, subfield_name,
       awards_count, total_usd, funders_count
FROM mv_funding_by_topic
WHERE 1=1
  {filters}
ORDER BY total_usd DESC NULLS LAST
LIMIT %(limit)s
"""

FUNDER_RANKINGS_SQL = """
SELECT f.id, f.openalex_id, f.name, f.country_code, f.homepage_url,
       f.works_count, f.awards_count, f.h_index, f.mean_citedness,
       COALESCE(SUM(CASE WHEN a.currency = 'USD' THEN a.amount
                        WHEN a.currency = 'EUR' THEN a.amount * 1.08
                        WHEN a.currency = 'GBP' THEN a.amount * 1.27
                        WHEN a.currency = 'JPY' THEN a.amount * 0.0067
                        ELSE NULL END), 0) as total_funded_usd
FROM funders f
LEFT JOIN awards a ON a.funder_id = f.id
WHERE 1=1
  {filters}
GROUP BY f.id
ORDER BY {sort} DESC
LIMIT %(limit)s OFFSET %(offset)s
"""

PAPER_GRANTS_SQL = """
SELECT a.id, a.openalex_id, a.display_name as award_title, a.amount, a.currency,
       a.start_year, a.end_year, a.funder_scheme, a.provenance,
       f.name as funder_name, f.country_code, f.openalex_id as funder_openalex_id
FROM award_papers ap
JOIN awards a ON ap.award_id = a.id
LEFT JOIN funders f ON a.funder_id = f.id
WHERE ap.paper_id = %(paper_id)s
ORDER BY a.start_year DESC
"""

# ────────────────────────────────────────────────────────────────────────────────
# FRONTEND ROUTES (Next.js pages)
# ────────────────────────────────────────────────────────────────────────────────
"""
/funding                    → Funding Intelligence dashboard (main)
/funding/search             → Search grants/awards
/funding/funders            → Browse funders by country/topic
/funding/funders/[id]       → Funder detail page
/funding/awards/[id]        → Award detail page
/funding/analytics          → Visual analytics (charts, trends)
/funding/suggest            → AI funding suggestion tool

Dashboard components:
- FundingSearch:        Full-text search across awards
- TopFundersTable:      Ranked table of funders by total $, publications, h-index
- FundingByTopic:       Treemap/bar chart of $ by research field
- FundingTrends:        Line chart of funding over time
- GrantOutcomes:        For a specific grant → publications, citations, impact
- FunderProfile:        Full funder page (like a researcher page but for funders)
- SuggestFunding:       "Find funding for my research" AI tool
"""
