"""
NobleBlocks MCP — Pre-built Research Prompts (Skills)
=====================================================

Each prompt is a structured multi-step workflow that guides AI assistants
through complex research tasks using NobleBlocks search tools.

These are exposed via the MCP "prompts" capability so users can one-click
activate them in Claude, ChatGPT, Cursor, etc.
"""

from __future__ import annotations

PROMPTS: dict[str, dict] = {}

# ═══════════════════════════════════════════════════════════════════════════════
# 1. LITERATURE REVIEW BUILDER
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["literature_review"] = {
    "name": "literature_review",
    "title": "Literature Review Builder",
    "description": (
        "Run a multi-step literature review: recon search, framework selection, "
        "systematic sub-area searches, and structured synthesis with citation mapping."
    ),
    "arguments": [
        {"name": "topic", "description": "Research topic or question", "required": True},
        {"name": "depth", "description": "quick (5 searches) or standard (10 searches)", "required": False},
    ],
    "template": """Help me build a comprehensive literature review on: {topic}

Use the NobleBlocks search tools. Work in this order:

1. RECON
Run one broad search to learn the terminology, major themes, and methodological distinctions. Note high-citation papers (sort by citations).

2. PLAN
Pick the best framework for this topic:
- PICO (Population, Intervention, Comparison, Outcome) — health/behavioral/social science
- SPIDER — qualitative / lived-experience questions
- Decomposition (Mechanism · Applications · Limitations · Comparisons) — technology topics

Break into 4-5 sub-areas. Show me the framework and sub-areas with rationale, then proceed with {depth} depth.

3. SEARCH
Run searches sequentially using search_papers with these strategies:
- Quick (5): one search per sub-area
- Standard (10): 5 sub-area + 2 "systematic review / meta-analysis" searches + 2 era-gated searches (one max_year: 2018, one min_year: 2022) + 1 follow-up on highest-cited paper

Track across ALL searches:
- Repeat-hit papers (3+ sub-areas → foundational)
- Recurring authors (dominant research groups)
- Citation count per year since publication (seminal work)

4. SYNTHESIZE
Output with these sections:

**Topic Overview** — One paragraph: what it is, framework used, evidence landscape shape.

**Start Here — Priority Reading Order** — 5-7 papers for a newcomer:
  1. Best recent review/meta-analysis
  2. Foundational/seminal paper(s)
  3. 2-3 papers at the current frontier
  4. One paper highlighting a key gap
For each: title, authors+year, what it contributes, what to pay attention to.

**How the Field Got Here** — Narrative + 5-8 row timeline table (Year | Milestone | Significance).

**Sub-area Guides** — Per sub-area:
  - What research shows (2-3 sentences with citations)
  - 3-5 key papers (with citation count + year + why it matters)
  - 6-10 search terms (synonyms, MeSH, historical terms)
  - 2-3 Boolean search strings

**Key Research Groups** — Top 3-5 recurring authors with affiliations.

**Open Questions & Gaps** — Three categories:
  - Methodological gaps
  - Population/context gaps
  - Conceptual/theoretical gaps

**Bibliography** — All papers cited with DOI links.

RULES:
- Only cite papers actually returned by NobleBlocks searches
- If a search returns few results, say so
- Flag papers appearing across multiple sub-area searches as must-reads""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 2. GRANT RESEARCH & POSITIONING
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["grant_research"] = {
    "name": "grant_research",
    "title": "Grant Research & Positioning",
    "description": (
        "Identify what makes a research idea novel, find matching NIH grants, "
        "similar funded projects, and produce draft Significance/Innovation language."
    ),
    "arguments": [
        {"name": "idea", "description": "Describe your research idea in a few sentences", "required": True},
        {"name": "career_stage", "description": "trainee / early-career / mid-career / senior", "required": False},
        {"name": "prelim_data", "description": "none / some / resubmission-scored / resubmission-triaged", "required": False},
    ],
    "template": """Help me find NIH grants and position my research idea: {idea}

Before diving in, confirm these details (or use what I provided):
- Career stage: {career_stage}
- Preliminary data status: {prelim_data}

═══════════════════════════════════════════════
PHASE 1 — POSITIONING ANALYSIS (5 NobleBlocks searches)
═══════════════════════════════════════════════
Goal: Produce draft Significance/Innovation language.

Run 5 searches (all with min_year set to 6 years ago):
1. **What's established** — core concept, field consensus
2. **The stakes** — prevalence, burden, outcomes, disparities
3. **Current approaches & limits** — interventions, guidelines
4. **Method in adjacent contexts** — has this approach been applied elsewhere?
5. **The gap** — search "research gaps limitations unmet needs future directions"

After all 5:
- Deduplicate papers. Multi-facet hits = high-signal.
- Extract 3-5 gap quotes ("future research should...", "remains unclear...", "no studies have...")
- Write a 2-3 paragraph positioning narrative.

═══════════════════════════════════════════════
PHASE 2 — NIH REPORTER MAPPING
═══════════════════════════════════════════════
Search NIH RePORTER API (https://api.reporter.nih.gov/v2/projects/search) for similar funded projects.
Extract: Institute targets, Study sections, NOSIs, FOAs.

═══════════════════════════════════════════════
PHASE 3 — MECHANISM MATCHING
═══════════════════════════════════════════════
Match mechanism to career stage + project scope:
| Mechanism | Budget | Best for |
|-----------|--------|----------|
| F31/F32 | Stipend | Trainee |
| R21 | ~$275K total | Exploratory |
| K01/K08/K23 | $100-250K/yr | Early-career mentored |
| R01 | ~$250K+/yr | Full research project |
| R35 | ~$750K/yr | Outstanding investigator |

═══════════════════════════════════════════════
OUTPUT
═══════════════════════════════════════════════
1. Executive Summary (3-4 bullets)
2. Positioning narrative with gap quotes
3. Where NIH is funding this work
4. Recommended funding opportunities (top 3)
5. Similar funded research (top 5 projects)
6. Study sections with fit notes
7. Strategic recommendations + program officer advice
8. Submission timeline
9. References with DOI links""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 3. CURRICULUM READING LIST
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["curriculum_reading_list"] = {
    "name": "curriculum_reading_list",
    "title": "Curriculum Reading List Builder",
    "description": (
        "Upload a course outline and get a recommended reading list of recent, "
        "peer-reviewed papers mapped to learning objectives."
    ),
    "arguments": [
        {"name": "syllabus", "description": "Course outline, topics, or syllabus text", "required": True},
    ],
    "template": """Build a recommended reading list from this syllabus/course outline:

{syllabus}

Use NobleBlocks search tools. Work in this order:

1. PARSE — Extract course topics and learning outcomes. Group related topics into 6-12 sections.

2. SEARCH — For each section, run 1-2 targeted searches with min_year set to last year for recency.
   - Build queries as "core topic + applied angle"
   - Prioritize: reviews > primary research, high citations, applied relevance
   - Pick 1-3 papers per section (15-25 total)

3. WRITE — For each paper produce:
   - **One-sentence summary** — plain language, make students want to read it
   - **Discussion question** — tied to a learning outcome, push beyond recall

4. OUTPUT:
   - Course header and intro
   - Learning outcomes (extracted or inferred)
   - Sections with numbered papers (title linked, authors, year, summary, question)
   - Search summary (queries run, results found, thin sections flagged)

RULES:
- Only cite papers returned by NobleBlocks searches
- If a section returned few results, flag it
- Base summaries on title + abstract from results""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 4. SYSTEMATIC REVIEW PROTOCOL
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["systematic_review_protocol"] = {
    "name": "systematic_review_protocol",
    "title": "Systematic Review Protocol Generator",
    "description": (
        "Generate a PRISMA-compliant systematic review protocol with search strategy, "
        "inclusion/exclusion criteria, and database search strings."
    ),
    "arguments": [
        {"name": "question", "description": "Research question for the systematic review", "required": True},
        {"name": "framework", "description": "PICO, PICOS, SPIDER, or PEO", "required": False},
    ],
    "template": """Help me develop a systematic review protocol for: {question}

Framework: {framework} (or recommend the best fit)

Use NobleBlocks search tools to:

1. DECOMPOSE the question into framework components (e.g., P-I-C-O)

2. PILOT SEARCH — Run 3-4 exploratory searches to:
   - Identify key terminology and MeSH headings
   - Estimate literature volume
   - Find existing systematic reviews on this topic
   - Identify methodological approaches used

3. BUILD SEARCH STRATEGY:
   - For each framework component, build search blocks with:
     - MeSH terms / controlled vocabulary
     - Free-text synonyms and variants
     - Truncation and wildcard suggestions
   - Combine blocks with Boolean operators
   - Generate database-specific strings for: PubMed, Scopus, Web of Science, CINAHL

4. DEFINE CRITERIA:
   - Inclusion criteria (study design, population, date range, language)
   - Exclusion criteria
   - Suggest screening tool (Covidence, Rayyan, etc.)

5. OUTPUT PROTOCOL with:
   - PROSPERO-ready registration text
   - PRISMA-P checklist alignment
   - Search strings per database
   - Estimated yield per database (from pilot)
   - Risk of bias tool recommendation
   - Data extraction form outline
   - Timeline estimate""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 5. RESEARCH GAP IDENTIFIER
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["research_gaps"] = {
    "name": "research_gaps",
    "title": "Research Gap Identifier",
    "description": (
        "Systematically identify understudied areas, methodological weaknesses, "
        "and open questions in a research field."
    ),
    "arguments": [
        {"name": "field", "description": "Research field or topic area", "required": True},
    ],
    "template": """Identify research gaps in: {field}

Use NobleBlocks search tools for a systematic gap analysis:

1. MAP THE FIELD (3 searches):
   - Search recent reviews/meta-analyses (sort by citations, min_year 2020)
   - Search "future directions" + "{field}" (look for gap quotes in abstracts)
   - Search "{field}" + "limitations" + "research agenda"

2. IDENTIFY GAP TYPES:
   - **Population gaps**: Who hasn't been studied? (age groups, ethnicities, geographies)
   - **Methodological gaps**: What designs are missing? (RCTs vs observational, longitudinal vs cross-sectional)
   - **Mechanism gaps**: What's unexplained? (pathways, mediators, moderators)
   - **Application gaps**: Where hasn't this been applied?
   - **Replication gaps**: What needs confirmation?

3. VALIDATE GAPS:
   For each claimed gap, run a targeted search to confirm it's actually under-researched
   (not just missed in the first pass).

4. OUTPUT:
   - Field overview (what's well-established vs. contested)
   - Gap map: table with Gap | Type | Evidence | Opportunity Score (1-5)
   - Top 5 gaps ranked by feasibility + impact
   - For each top gap: suggested research question, likely methodology, relevant funding calls
   - Supporting quotes from review papers (with citations)""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 6. EVIDENCE SYNTHESIS / WHAT DOES RESEARCH SAY
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["evidence_synthesis"] = {
    "name": "evidence_synthesis",
    "title": "Evidence Synthesis",
    "description": (
        "Answer a research question with cited evidence — what does the literature "
        "actually say? Handles clinical, policy, and scientific questions."
    ),
    "arguments": [
        {"name": "question", "description": "Research question to answer with evidence", "required": True},
    ],
    "template": """What does the research say about: {question}

Use NobleBlocks search tools to provide an evidence-based answer:

1. SEARCH STRATEGY (3-5 searches):
   - Main question (sort by relevance)
   - Systematic reviews on this topic (add "systematic review" or "meta-analysis")
   - Most-cited papers (sort by citations)
   - Recent evidence (min_year: 2022, sort by date)
   - Contradictory evidence (add "conflicting" or "no association" or "negative results")

2. SYNTHESIZE by evidence strength:
   - **Strong consensus** — What do meta-analyses conclude?
   - **Emerging evidence** — What do recent high-quality studies suggest?
   - **Contested/unclear** — Where do findings conflict? Why?
   - **Gaps** — What hasn't been studied?

3. OUTPUT:
   **Quick Answer** — 2-3 sentence evidence-based summary (confidence level: high/moderate/low)

   **Detailed Evidence**:
   - Consensus findings (with meta-analysis citations)
   - Effect sizes where available
   - Key moderators (it depends on X, Y, Z)
   - Limitations of current evidence
   - Clinical/practical implications

   **Evidence Table**:
   | Study | Design | N | Finding | Quality |
   
   **Bottom Line** — What can you confidently conclude vs. what remains uncertain?
   
   **References** — All papers with DOI links""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 7. PAPER DEEP DIVE
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["paper_deep_dive"] = {
    "name": "paper_deep_dive",
    "title": "Paper Deep Dive & Context",
    "description": (
        "Given a paper DOI/title, find its intellectual context: what it built on, "
        "who cited it, how it fits in the field, and what came next."
    ),
    "arguments": [
        {"name": "paper", "description": "Paper DOI, title, or ID", "required": True},
    ],
    "template": """Do a deep dive on this paper: {paper}

Use NobleBlocks tools (get_paper, get_citation_graph, find_similar, search_papers):

1. GET THE PAPER — Fetch full metadata with get_paper

2. INTELLECTUAL ANCESTRY — Use get_citation_graph (direction: references) to find:
   - What foundational work did this build on?
   - Which 3-5 papers are most essential to understanding it?

3. IMPACT & LEGACY — Use get_citation_graph (direction: citations) to find:
   - Who cited this and how? (extensions, replications, critiques)
   - Has it spawned new subfields or methods?

4. NEIGHBORHOOD — Use find_similar to discover:
   - Concurrent independent work (same period, similar ideas)
   - Alternative approaches to the same problem

5. FIELD CONTEXT — Search for the topic + "review" to place it in broader narratives

6. OUTPUT:
   **Paper Summary** — Title, authors, year, venue, citations, DOI link
   
   **One-Paragraph Context** — Where this paper fits in the field's evolution
   
   **Intellectual Lineage**:
   - Key predecessors (what it built on)
   - Key descendants (what it enabled)
   
   **Related Work Map**:
   - Complementary papers (read alongside this)
   - Competing/alternative approaches
   - Critical responses
   
   **Impact Assessment**:
   - Citations per year trend
   - Breakthrough vs incremental
   - Remaining open questions raised by this paper""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 8. METHODOLOGY SCOUT
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["methodology_scout"] = {
    "name": "methodology_scout",
    "title": "Methodology Scout",
    "description": (
        "Find papers that use a specific research method, analyze how it's applied "
        "across fields, and identify best practices and pitfalls."
    ),
    "arguments": [
        {"name": "method", "description": "Research methodology (e.g., 'grounded theory', 'difference-in-differences', 'CRISPR screening')", "required": True},
        {"name": "field", "description": "Optional: specific field to focus on", "required": False},
    ],
    "template": """Find exemplary papers using this methodology: {method}
Field focus: {field}

Use NobleBlocks search tools:

1. FIND METHODOLOGICAL PAPERS (4 searches):
   - "{method}" + "methodology" or "methods paper" (sort by citations — find the seminal methods papers)
   - "{method}" + "{field}" (applied examples in your domain)
   - "{method}" + "best practices" or "guidelines" or "reporting standards"
   - "{method}" + "limitations" or "pitfalls" or "common mistakes"

2. ANALYZE APPLICATION PATTERNS:
   - Sample sizes typically used
   - Software/tools commonly employed
   - Reporting standards (CONSORT, PRISMA, STROBE, etc.)
   - How the method evolved over time

3. OUTPUT:
   **Method Overview** — What it is, when to use it, key assumptions

   **Gold Standard Papers** — 3-5 exemplary applications (with links):
   - Why each is well-executed
   - What to emulate

   **Practical Guide**:
   - Step-by-step workflow
   - Tools and software
   - Sample size considerations
   - Common pitfalls and how to avoid them
   - Reporting checklist

   **Evolution** — How has the method changed? Recent innovations?

   **Alternative Methods** — When to use something else instead""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 9. THESIS/DISSERTATION PLANNER
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["thesis_planner"] = {
    "name": "thesis_planner",
    "title": "Thesis & Dissertation Planner",
    "description": (
        "Map the literature landscape around a thesis topic, identify theoretical "
        "frameworks, and suggest chapter structure with key references."
    ),
    "arguments": [
        {"name": "topic", "description": "Thesis/dissertation topic", "required": True},
        {"name": "level", "description": "masters or phd", "required": False},
    ],
    "template": """Help me plan my {level} thesis on: {topic}

Use NobleBlocks search tools to build the intellectual foundation:

1. LANDSCAPE MAPPING (5 searches):
   - Broad topic search (identify major themes)
   - Theoretical frameworks used in this area
   - Methodological approaches (what designs dominate?)
   - Recent developments (min_year: 2022)
   - Research gaps and future directions

2. THEORETICAL FRAMEWORK:
   - Identify 2-3 candidate frameworks from the literature
   - Which papers establish/validate each framework?
   - Recommend the best fit for your study

3. CHAPTER STRUCTURE:
   Based on the literature landscape, suggest:
   - Chapter 2 (Lit Review) sections and sub-sections
   - Key papers per section (5-8 per sub-section)
   - Theoretical vs empirical balance

4. OUTPUT:
   **Literature Landscape** — Visual map of themes, gaps, and your positioning

   **Recommended Theoretical Framework** — Which one, why, who established it

   **Suggested Chapter Outline**:
   - Ch 2: Literature Review structure
   - Key references per section
   - How many papers to cover per area

   **Positioning Statement** — 1-paragraph draft of how your study fills a gap

   **Must-Read List** — 15-20 essential papers (reading order for newcomer)

   **Methodological Precedents** — How others studied this; what worked/didn't""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 10. JOURNAL TARGETING
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["journal_targeting"] = {
    "name": "journal_targeting",
    "title": "Journal Targeting & Submission Strategy",
    "description": (
        "Analyze where similar research gets published and recommend the best "
        "journals for your manuscript, with fit analysis."
    ),
    "arguments": [
        {"name": "title", "description": "Your paper title", "required": True},
        {"name": "abstract", "description": "Your paper abstract (or brief description)", "required": True},
    ],
    "template": """Help me find the best journals for my paper:
Title: {title}
Abstract: {abstract}

Use NobleBlocks search tools:

1. FIND SIMILAR PUBLISHED WORK (3 searches):
   - Search your exact topic (note which journals appear most)
   - Use find_similar with your abstract as query
   - Search for recent reviews on this topic (review journals differ from primary research journals)

2. ANALYZE PUBLICATION PATTERNS:
   - Tally journals from results: which appears most frequently?
   - Note journal tier (by citation norms in the field)
   - Identify specialty vs generalist journals

3. OUTPUT:
   **Top 5 Journal Recommendations** — ranked by fit:
   For each:
   - Journal name, impact factor range, acceptance rate (if known)
   - Why it fits (specific similar papers published there)
   - Typical time to decision
   - Open access options/cost
   
   **Strategy**:
   - Tier 1 (reach): Aspirational target
   - Tier 2 (fit): Most likely acceptance
   - Tier 3 (backup): High-probability backup
   
   **Positioning Tips**:
   - How to frame your paper for each journal's audience
   - Keywords to use in cover letter
   - Suggested reviewers (frequent authors in those journals)""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 11. COMPETITOR/PRIOR ART SEARCH
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["prior_art_search"] = {
    "name": "prior_art_search",
    "title": "Prior Art & Competitor Research",
    "description": (
        "Exhaustive search for prior work on an idea — for patent applications, "
        "grant novelty claims, or startup due diligence."
    ),
    "arguments": [
        {"name": "idea", "description": "Your innovation, method, or research idea", "required": True},
        {"name": "context", "description": "patent / grant / startup", "required": False},
    ],
    "template": """Search for prior art and competing work on: {idea}
Context: {context}

Use NobleBlocks search tools for exhaustive coverage:

1. DIRECT SEARCH (3 variations):
   - Exact concept (narrow keywords, AND logic)
   - Broader concept (synonyms, related terms)
   - Component search (each key element separately)

2. ADJACENT SEARCH (2 searches):
   - Same method, different application
   - Same application, different method

3. TEMPORAL ANALYSIS:
   - Sort by date to find the earliest instance
   - Sort by citations to find the dominant players

4. OUTPUT:
   **Prior Art Assessment**:
   - Closest existing work (papers that most overlap with your idea)
   - How your idea differs (be specific)
   - Novelty confidence: High / Moderate / Low

   **Landscape Map**:
   | Approach | Application | Key Paper | Year | Gap vs. Your Idea |

   **Differentiation Statement** — 1 paragraph positioning your innovation

   **Risk Factors**:
   - Papers that could be cited against you
   - How to address them in your claims

   **White Space Confirmed** — What specific aspect is genuinely new?

   **References** — Complete list with DOI links""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 12. RESEARCH TREND TRACKER
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["trend_tracker"] = {
    "name": "trend_tracker",
    "title": "Research Trend & Hotspot Tracker",
    "description": (
        "Analyze publication trends, emerging subfields, and shifting research "
        "frontiers over time for a given topic."
    ),
    "arguments": [
        {"name": "topic", "description": "Research area to track trends for", "required": True},
        {"name": "years", "description": "Time window (e.g., '2015-2025')", "required": False},
    ],
    "template": """Track research trends and emerging hotspots in: {topic}
Time window: {years}

Use NobleBlocks search tools with era-gated searches:

1. HISTORICAL BASELINE:
   - Search topic with max_year: 2015, sort by citations (find the classics)
   - Note dominant themes, methods, and vocabulary of that era

2. TRANSITION PERIOD:
   - Search min_year: 2016, max_year: 2019 (what shifted?)
   - Look for new terminology, methods, or subfields emerging

3. CURRENT FRONTIER:
   - Search min_year: 2022 (what's hot now?)
   - Sort by date to find the very latest
   - Sort by citations to find recent high-impact work

4. EMERGING SIGNALS:
   - Search topic + "novel" or "emerging" or "paradigm shift" (min_year: 2023)
   - Look for new intersections (topic + adjacent fields)

5. OUTPUT:
   **Trend Summary** — 3-sentence overview of how the field has evolved

   **Timeline**:
   | Era | Dominant Theme | Key Method | Seminal Paper |
   
   **Current Hotspots** — Top 3-5 active research fronts (with evidence)
   
   **Emerging Directions** — What's gaining traction but isn't mainstream yet?
   
   **Declining Areas** — What's losing steam? (fewer recent publications)
   
   **Prediction** — Based on trajectory, what's likely next?
   
   **Bibliometric Snapshot**:
   - Total papers by year (estimate from search totals)
   - Top contributing countries/institutions
   - Key journals for this topic""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 13. CLINICAL EVIDENCE REVIEWER
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["clinical_evidence"] = {
    "name": "clinical_evidence",
    "title": "Clinical Evidence Reviewer",
    "description": (
        "Evaluate clinical evidence for a treatment, intervention, or diagnostic — "
        "structured by evidence hierarchy and patient population."
    ),
    "arguments": [
        {"name": "intervention", "description": "Treatment, drug, procedure, or diagnostic test", "required": True},
        {"name": "condition", "description": "Disease or condition", "required": True},
        {"name": "population", "description": "Patient population (optional)", "required": False},
    ],
    "template": """Evaluate clinical evidence for: {intervention} in {condition}
Population: {population}

Use NobleBlocks search tools (focus on PubMed source when available):

1. TOP OF EVIDENCE PYRAMID:
   - Search "meta-analysis" + intervention + condition (sort by citations)
   - Search "systematic review" + intervention + condition
   - Search "randomized controlled trial" + intervention + condition (min_year: 2019)

2. SAFETY & HARMS:
   - Search intervention + "adverse effects" or "safety" or "side effects"

3. GUIDELINES:
   - Search intervention + condition + "clinical guideline" or "recommendation"

4. REAL-WORLD EVIDENCE:
   - Search intervention + condition + "cohort" or "real-world" or "observational"

5. OUTPUT:
   **Clinical Bottom Line** — 2-3 sentences: Does it work? How well? For whom?
   
   **Evidence Hierarchy**:
   - Meta-analyses (N=?, pooled effect size, confidence)
   - RCTs (key trials, sample sizes, outcomes)
   - Observational studies (real-world effectiveness)
   
   **Safety Profile** — Known adverse effects, frequency, serious risks
   
   **Guideline Status** — Which guidelines recommend/discourage this?
   
   **Evidence Quality** — GRADE-style assessment (High/Moderate/Low/Very Low)
   
   **Gaps**:
   - Populations not studied
   - Outcomes not measured
   - Long-term data availability
   
   **Patient Considerations** — NNT, NNH, absolute risk differences (where available)

   **References** — All papers with DOI links""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 14. COLLABORATION FINDER
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["collaboration_finder"] = {
    "name": "collaboration_finder",
    "title": "Research Collaboration Finder",
    "description": (
        "Identify potential collaborators, research groups, and institutions "
        "working on similar or complementary research."
    ),
    "arguments": [
        {"name": "research_area", "description": "Your research area/focus", "required": True},
        {"name": "complement", "description": "What expertise or resource are you looking for?", "required": False},
    ],
    "template": """Help me find potential research collaborators for: {research_area}
Looking for: {complement}

Use NobleBlocks search tools:

1. IDENTIFY ACTIVE RESEARCHERS (3 searches):
   - Core topic (sort by citations — find the established experts)
   - Core topic (sort by date, min_year: 2023 — find actively publishing groups)
   - Complementary expertise + your area (find cross-disciplinary collaborators)

2. ANALYZE AUTHORSHIP PATTERNS:
   - Who publishes most frequently on this?
   - Who collaborates with whom? (co-authorship patterns)
   - Which institutions are hubs?

3. FIND COMPLEMENTARY EXPERTISE:
   - Search for the specific skill/method/resource you need + your domain
   - Identify groups with both technical AND domain expertise

4. OUTPUT:
   **Top Research Groups** (5-8):
   For each:
   - Lead researcher(s) name
   - Institution
   - Key publications (2-3 with links)
   - Expertise areas
   - Why they'd be a good fit

   **Complementary Expertise Map**:
   - Groups with the specific skills you're seeking
   - How their work intersects with yours
   
   **Collaboration Opportunities**:
   - Multi-PI grant potential
   - Data sharing possibilities
   - Methodological complementarity
   
   **Contact Strategy** — How to approach (cite their relevant work)""",
}

# ═══════════════════════════════════════════════════════════════════════════════
# 15. RESEARCH PROPOSAL STRENGTHENER
# ═══════════════════════════════════════════════════════════════════════════════
PROMPTS["proposal_strengthener"] = {
    "name": "proposal_strengthener",
    "title": "Research Proposal Strengthener",
    "description": (
        "Review a research proposal's claims, find supporting and contradicting "
        "evidence, and suggest improvements with citations."
    ),
    "arguments": [
        {"name": "proposal", "description": "Paste your research proposal or specific aims", "required": True},
    ],
    "template": """Strengthen my research proposal with evidence:

{proposal}

Use NobleBlocks search tools to:

1. VALIDATE CLAIMS (for each major claim in the proposal):
   - Search for supporting evidence
   - Search for contradicting evidence
   - Assess: well-supported / partially supported / needs work?

2. FIND STRONGER CITATIONS:
   - For each cited reason/claim, find higher-impact or more recent papers
   - Identify meta-analyses that could replace individual study citations

3. IDENTIFY WEAKNESSES:
   - What counterarguments exist in the literature?
   - What alternative explanations aren't addressed?
   - What similar studies failed and why?

4. FIND METHODOLOGICAL PRECEDENTS:
   - Has this approach been used before? With what results?
   - What sample sizes achieved power in similar studies?

5. OUTPUT:
   **Proposal Strength Assessment** — Overall: Strong / Moderate / Needs Work

   **Claim-by-Claim Review**:
   | Claim | Support Level | Best Citation | Suggestion |

   **Stronger Citations** — Replacements that would strengthen the proposal

   **Anticipated Reviewer Concerns** — Top 3-5 likely critiques + evidence to address them

   **Methodological Precedents** — Similar successful studies (design, N, results)

   **Gap Confirmation** — Evidence that this gap genuinely exists

   **Recommended Additions** — Specific sentences/paragraphs to add with citations""",
}


def get_all_prompts() -> list[dict]:
    """Return all prompts in MCP-compatible format."""
    return list(PROMPTS.values())


def get_prompt(name: str) -> dict | None:
    """Get a single prompt by name."""
    return PROMPTS.get(name)
