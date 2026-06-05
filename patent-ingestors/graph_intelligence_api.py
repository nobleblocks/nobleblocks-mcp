#!/usr/bin/env python3
"""
NobleBlocks Graph Intelligence API

The layer that turns a data lake into a sellable product.
Exposes graph traversal, prior-art discovery, velocity signals,
and multi-hop queries over 333M papers + patents + KG entities.

Runs on the paper-db server as a FastAPI service (port 8100).
"""

import os
import logging
from datetime import date, datetime
from typing import Optional
from contextlib import asynccontextmanager

import psycopg2
import psycopg2.pool
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# === Configuration ===
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "paper_search")
DB_USER = os.getenv("DB_USER", "nobleblocks")
DB_PASS = os.getenv("DB_PASS", "nb_papers_2026_prod")
API_PORT = int(os.getenv("API_PORT", "8100"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Connection pool
pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=2, maxconn=20,
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        options="-c statement_timeout=60000"  # 60s max per query
    )
    log.info(f"Graph Intelligence API started — pool connected to {DB_HOST}:{DB_PORT}/{DB_NAME}")
    yield
    pool.closeall()


app = FastAPI(
    title="NobleBlocks Graph Intelligence API",
    version="1.0.0",
    description="Graph traversal, prior-art discovery, and velocity intelligence over 333M papers + 9.4M patents",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def get_conn():
    return pool.getconn()


def put_conn(conn):
    pool.putconn(conn)


# ============================================================
# RESPONSE MODELS
# ============================================================

class PaperBrief(BaseModel):
    doi: Optional[str] = None
    title: Optional[str] = None
    year: Optional[int] = None
    citation_count: Optional[int] = None
    patent_citations: Optional[int] = None


class PatentBrief(BaseModel):
    patent_id: str
    title: Optional[str] = None
    assignee: Optional[str] = None
    grant_date: Optional[str] = None
    jurisdiction: Optional[str] = None


class GeneTarget(BaseModel):
    gene_id: str
    gene_name: Optional[str] = None
    paper_count: int = 0
    patent_count: int = 0
    velocity_score: Optional[float] = None


class PriorArtResult(BaseModel):
    paper: PaperBrief
    relevance_score: float
    connection_type: str  # direct_citation, shared_entity, shared_patent_family
    shared_entities: list[str] = []


class VelocityAlert(BaseModel):
    doi: str
    title: Optional[str] = None
    velocity_score: float
    patent_citations: int
    is_spike: bool = False
    top_citing_patents: list[str] = []


# ============================================================
# 1. GRAPH TRAVERSAL ENDPOINTS
# ============================================================

@app.get("/graph/paper-to-patents/{doi:path}", response_model=list[PatentBrief],
         summary="1-hop: Paper → Patents citing it")
async def paper_to_patents(doi: str, limit: int = Query(50, le=500)):
    """Given a paper DOI, find all patents that cite it."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.patent_id, p.title, p.assignee, p.grant_date::text, p.jurisdiction
                FROM patent_paper_citations ppc
                JOIN patents p ON p.patent_id = ppc.patent_id
                WHERE ppc.paper_doi = %s
                ORDER BY p.grant_date DESC NULLS LAST
                LIMIT %s
            """, (doi, limit))
            rows = cur.fetchall()
            return [PatentBrief(
                patent_id=r[0], title=r[1], assignee=r[2],
                grant_date=r[3], jurisdiction=r[4]
            ) for r in rows]
    finally:
        put_conn(conn)


@app.get("/graph/patent-to-papers/{patent_id:path}", response_model=list[PaperBrief],
         summary="1-hop: Patent → Papers it cites")
async def patent_to_papers(patent_id: str, limit: int = Query(50, le=500)):
    """Given a patent ID, find all academic papers it cites."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ppc.paper_doi, p.title, p.year, p.citation_count
                FROM patent_paper_citations ppc
                LEFT JOIN papers p ON p.doi = ppc.paper_doi
                WHERE ppc.patent_id = %s
                ORDER BY p.citation_count DESC NULLS LAST
                LIMIT %s
            """, (patent_id, limit))
            rows = cur.fetchall()
            return [PaperBrief(
                doi=r[0], title=r[1], year=r[2], citation_count=r[3]
            ) for r in rows]
    finally:
        put_conn(conn)


@app.get("/graph/gene-to-patents/{gene_id}", response_model=list[PatentBrief],
         summary="2-hop: Gene → Papers mentioning it → Patents citing those papers")
async def gene_to_patents(gene_id: str, limit: int = Query(50, le=200)):
    """
    2-hop traversal: Find patents citing papers that mention a specific gene.
    This is the money query — shows which genes are being commercialized.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT pt.patent_id, pt.title, pt.assignee,
                       pt.grant_date::text, pt.jurisdiction
                FROM kg_paper_pubtator kp
                JOIN patent_paper_citations ppc ON ppc.paper_doi = (
                    SELECT doi FROM papers WHERE id = kp.paper_id LIMIT 1
                )
                JOIN patents pt ON pt.patent_id = ppc.patent_id
                WHERE kp.entity_id = %s
                ORDER BY pt.grant_date DESC NULLS LAST
                LIMIT %s
            """, (gene_id, limit))
            rows = cur.fetchall()
            return [PatentBrief(
                patent_id=r[0], title=r[1], assignee=r[2],
                grant_date=r[3], jurisdiction=r[4]
            ) for r in rows]
    finally:
        put_conn(conn)


@app.get("/graph/disease-to-drugs/{disease_name}",
         summary="2-hop: Disease → Papers → Drug targets (OpenTargets + DrugBank)")
async def disease_to_drugs(disease_name: str, limit: int = Query(50, le=200)):
    """
    Multi-hop: Disease → genes in disease papers → drugs targeting those genes.
    Combines PubTator disease annotations + Open Targets + DrugBank links.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH disease_papers AS (
                    SELECT DISTINCT kp.paper_id
                    FROM kg_pubtator_entities e
                    JOIN kg_paper_pubtator kp ON kp.entity_id = e.entity_id
                    WHERE e.entity_type = 'disease'
                      AND e.name ILIKE %s
                    LIMIT 10000
                ),
                disease_genes AS (
                    SELECT DISTINCT kp2.entity_id as gene_id, e2.name as gene_name
                    FROM disease_papers dp
                    JOIN kg_paper_pubtator kp2 ON kp2.paper_id = dp.paper_id
                    JOIN kg_pubtator_entities e2 ON e2.entity_id = kp2.entity_id
                    WHERE e2.entity_type = 'gene'
                )
                SELECT dg.gene_id, dg.gene_name,
                       dpl.drug_name, dpl.mechanism_of_action,
                       dpl.phase as clinical_phase
                FROM disease_genes dg
                LEFT JOIN drug_paper_links dpl ON dpl.target_id = dg.gene_id
                WHERE dpl.drug_name IS NOT NULL
                ORDER BY dpl.phase DESC NULLS LAST, dg.gene_name
                LIMIT %s
            """, (f"%{disease_name}%", limit))
            rows = cur.fetchall()
            return [{"gene_id": r[0], "gene_name": r[1], "drug_name": r[2],
                     "mechanism": r[3], "clinical_phase": r[4]} for r in rows]
    finally:
        put_conn(conn)


@app.get("/graph/institution-patents/{institution}",
         summary="2-hop: Institution → Papers → Patents citing them")
async def institution_to_patents(institution: str, limit: int = Query(50, le=200)):
    """Which patents cite papers from a specific institution? (TTO intelligence)"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pt.patent_id, pt.title, pt.assignee, pt.grant_date::text,
                       p.doi, p.title as paper_title
                FROM papers p
                JOIN patent_paper_citations ppc ON ppc.paper_doi = p.doi
                JOIN patents pt ON pt.patent_id = ppc.patent_id
                WHERE p.affiliations ILIKE %s
                ORDER BY pt.grant_date DESC NULLS LAST
                LIMIT %s
            """, (f"%{institution}%", limit))
            rows = cur.fetchall()
            return [{"patent_id": r[0], "patent_title": r[1], "assignee": r[2],
                     "grant_date": r[3], "paper_doi": r[4], "paper_title": r[5]}
                    for r in rows]
    finally:
        put_conn(conn)


# ============================================================
# 2. PRIOR ART DISCOVERY (strongest enterprise use case)
# ============================================================

@app.get("/prior-art/by-patent/{patent_id:path}",
         summary="Find prior art for a patent — papers it should have cited")
async def prior_art_by_patent(patent_id: str, limit: int = Query(30, le=100)):
    """
    Prior art discovery: Given a patent, find papers it SHOULD be citing
    but isn't — by finding papers that share entities/concepts with the
    papers it DOES cite.
    
    This is the #1 enterprise use case. Patent lawyers pay $10K+ per search.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Step 1: Get papers this patent already cites
            cur.execute("""
                SELECT paper_doi FROM patent_paper_citations
                WHERE patent_id = %s AND paper_doi IS NOT NULL
            """, (patent_id,))
            cited_dois = [r[0] for r in cur.fetchall()]

            if not cited_dois:
                return []

            # Step 2: Get entities from those papers
            cur.execute("""
                SELECT DISTINCT kp.entity_id, e.entity_type, e.name
                FROM papers p
                JOIN kg_paper_pubtator kp ON kp.paper_id = p.id
                JOIN kg_pubtator_entities e ON e.entity_id = kp.entity_id
                WHERE p.doi = ANY(%s)
                LIMIT 200
            """, (cited_dois,))
            entities = [(r[0], r[1], r[2]) for r in cur.fetchall()]
            entity_ids = [e[0] for e in entities]

            if not entity_ids:
                return []

            # Step 3: Find OTHER papers sharing those entities (not already cited)
            cur.execute("""
                SELECT p.doi, p.title, p.year, p.citation_count,
                       count(DISTINCT kp.entity_id) as shared_entities,
                       array_agg(DISTINCT e.name) FILTER (WHERE e.name IS NOT NULL) as entity_names
                FROM kg_paper_pubtator kp
                JOIN papers p ON p.id = kp.paper_id
                JOIN kg_pubtator_entities e ON e.entity_id = kp.entity_id
                WHERE kp.entity_id = ANY(%s)
                  AND p.doi IS NOT NULL
                  AND p.doi != ALL(%s)
                GROUP BY p.doi, p.title, p.year, p.citation_count
                HAVING count(DISTINCT kp.entity_id) >= 2
                ORDER BY count(DISTINCT kp.entity_id) DESC, p.citation_count DESC NULLS LAST
                LIMIT %s
            """, (entity_ids, cited_dois, limit))
            rows = cur.fetchall()

            return [PriorArtResult(
                paper=PaperBrief(doi=r[0], title=r[1], year=r[2], citation_count=r[3]),
                relevance_score=r[4] / max(len(entity_ids), 1),  # fraction of shared entities
                connection_type="shared_entity",
                shared_entities=(r[5] or [])[:10]
            ) for r in rows]
    finally:
        put_conn(conn)


@app.get("/prior-art/by-text",
         summary="Find prior art by technology description (text search → entity match)")
async def prior_art_by_text(
    query: str = Query(..., min_length=3, max_length=500),
    limit: int = Query(30, le=100)
):
    """
    Prior art search by text description. Uses full-text search + entity overlap.
    Input: describe the technology/invention.
    Output: papers that constitute potential prior art.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Full-text search with ranking
            cur.execute("""
                SELECT p.doi, p.title, p.year, p.citation_count,
                       ts_rank_cd(p.search_vector, query) as text_rank,
                       (SELECT count(*) FROM patent_paper_citations ppc 
                        WHERE ppc.paper_doi = p.doi) as patent_cites
                FROM papers p,
                     websearch_to_tsquery('english', %s) query
                WHERE p.search_vector @@ query
                  AND p.doi IS NOT NULL
                ORDER BY 
                    -- Papers already cited by patents are stronger prior art signals
                    (CASE WHEN EXISTS(
                        SELECT 1 FROM patent_paper_citations ppc WHERE ppc.paper_doi = p.doi
                    ) THEN 2.0 ELSE 1.0 END) * ts_rank_cd(p.search_vector, query) DESC
                LIMIT %s
            """, (query, limit))
            rows = cur.fetchall()

            return [PriorArtResult(
                paper=PaperBrief(
                    doi=r[0], title=r[1], year=r[2],
                    citation_count=r[3], patent_citations=r[5]
                ),
                relevance_score=float(r[4]),
                connection_type="text_match",
                shared_entities=[]
            ) for r in rows]
    finally:
        put_conn(conn)


# ============================================================
# 3. VELOCITY & INTELLIGENCE SIGNALS
# ============================================================

@app.get("/velocity/top-papers", response_model=list[VelocityAlert],
         summary="Papers with highest patent citation velocity (spike detection)")
async def velocity_top_papers(
    min_citations: int = Query(3, ge=1),
    spikes_only: bool = Query(False),
    limit: int = Query(50, le=500)
):
    """Top papers by patent citation velocity — the core VC signal."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            where_clause = "WHERE pcs.patent_citations_count >= %s"
            params = [min_citations]
            if spikes_only:
                where_clause += " AND pcs.is_spike = TRUE"

            cur.execute(f"""
                SELECT pcs.paper_doi, p.title, pcs.velocity_score,
                       pcs.patent_citations_count, pcs.is_spike,
                       array_agg(DISTINCT ppc.patent_id) FILTER (WHERE ppc.patent_id IS NOT NULL) as patents
                FROM patent_citation_signals pcs
                LEFT JOIN papers p ON p.doi = pcs.paper_doi
                LEFT JOIN patent_paper_citations ppc ON ppc.paper_doi = pcs.paper_doi
                {where_clause}
                GROUP BY pcs.paper_doi, p.title, pcs.velocity_score, 
                         pcs.patent_citations_count, pcs.is_spike
                ORDER BY pcs.velocity_score DESC, pcs.patent_citations_count DESC
                LIMIT %s
            """, params + [limit])
            rows = cur.fetchall()

            return [VelocityAlert(
                doi=r[0] or "",
                title=r[1],
                velocity_score=r[2] or 0,
                patent_citations=r[3] or 0,
                is_spike=r[4] or False,
                top_citing_patents=(r[5] or [])[:5]
            ) for r in rows]
    finally:
        put_conn(conn)


@app.get("/velocity/top-genes",
         summary="Genes with highest commercialization velocity")
async def velocity_top_genes(limit: int = Query(50, le=200)):
    """Top genes by combined publication velocity + patent citation density."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT vs.entity_id, e.name,
                       vs.composite_score, vs.velocity, vs.acceleration,
                       vs.total_papers, vs.patent_citations
                FROM velocity_gene_scores vs
                JOIN kg_pubtator_entities e ON e.entity_id = vs.entity_id
                ORDER BY vs.composite_score DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return [{"gene_id": r[0], "name": r[1], "composite_score": r[2],
                     "velocity": r[3], "acceleration": r[4],
                     "total_papers": r[5], "patent_citations": r[6]}
                    for r in rows]
    except Exception:
        # Table might not exist yet (velocity model still running)
        return []
    finally:
        put_conn(conn)


@app.get("/velocity/top-diseases",
         summary="Diseases with fastest-growing research activity")
async def velocity_top_diseases(limit: int = Query(50, le=200)):
    """Top diseases by publication velocity — where is research accelerating?"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT vs.entity_id, e.name,
                       vs.composite_score, vs.velocity, vs.acceleration,
                       vs.total_papers
                FROM velocity_disease_scores vs
                JOIN kg_pubtator_entities e ON e.entity_id = vs.entity_id
                ORDER BY vs.composite_score DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return [{"disease_id": r[0], "name": r[1], "composite_score": r[2],
                     "velocity": r[3], "acceleration": r[4], "total_papers": r[5]}
                    for r in rows]
    except Exception:
        return []
    finally:
        put_conn(conn)


# ============================================================
# 4. BULK INTELLIGENCE QUERIES
# ============================================================

@app.get("/intelligence/hot-assignees",
         summary="Top patent assignees citing academic papers (corporate R&D tracking)")
async def hot_assignees(limit: int = Query(30, le=100)):
    """Which companies are most actively citing academic research?"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pt.assignee, pt.assignee_type,
                       count(DISTINCT ppc.paper_doi) as papers_cited,
                       count(DISTINCT pt.patent_id) as patent_count,
                       max(pt.grant_date)::text as latest_grant
                FROM patents pt
                JOIN patent_paper_citations ppc ON ppc.patent_id = pt.patent_id
                WHERE pt.assignee IS NOT NULL AND pt.assignee != ''
                GROUP BY pt.assignee, pt.assignee_type
                ORDER BY papers_cited DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return [{"assignee": r[0], "type": r[1], "papers_cited": r[2],
                     "patent_count": r[3], "latest_grant": r[4]}
                    for r in rows]
    finally:
        put_conn(conn)


@app.get("/intelligence/commercialization-map",
         summary="Geographic/institutional heatmap of commercialization activity")
async def commercialization_map(limit: int = Query(50, le=200)):
    """Which institutions have the most patent-cited papers?"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.affiliations, count(DISTINCT ppc.patent_id) as patent_citations,
                       count(DISTINCT p.doi) as papers_cited_by_patents,
                       max(pt.grant_date)::text as latest
                FROM papers p
                JOIN patent_paper_citations ppc ON ppc.paper_doi = p.doi
                JOIN patents pt ON pt.patent_id = ppc.patent_id
                WHERE p.affiliations IS NOT NULL AND p.affiliations != ''
                GROUP BY p.affiliations
                ORDER BY patent_citations DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return [{"institution": r[0], "patent_citations": r[1],
                     "papers_cited": r[2], "latest_patent": r[3]}
                    for r in rows]
    finally:
        put_conn(conn)


@app.get("/intelligence/emerging-fields",
         summary="Technology domains with accelerating patent activity")
async def emerging_fields(limit: int = Query(30, le=100)):
    """IPC/CPC classification codes with fastest growth in patent filings."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT unnest(ipc_codes) as ipc, 
                       count(*) as patent_count,
                       count(*) FILTER (WHERE grant_date >= '2023-01-01') as recent_count,
                       count(*) FILTER (WHERE grant_date < '2023-01-01') as older_count
                FROM patents
                WHERE ipc_codes IS NOT NULL AND array_length(ipc_codes, 1) > 0
                GROUP BY ipc
                HAVING count(*) >= 10
                ORDER BY 
                    count(*) FILTER (WHERE grant_date >= '2023-01-01')::float / 
                    GREATEST(count(*) FILTER (WHERE grant_date < '2023-01-01'), 1) DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return [{"ipc_code": r[0], "total_patents": r[1],
                     "recent_patents": r[2], "older_patents": r[3],
                     "growth_ratio": r[2] / max(r[3], 1)}
                    for r in rows]
    finally:
        put_conn(conn)


# ============================================================
# 5. STATS & HEALTH
# ============================================================

@app.get("/stats", summary="Database statistics")
async def stats():
    """Current data coverage stats."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            stats = {}
            tables = [
                ("papers", "total_papers"),
                ("patents", "total_patents"),
                ("patent_paper_citations", "patent_paper_links"),
                ("kg_paper_pubtator", "kg_annotations"),
                ("kg_pubtator_entities", "kg_entities"),
                ("drug_paper_links", "drug_links"),
                ("patent_citation_signals", "velocity_signals"),
            ]
            for table, key in tables:
                try:
                    cur.execute(f"SELECT reltuples::bigint FROM pg_class WHERE relname = %s", (table,))
                    row = cur.fetchone()
                    stats[key] = row[0] if row else 0
                except Exception:
                    stats[key] = 0
            return stats
    finally:
        put_conn(conn)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "graph-intelligence", "version": "1.0.0"}


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=API_PORT, log_level="info")
