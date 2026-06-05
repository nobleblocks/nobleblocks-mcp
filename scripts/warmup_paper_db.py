#!/usr/bin/env python3
"""
Paper DB GIN Index Warm-up Script

Pre-queries the top ~100 academic search terms to load GIN index pages
into PostgreSQL's shared_buffers. Without this, cold GIN scans take 17-18s
for broad terms. After warming, queries respond in <1s.

Run via cron every 4h (GIN pages get evicted after ~3-4h of inactivity):
  0 */4 * * * /usr/bin/python3 /opt/nobleblocks/scripts/warmup_paper_db.py

Or on Paper DB EC2 startup:
  /opt/nobleblocks/scripts/warmup_paper_db.py
"""

import asyncio
import time
import httpx

# Paper DB direct URL (run this on the same EC2 or VPC)
PAPER_DB_URL = "http://localhost:8000"
# Fallback: use prod frontend
FRONTEND_URL = "https://www.nobleblocks.com"
INTERNAL_TOKEN = "6vdNeoQJ0Dbi2-NDhRiNljKZoKCzxxg2vxI-i1oy7u56mdr_YP6K1H9RDu1amXLu"

# Top academic search terms — covers ~80% of search traffic patterns
WARMUP_QUERIES = [
    # Life sciences
    "CRISPR", "cancer", "COVID-19", "mRNA", "vaccine", "protein",
    "gene therapy", "stem cells", "immunotherapy", "antibiotics",
    "epigenetics", "microbiome", "neuroscience", "Alzheimer",
    "diabetes", "genomics", "proteomics", "metabolism", "apoptosis",
    "inflammation", "biomarkers", "clinical trials", "drug discovery",
    "cell signaling", "gene expression", "molecular biology",
    # AI/CS
    "machine learning", "deep learning", "artificial intelligence",
    "neural networks", "transformer", "large language models",
    "computer vision", "natural language processing", "reinforcement learning",
    "graph neural networks", "attention mechanism", "generative AI",
    # Physics/Chemistry
    "quantum computing", "quantum mechanics", "superconductor",
    "nanotechnology", "materials science", "catalysis", "photovoltaic",
    "battery", "fusion energy", "dark matter", "gravitational waves",
    # Environment
    "climate change", "sustainability", "biodiversity", "carbon capture",
    "renewable energy", "deforestation", "ocean acidification",
    # Social sciences
    "mental health", "education", "economics", "public health",
    "inequality", "psychology", "cognitive science",
    # Broad single-word terms (these cause the worst cold GIN scans)
    "AI", "biology", "chemistry", "physics", "mathematics",
    "engineering", "medicine", "genetics", "ecology", "robotics",
    # Terms that caused cold-cache failures in stress tests
    "transformer", "large language models", "IL-6 inflammation",
    "chimeric antigen receptor T cell", "photosynthesis", "graphene",
    "autonomous vehicles", "perimenopause", "β-amyloid plaque",
    "mRNA vaccine technology", "HER2 breast cancer", "epigenetics",
    "attention mechanism neural networks", "quantum computing error correction",
]


async def warmup(use_local: bool = True):
    base_url = PAPER_DB_URL if use_local else FRONTEND_URL
    headers = {"x-internal-token": INTERNAL_TOKEN} if not use_local else {}

    print(f"Warming up Paper DB GIN index ({len(WARMUP_QUERIES)} queries)")
    print(f"Target: {base_url}")
    print("=" * 60)

    start = time.time()
    slow = []

    async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
        sem = asyncio.Semaphore(3)  # Don't overload DB

        async def query_one(q: str):
            async with sem:
                t0 = time.time()
                try:
                    if use_local:
                        url = f"{base_url}/api/v1/search/keyword"
                        params = {"query": q, "limit": 5}
                    else:
                        url = f"{base_url}/api/v1/papers/search"
                        params = {"query": q, "limit": 5, "phase": "fast"}
                    await client.get(url, params=params)
                    elapsed = time.time() - t0
                    if elapsed > 5.0:
                        slow.append((elapsed, q))
                    return elapsed
                except Exception as e:
                    elapsed = time.time() - t0
                    print(f"  WARN: {q} failed ({e}) in {elapsed:.1f}s")
                    return elapsed

        latencies = await asyncio.gather(*[query_one(q) for q in WARMUP_QUERIES])

    total = time.time() - start
    avg = sum(latencies) / len(latencies)
    print(f"\nDone in {total:.1f}s total")
    print(f"  Avg latency: {avg:.2f}s")
    print(f"  Max latency: {max(latencies):.2f}s")
    if slow:
        print(f"  Slow (>5s): {len(slow)}")
        for elapsed, q in sorted(slow, reverse=True)[:10]:
            print(f"    {elapsed:.1f}s | {q}")
    print("\nGIN index pages now in shared_buffers — queries should be <1s")


if __name__ == "__main__":
    import sys
    use_local = "--remote" not in sys.argv
    if not use_local:
        print("Using remote frontend (phase=fast)")
    asyncio.run(warmup(use_local=use_local))
