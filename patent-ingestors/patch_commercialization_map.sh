#!/bin/bash
# Patch graph_intelligence_api.py to use mv_commercialization_map
# Run via SSM on the paper-db server

API_FILE="/opt/nobleblocks/paper-db/scripts/graph_intelligence_api.py"

# Replace the commercialization_map function with matview version
python3 - "$API_FILE" << 'PYEOF'
import sys

filepath = sys.argv[1]
with open(filepath, 'r') as f:
    content = f.read()

# Find and replace the commercialization_map function body
old_body = '''async def commercialization_map(limit: int = Query(50, le=200)):
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
        put_conn(conn)'''

new_body = '''async def commercialization_map(limit: int = Query(50, le=200)):
    """Which assignees have the most patent-to-paper citations? (Uses pre-computed matview)"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT institution, assignee_type, patent_count,
                       cited_papers, earliest_patent, latest_patent
                FROM mv_commercialization_map
                ORDER BY cited_papers DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return [{"institution": r[0], "assignee_type": r[1],
                     "patent_count": r[2], "papers_cited": r[3],
                     "earliest_patent": r[4], "latest_patent": r[5]}
                    for r in rows]
    finally:
        put_conn(conn)'''

if old_body in content:
    content = content.replace(old_body, new_body)
    with open(filepath, 'w') as f:
        f.write(content)
    print("SUCCESS: Patched commercialization_map to use mv_commercialization_map")
else:
    print("WARNING: Could not find exact match for old code. Manual patch needed.")
    print("Trying partial match...")
    # Try line-by-line approach
    if "p.affiliations" in content and "commercialization_map" in content:
        # Replace the function entirely using line numbers
        lines = content.split('\n')
        start = None
        end = None
        for i, line in enumerate(lines):
            if 'async def commercialization_map' in line:
                start = i
            elif start is not None and line.startswith('@app.') and i > start + 3:
                end = i
                break
            elif start is not None and line.startswith('async def ') and i > start + 3:
                end = i
                break
        
        if start is not None and end is not None:
            new_lines = lines[:start] + new_body.split('\n') + ['', ''] + lines[end:]
            with open(filepath, 'w') as f:
                f.write('\n'.join(new_lines))
            print(f"SUCCESS: Replaced lines {start+1}-{end} with matview version")
        else:
            print(f"FAILED: Could not determine function boundaries (start={start}, end={end})")
    else:
        print("FAILED: commercialization_map or p.affiliations not found in file")
PYEOF
