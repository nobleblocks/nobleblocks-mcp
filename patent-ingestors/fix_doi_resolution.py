#!/usr/bin/env python3
"""Quick script to run diagnostics and DOI resolution fix on paper-db server via SSM."""
import json
import subprocess
import time
import sys

INSTANCE = "i-0cb48faa3f931c661"
REGION = "ap-southeast-1"

def run_ssm(commands, timeout=120):
    """Run commands on server via SSM and return output."""
    cmd_json = json.dumps({"commands": commands})
    result = subprocess.run([
        "/opt/homebrew/bin/aws", "ssm", "send-command",
        "--instance-ids", INSTANCE,
        "--document-name", "AWS-RunShellScript",
        "--parameters", cmd_json,
        "--region", REGION,
        "--timeout-seconds", str(timeout),
        "--query", "Command.CommandId",
        "--output", "text"
    ], capture_output=True, text=True, env={"AWS_PROFILE": "admin-delroy", "PATH": "/usr/local/bin:/usr/bin:/bin"})
    
    cmd_id = result.stdout.strip()
    if not cmd_id:
        print(f"ERROR sending command: {result.stderr}")
        return None
    
    print(f"  Command ID: {cmd_id}")
    
    # Poll for result
    for _ in range(30):
        time.sleep(5)
        poll = subprocess.run([
            "/opt/homebrew/bin/aws", "ssm", "get-command-invocation",
            "--command-id", cmd_id,
            "--instance-id", INSTANCE,
            "--region", REGION,
            "--output", "json"
        ], capture_output=True, text=True, env={"AWS_PROFILE": "admin-delroy", "PATH": "/usr/local/bin:/usr/bin:/bin"})
        
        data = json.loads(poll.stdout)
        status = data.get("Status")
        if status in ("Success", "Failed", "Cancelled", "TimedOut"):
            print(f"  Status: {status}")
            stdout = data.get("StandardOutputContent", "")
            stderr = data.get("StandardErrorContent", "")
            if stdout:
                print(stdout)
            if stderr:
                print(f"  STDERR: {stderr[:500]}")
            return stdout
    
    print("  TIMEOUT waiting for command")
    return None


if __name__ == "__main__":
    import os
    os.environ["AWS_PROFILE"] = "admin-delroy"
    os.environ["PATH"] = "/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin"

    print("=" * 60)
    print("  Paper-DB Patent Data Diagnostics")
    print("=" * 60)

    # Step 1: Check enriched ingestor + sample DOIs
    print("\n[1] Checking enriched ingestor & DOI samples...")
    run_ssm([
        "export PGPASSWORD=nb_papers_2026_prod",
        "echo '=== ENRICHED INGESTOR ==='",
        "ps -p 5412 --no-headers 2>/dev/null || echo DEAD",
        "tail -20 /tmp/enriched_citations.log 2>/dev/null || echo NO_LOG",
        "echo '=== REAL DOIS (10.1xxx) ==='",
        "psql -U nobleblocks -d paper_search -h localhost -t -c \"SELECT paper_doi FROM patent_paper_citations WHERE paper_doi LIKE '10.1%' LIMIT 5;\"",
        "echo '=== PAPERS TABLE DOIS ==='",
        "psql -U nobleblocks -d paper_search -h localhost -t -c \"SELECT doi FROM papers WHERE doi LIKE '10.1%' LIMIT 5;\"",
    ])

    # Step 2: Quick match test (using existing index)
    print("\n[2] Quick DOI match test...")
    run_ssm([
        "export PGPASSWORD=nb_papers_2026_prod",
        "psql -U nobleblocks -d paper_search -h localhost -t -c \"SELECT ppc.paper_doi FROM patent_paper_citations ppc JOIN papers p ON ppc.paper_doi = p.doi WHERE ppc.paper_doi IS NOT NULL AND ppc.paper_doi LIKE '10.1%' LIMIT 5;\"",
    ])

    # Step 3: Create index if needed and run resolution
    print("\n[3] Creating index + running DOI resolution...")
    run_ssm([
        "export PGPASSWORD=nb_papers_2026_prod",
        "psql -U nobleblocks -d paper_search -h localhost -c \"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ppc_paper_doi ON patent_paper_citations(paper_doi) WHERE paper_doi IS NOT NULL;\"",
        "echo INDEX_DONE",
    ], timeout=300)

    print("\n[4] Running resolution UPDATE...")
    run_ssm([
        "export PGPASSWORD=nb_papers_2026_prod",
        "psql -U nobleblocks -d paper_search -h localhost -c \"UPDATE patent_paper_citations ppc SET paper_id = p.id, paper_title = p.title FROM papers p WHERE ppc.paper_doi = p.doi AND ppc.paper_id IS NULL AND ppc.paper_doi IS NOT NULL;\"",
        "echo '=== RESOLVED COUNT ==='",
        "psql -U nobleblocks -d paper_search -h localhost -t -c \"SELECT count(*) FROM patent_paper_citations WHERE paper_id IS NOT NULL;\"",
    ], timeout=600)

    print("\nDone!")
