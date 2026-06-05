#!/bin/bash
# Start all working patent ingestors as systemd transient services
# Deploy: copy to server, run as root
set -e

DIR=/opt/nobleblocks/paper-db/patent-ingestors
LOG=/tmp

# Common env vars
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=paper_search
export DB_USER=nobleblocks
export DB_PASS=nb_papers_2026_prod
export NCBI_EMAIL=admin@nobleblocks.com

echo "=== Stopping existing patent ingestors ==="
systemctl stop patent-crossref 2>/dev/null || true
systemctl stop patent-genbank 2>/dev/null || true  
systemctl stop patent-patentsview 2>/dev/null || true
systemctl stop patent-s2 2>/dev/null || true
# Wait for clean shutdown
sleep 2

echo "=== Syncing latest scripts from S3 ==="
cd $DIR
aws s3 sync s3://nobleblocks-paper-db-backups/deploy/patent-ingestors/ . --region ap-southeast-1
chmod +x *.py

echo "=== Starting PatentsView ingestor ==="
systemd-run --unit=patent-patentsview \
  --setenv=DB_HOST=$DB_HOST --setenv=DB_PORT=$DB_PORT \
  --setenv=DB_NAME=$DB_NAME --setenv=DB_USER=$DB_USER --setenv=DB_PASS=$DB_PASS \
  --property=StandardOutput=append:$LOG/patent_patentsview.log \
  --property=StandardError=append:$LOG/patent_patentsview.log \
  --property=Restart=on-failure \
  /usr/bin/python3 -u $DIR/run_ingestor.py ingest_patentsview.py
echo "  Started patent-patentsview"

echo "=== Starting GenBank ingestor (fixed parser) ==="
systemd-run --unit=patent-genbank \
  --setenv=DB_HOST=$DB_HOST --setenv=DB_PORT=$DB_PORT \
  --setenv=DB_NAME=$DB_NAME --setenv=DB_USER=$DB_USER --setenv=DB_PASS=$DB_PASS \
  --setenv=NCBI_EMAIL=$NCBI_EMAIL \
  --property=StandardOutput=append:$LOG/patent_genbank.log \
  --property=StandardError=append:$LOG/patent_genbank.log \
  --property=Restart=on-failure \
  /usr/bin/python3 -u $DIR/run_ingestor.py ingest_genbank_sequences.py
echo "  Started patent-genbank"

echo "=== Starting Semantic Scholar ingestor ==="
systemd-run --unit=patent-s2 \
  --setenv=DB_HOST=$DB_HOST --setenv=DB_PORT=$DB_PORT \
  --setenv=DB_NAME=$DB_NAME --setenv=DB_USER=$DB_USER --setenv=DB_PASS=$DB_PASS \
  --property=StandardOutput=append:$LOG/patent_s2.log \
  --property=StandardError=append:$LOG/patent_s2.log \
  --property=Restart=on-failure \
  /usr/bin/python3 -u $DIR/run_ingestor.py ingest_semantic_scholar.py
echo "  Started patent-s2"

echo ""
echo "=== All ingestors started ==="
echo "Logs:"
echo "  PatentsView: $LOG/patent_patentsview.log"
echo "  GenBank:     $LOG/patent_genbank.log"
echo "  Sem Scholar: $LOG/patent_s2.log"
echo ""
echo "Monitor: systemctl status patent-patentsview patent-genbank patent-s2"
echo "DB check: psql -U nobleblocks paper_search -c 'SELECT * FROM patent_ingest_status'"
