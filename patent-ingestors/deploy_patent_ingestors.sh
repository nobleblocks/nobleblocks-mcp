#!/bin/bash
# deploy_patent_ingestors.sh
# Uploads all patent ingestor scripts to S3 and deploys to Paper DB server.
# Run from: /Users/bblist/projects/nobleblocks/nobleblocks-mcp/patent-ingestors/
set -e

REGION="ap-southeast-1"
INSTANCE_ID="i-0cb48faa3f931c661"
S3_BUCKET="nobleblocks-paper-db-backups"
S3_PREFIX="deploy/patent-ingestors"
REMOTE_DIR="/opt/nobleblocks/paper-db/patent-ingestors"

echo "═══════════════════════════════════════════════════"
echo "  Patent Ingestor Deployment"
echo "═══════════════════════════════════════════════════"

# Upload all scripts to S3
echo ""
echo "  Uploading to s3://${S3_BUCKET}/${S3_PREFIX}/..."
for f in *.py; do
    aws s3 cp "$f" "s3://${S3_BUCKET}/${S3_PREFIX}/${f}" --region $REGION
    echo "    ✓ $f"
done

echo ""
echo "  Deploying to Paper DB server ($INSTANCE_ID)..."

# Deploy via SSM
COMMAND_ID=$(aws ssm send-command \
    --instance-ids "$INSTANCE_ID" \
    --document-name "AWS-RunShellScript" \
    --parameters "commands=[
        \"set -e\",
        \"mkdir -p ${REMOTE_DIR}\",
        \"cd ${REMOTE_DIR}\",
        \"aws s3 sync s3://${S3_BUCKET}/${S3_PREFIX}/ . --region ${REGION}\",
        \"chmod +x *.py\",
        \"pip3 install psycopg2-binary requests --quiet 2>/dev/null || true\",
        \"echo '✓ All patent ingestor scripts deployed to ${REMOTE_DIR}'\",
        \"ls -la ${REMOTE_DIR}/\"
    ]" \
    --region "$REGION" \
    --output text \
    --query 'Command.CommandId')

echo "  SSM Command: $COMMAND_ID"
echo "  Waiting for completion..."

sleep 10

# Get result
aws ssm get-command-invocation \
    --command-id "$COMMAND_ID" \
    --instance-id "$INSTANCE_ID" \
    --region "$REGION" \
    --query '[Status, StandardOutputContent]' \
    --output text

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Deployment complete!"
echo ""
echo "  Next steps (run on server):"
echo "    1. Create schema:  python3 ${REMOTE_DIR}/create_patent_schema.py"
echo "    2. OpenAlex:       python3 ${REMOTE_DIR}/ingest_openalex_patents.py"
echo "    3. USPTO:          python3 ${REMOTE_DIR}/ingest_uspto_bulk.py"
echo "    4. GenBank:        python3 ${REMOTE_DIR}/ingest_genbank_sequences.py"
echo "    5. EPO (needs key): python3 ${REMOTE_DIR}/ingest_epo_ops.py"
echo "═══════════════════════════════════════════════════"
