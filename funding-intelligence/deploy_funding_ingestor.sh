#!/bin/bash
# Deploy and run Funding Intelligence ingestor on paper-db server
# Usage: AWS_PROFILE=admin-delroy bash deploy_funding_ingestor.sh

set -e

INSTANCE_ID="i-0cb48faa3f931c661"
REGION="ap-southeast-1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "═══════════════════════════════════════════════════════"
echo "  Funding Intelligence Ingestor — Deployment"
echo "═══════════════════════════════════════════════════════"

# Step 1: Upload schema and ingestor to S3
echo ""
echo "1. Uploading files to S3..."
aws s3 cp "$SCRIPT_DIR/schema.sql" s3://nobleblocks-assets/funding-intelligence/schema.sql --region $REGION
aws s3 cp "$SCRIPT_DIR/ingest_openalex_entities.py" s3://nobleblocks-assets/funding-intelligence/ingest_openalex_entities.py --region $REGION
echo "   ✓ Uploaded to s3://nobleblocks-assets/funding-intelligence/"

# Step 2: Run schema migration on paper-db
echo ""
echo "2. Running schema migration..."
CMD_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters "{\"commands\":[
    \"aws s3 cp s3://nobleblocks-assets/funding-intelligence/schema.sql /tmp/funding_schema.sql --region $REGION\",
    \"sudo -u postgres psql -d paper_search -f /tmp/funding_schema.sql 2>&1 | tail -20\"
  ]}" \
  --region "$REGION" \
  --timeout-seconds 120 \
  --output text \
  --query 'Command.CommandId')

echo "   SSM Command: $CMD_ID"
echo "   Waiting for schema migration..."
aws ssm wait command-executed --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" --region "$REGION" 2>/dev/null || true
sleep 5

RESULT=$(aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" --region "$REGION" --output text --query '[Status, StandardOutputContent]' 2>&1)
echo "   $RESULT" | head -20

# Step 3: Deploy ingestor script
echo ""
echo "3. Deploying ingestor script..."
CMD_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters "{\"commands\":[
    \"aws s3 cp s3://nobleblocks-assets/funding-intelligence/ingest_openalex_entities.py /opt/nobleblocks/paper-db/scripts/ingest_openalex_entities.py --region $REGION\",
    \"chmod +x /opt/nobleblocks/paper-db/scripts/ingest_openalex_entities.py\",
    \"pip3 install requests psycopg2-binary 2>&1 | tail -3\"
  ]}" \
  --region "$REGION" \
  --timeout-seconds 60 \
  --output text \
  --query 'Command.CommandId')

echo "   SSM Command: $CMD_ID"
aws ssm wait command-executed --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" --region "$REGION" 2>/dev/null || true
sleep 3
echo "   ✓ Ingestor deployed to /opt/nobleblocks/paper-db/scripts/"

# Step 4: Start ingestion (small entities first)
echo ""
echo "4. Starting ingestion (topics → publishers → sources → institutions → funders)..."
echo "   Awards (12M, 3GB) will run after small entities complete."
echo ""

CMD_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters "{\"commands\":[
    \"cd /opt/nobleblocks/paper-db/scripts && nohup python3 -u ingest_openalex_entities.py --entity all > /tmp/funding_ingest.log 2>&1 &\",
    \"echo 'Ingestor PID:' \$!\",
    \"sleep 5 && tail -20 /tmp/funding_ingest.log\"
  ]}" \
  --region "$REGION" \
  --timeout-seconds 30 \
  --output text \
  --query 'Command.CommandId')

echo "   SSM Command: $CMD_ID"
aws ssm wait command-executed --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" --region "$REGION" 2>/dev/null || true
sleep 8

RESULT=$(aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" --region "$REGION" --output text --query 'StandardOutputContent' 2>&1)
echo "$RESULT"

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Deployment complete!"
echo ""
echo "  Monitor: ssh paper-db 'tail -f /tmp/funding_ingest.log'"
echo "  Or:      aws ssm send-command --instance-ids $INSTANCE_ID \\"
echo "           --parameters '{\"commands\":[\"tail -50 /tmp/funding_ingest.log\"]}' \\"
echo "           --document-name AWS-RunShellScript --region $REGION"
echo ""
echo "  Expected timeline:"
echo "    Topics (4.5K):       ~5 seconds"
echo "    Publishers (10.7K):  ~10 seconds"
echo "    Sources (280K):      ~2 minutes"
echo "    Institutions (121K): ~1 minute"
echo "    Funders (32K):       ~30 seconds"
echo "    Awards (12.2M):      ~30-45 minutes"
echo "═══════════════════════════════════════════════════════"
