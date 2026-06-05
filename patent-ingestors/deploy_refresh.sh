#!/bin/bash
# Deploy patent refresh orchestrator + systemd timer to paper-db server
# Run from local Mac: bash deploy_refresh.sh

set -e

BUCKET="nobleblocks-paper-db-backups"
REGION="ap-southeast-1"
INSTANCE="i-0cb48faa3f931c661"
DEPLOY_PREFIX="deploy/patent-ingestors"

echo "=== Uploading patent refresh files to S3 ==="
cd "$(dirname "$0")"

aws s3 cp patent_refresh_orchestrator.py "s3://$BUCKET/$DEPLOY_PREFIX/patent_refresh_orchestrator.py" --region $REGION
aws s3 cp systemd/patent-refresh.service "s3://$BUCKET/$DEPLOY_PREFIX/systemd/patent-refresh.service" --region $REGION
aws s3 cp systemd/patent-refresh.timer "s3://$BUCKET/$DEPLOY_PREFIX/systemd/patent-refresh.timer" --region $REGION

echo "=== Deploying to server via SSM ==="
aws ssm send-command \
  --instance-ids "$INSTANCE" \
  --document-name "AWS-RunShellScript" \
  --parameters '{"commands":[
    "set -e",
    "mkdir -p /opt/nobleblocks/paper-db/patent-ingestors",
    "aws s3 cp s3://nobleblocks-paper-db-backups/deploy/patent-ingestors/patent_refresh_orchestrator.py /opt/nobleblocks/paper-db/patent-ingestors/patent_refresh_orchestrator.py --region ap-southeast-1",
    "aws s3 cp s3://nobleblocks-paper-db-backups/deploy/patent-ingestors/systemd/patent-refresh.service /etc/systemd/system/patent-refresh.service --region ap-southeast-1",
    "aws s3 cp s3://nobleblocks-paper-db-backups/deploy/patent-ingestors/systemd/patent-refresh.timer /etc/systemd/system/patent-refresh.timer --region ap-southeast-1",
    "systemctl daemon-reload",
    "systemctl enable patent-refresh.timer",
    "systemctl start patent-refresh.timer",
    "echo === Timer Status ===",
    "systemctl status patent-refresh.timer --no-pager",
    "echo === Next Run ===",
    "systemctl list-timers patent-refresh.timer --no-pager"
  ]}' \
  --timeout-seconds 120 \
  --region "$REGION" \
  --query "Command.CommandId" \
  --output text

echo "=== Deploy command sent. Check status with: ==="
echo "aws ssm get-command-invocation --command-id <CMD_ID> --instance-id $INSTANCE --region $REGION"
