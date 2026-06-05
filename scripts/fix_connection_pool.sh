#!/bin/bash
# Fix connection saturation on paper-db
# - Increase PG max_connections from 200 → 400
# - Reduce per-worker pool maxconn from 80 → 25 (8 workers × 25 = 200, safe headroom)
#
# Deploy via SSM:
#   aws s3 cp fix_connection_pool.sh s3://nobleblocks-deploy-temp/paper-db/fix_connection_pool.sh
#   aws ssm send-command --instance-ids i-0cb48faa3f931c661 \
#     --document-name AWS-RunShellScript \
#     --parameters 'commands=["aws s3 cp s3://nobleblocks-deploy-temp/paper-db/fix_connection_pool.sh /tmp/fix_connection_pool.sh && bash /tmp/fix_connection_pool.sh"]' \
#     --region ap-southeast-1

set -e

echo "=== Fixing paper-db connection pool ==="

# 1. Increase PG max_connections (requires restart)
echo "Increasing max_connections to 400..."
sudo -u postgres psql -c "ALTER SYSTEM SET max_connections = 400;"
# This takes effect after restart; skip restart during business hours

# 2. Reduce per-worker pool maxconn (prevents pool exhaustion)
echo "Patching search_api.py pool maxconn: 80 → 25..."
sudo sed -i 's/maxconn=80/maxconn=25/' /opt/nobleblocks/paper-db/search_api.py

# 3. Add idle connection timeout (reclaims stale connections)
# Check if idle_in_transaction_session_timeout is already set
IDLE_TIMEOUT=$(sudo -u postgres psql -t -c "SHOW idle_in_transaction_session_timeout;" | tr -d ' ')
if [ "$IDLE_TIMEOUT" = "0" ] || [ "$IDLE_TIMEOUT" = "0ms" ]; then
    echo "Setting idle_in_transaction_session_timeout to 30s..."
    sudo -u postgres psql -c "ALTER SYSTEM SET idle_in_transaction_session_timeout = '30s';"
    sudo -u postgres psql -c "SELECT pg_reload_conf();"
fi

# 4. Restart search API to pick up new pool settings
echo "Restarting paper-search-api..."
sudo systemctl restart paper-search-api

# 5. Verify
sleep 3
echo "=== Verification ==="
curl -s http://localhost:8080/health | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Health: {d.get(\"status\")}, papers: {d.get(\"total_papers\",\"?\")}')" 2>/dev/null || echo "Health check failed (API may still be starting)"

echo ""
echo "Done. max_connections change requires PG restart (run during next maintenance window):"
echo "  sudo systemctl restart postgresql"
echo ""
echo "Current state:"
sudo -u postgres psql -c "SELECT count(*) as connections FROM pg_stat_activity;" 2>/dev/null || true
