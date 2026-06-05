#!/bin/bash
sudo -u postgres psql -d paper_search <<'EOF'
\d funders
\d funding_edges
SELECT COUNT(*) AS total_funders FROM funders;
SELECT * FROM funders LIMIT 5;
SELECT COUNT(*) AS total_funding_edges FROM funding_edges;
EOF
