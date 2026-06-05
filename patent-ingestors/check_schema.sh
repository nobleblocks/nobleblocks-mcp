#!/bin/bash
# Check funding schema
sudo -u postgres psql -d paper_search <<'EOF'
\dt *fund*
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_name = 'papers' AND column_name LIKE '%fund%'
ORDER BY ordinal_position;

SELECT column_name, data_type FROM information_schema.columns 
WHERE table_name = 'papers'
ORDER BY ordinal_position;
EOF
