#!/bin/bash
# Add noble_id column to funders table
sudo -u postgres psql -d paper_search <<'EOF'
-- Add noble_id column to funders for NobleID integration
ALTER TABLE funders ADD COLUMN IF NOT EXISTS noble_id VARCHAR(64);
CREATE INDEX IF NOT EXISTS idx_funders_noble_id ON funders(noble_id) WHERE noble_id IS NOT NULL;

-- Add crossref_id for CrossRef funder registry matching
ALTER TABLE funders ADD COLUMN IF NOT EXISTS crossref_id TEXT;
CREATE INDEX IF NOT EXISTS idx_funders_crossref_id ON funders(crossref_id) WHERE crossref_id IS NOT NULL;

-- Verify
\d funders
SELECT 'noble_id column added successfully' AS status;
EOF
