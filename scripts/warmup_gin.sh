#!/bin/bash
# Paper DB GIN Index Warmup — runs every 4h via cron
# Keeps PostgreSQL shared_buffers warm so keyword searches respond in <1s
# Without this, cold GIN scans on 348M papers take 17-25s

BASE="http://localhost:8080/api/v1/search/keyword"
QUERIES=(
  "CRISPR" "cancer" "COVID-19" "mRNA" "vaccine" "protein"
  "gene+therapy" "stem+cells" "immunotherapy" "antibiotics"
  "epigenetics" "microbiome" "neuroscience" "Alzheimer"
  "diabetes" "genomics" "proteomics" "metabolism" "apoptosis"
  "inflammation" "biomarkers" "clinical+trials" "drug+discovery"
  "machine+learning" "deep+learning" "artificial+intelligence"
  "neural+networks" "transformer" "large+language+models"
  "computer+vision" "natural+language+processing"
  "quantum+computing" "quantum+mechanics" "nanotechnology"
  "materials+science" "catalysis" "photovoltaic" "battery"
  "climate+change" "sustainability" "biodiversity" "carbon+capture"
  "renewable+energy" "mental+health" "education" "economics"
  "AI" "biology" "chemistry" "physics" "mathematics"
  "engineering" "medicine" "genetics" "ecology" "robotics"
  "graphene" "photosynthesis" "autonomous+vehicles"
  "attention+mechanism" "gravitational+waves"
  "chimeric+antigen+receptor" "IL-6+inflammation"
)

echo "[$(date -Iseconds)] GIN warmup: ${#QUERIES[@]} queries"
SLOW=0
for q in "${QUERIES[@]}"; do
  T0=$(date +%s%N)
  curl -s -o /dev/null -m 30 "${BASE}?query=${q}&limit=5"
  T1=$(date +%s%N)
  MS=$(( (T1 - T0) / 1000000 ))
  if [ "$MS" -gt 5000 ]; then
    echo "  SLOW: ${q} (${MS}ms)"
    SLOW=$((SLOW+1))
  fi
done
echo "[$(date -Iseconds)] Done. Slow queries (>5s): $SLOW"
