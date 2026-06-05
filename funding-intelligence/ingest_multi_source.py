#!/usr/bin/env python3
"""
Multi-Source Funding Data Ingestor

OpenAlex has gaps: only ~30% of awards have dollar amounts, rate-limited API,
and their data could become restricted. This script ingests from ALL major
open grant databases to build a comprehensive funding picture.

Sources:
  1. NIH Reporter        - All US NIH grants (publicly searchable, full amounts)
  2. NSF Awards          - All US NSF grants (full REST API, complete data)
  3. Europe PMC Grants   - European grants linked to papers
  4. CrossRef Funder     - DOI-to-funder linkages from publisher metadata
  5. UKRI Gateway        - UK Research & Innovation (EPSRC, BBSRC, MRC, etc.)
  6. Australian ARC      - Australian Research Council grants
  7. Japan KAKEN         - Japanese grants (JSPS) - already in OpenAlex via provenance
  8. Dimensions (free)   - Limited free API for grants

Each source fills gaps the others miss:
  - OpenAlex: broad coverage, weak on amounts
  - NIH Reporter: complete US biomedical funding with exact dollars
  - NSF Awards: complete US STEM funding with exact dollars
  - Europe PMC: strong on EU/UK grants linked to publications
  - CrossRef: knows which funder paid for which DOI (from publisher metadata)
  - UKRI: complete UK research council funding
"""

import os
import sys
import json
import time
import gzip
import signal
import logging
import hashlib
import requests
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from pathlib import Path

import psycopg2
import psycopg2.extras

# ─── Configuration ──────────────────────────────────────────────────────────────

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', '5432')),
    'dbname': os.environ.get('DB_NAME', 'paper_search'),
    'user': os.environ.get('DB_USER', 'nobleblocks'),
    'password': os.environ.get('DB_PASS', 'nb_papers_2026_prod'),
}

PROGRESS_DIR = Path('/tmp/funding_multi_source/')
PROGRESS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/tmp/funding_multi_source/ingest.log')
    ]
)
log = logging.getLogger(__name__)

# Rate limiting
RATE_LIMITS = {
    'nih_reporter': 1.0,     # 1 req/sec
    'nsf': 1.0,              # 1 req/sec (conservative)
    'europe_pmc': 0.5,       # 2 req/sec allowed
    'crossref': 0.1,         # 10 req/sec with polite pool
    'ukri': 1.0,             # 1 req/sec
}

shutdown_requested = False

def signal_handler(sig, frame):
    global shutdown_requested
    log.warning("Shutdown requested, finishing current batch...")
    shutdown_requested = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


# ─── Database Connection ────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def save_progress(source: str, key: str, value: str):
    """Save ingestion progress for resume capability."""
    progress_file = PROGRESS_DIR / f'{source}_progress.json'
    progress = {}
    if progress_file.exists():
        progress = json.loads(progress_file.read_text())
    progress[key] = value
    progress['updated_at'] = datetime.now().isoformat()
    progress_file.write_text(json.dumps(progress, indent=2))


def load_progress(source: str, key: str) -> Optional[str]:
    """Load saved progress for resume."""
    progress_file = PROGRESS_DIR / f'{source}_progress.json'
    if progress_file.exists():
        progress = json.loads(progress_file.read_text())
        return progress.get(key)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE 1: NIH REPORTER
# https://api.reporter.nih.gov/
# Complete US NIH grant data: amounts, PIs, institutions, publications
# ═══════════════════════════════════════════════════════════════════════════════

NIH_REPORTER_SEARCH_URL = "https://api.reporter.nih.gov/v2/projects/search"
NIH_REPORTER_PUBS_URL = "https://api.reporter.nih.gov/v2/publications/search"

def ingest_nih_reporter(fiscal_years: List[int] = None, batch_size: int = 500):
    """
    Ingest NIH grants from NIH Reporter API.
    
    NIH Reporter has:
    - Complete funding amounts (direct + indirect costs)
    - PI details with ORCID
    - Institution details
    - FOA (Funding Opportunity Announcement)
    - Linked publications via PubMed IDs
    - ~1.2M active projects, ~5M total historical
    """
    if fiscal_years is None:
        fiscal_years = list(range(2000, 2027))  # 2000-2026
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Resume from last processed year/offset
    last_year = load_progress('nih_reporter', 'last_year')
    last_offset = int(load_progress('nih_reporter', 'last_offset') or '0')
    
    total_inserted = 0
    
    for year in fiscal_years:
        if last_year and year < int(last_year):
            continue
        
        offset = last_offset if (last_year and year == int(last_year)) else 0
        last_offset = 0  # Reset for subsequent years
        
        log.info(f"[NIH] Fetching fiscal year {year}, starting at offset {offset}")
        
        while not shutdown_requested:
            payload = {
                "criteria": {
                    "fiscal_years": [year],
                    "exclude_subprojects": True,
                },
                "offset": offset,
                "limit": batch_size,
                "sort_field": "project_num",
                "sort_order": "asc"
            }
            
            try:
                resp = requests.post(NIH_REPORTER_SEARCH_URL, json=payload, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.error(f"[NIH] API error at year={year} offset={offset}: {e}")
                time.sleep(5)
                continue
            
            results = data.get('results', [])
            if not results:
                break
            
            batch = []
            for project in results:
                award = _nih_project_to_award(project)
                if award:
                    batch.append(award)
            
            if batch:
                _upsert_awards(cursor, batch)
                conn.commit()
                total_inserted += len(batch)
            
            offset += batch_size
            save_progress('nih_reporter', 'last_year', str(year))
            save_progress('nih_reporter', 'last_offset', str(offset))
            
            if len(results) < batch_size:
                break
            
            time.sleep(RATE_LIMITS['nih_reporter'])
        
        if shutdown_requested:
            break
        
        log.info(f"[NIH] Year {year} complete. Total inserted: {total_inserted}")
    
    cursor.close()
    conn.close()
    log.info(f"[NIH] Done. Total awards ingested: {total_inserted}")
    return total_inserted


def _nih_project_to_award(project: dict) -> Optional[dict]:
    """Convert NIH Reporter project to our awards schema."""
    project_num = project.get('project_num')
    if not project_num:
        return None
    
    # Build a stable unique ID
    fy = project.get('fiscal_year', '')
    unique_id = f"nih:{project_num}:{fy}"
    
    # PI info
    pi_info = None
    pis = project.get('principal_investigators', [])
    if pis:
        lead_pi = pis[0]
        pi_info = {
            'given_name': lead_pi.get('first_name'),
            'family_name': lead_pi.get('last_name'),
            'orcid': lead_pi.get('orcid_id'),
            'affiliation': lead_pi.get('org_name'),
        }
    
    # Institution
    org = project.get('organization', {})
    institution = None
    if org:
        institution = {
            'name': org.get('org_name'),
            'city': org.get('org_city'),
            'state': org.get('org_state'),
            'country': org.get('org_country'),
        }
    
    # Funding amount = direct + indirect costs
    amount = None
    direct = project.get('direct_cost_amt')
    indirect = project.get('indirect_cost_amt')
    if direct is not None:
        amount = (direct or 0) + (indirect or 0)
    elif project.get('award_amount'):
        amount = project['award_amount']
    
    # Activity code determines funding type
    activity = project.get('activity_code', '')
    funding_type = _nih_activity_to_type(activity)
    
    return {
        'source_id': unique_id,
        'display_name': project.get('project_title'),
        'description': project.get('abstract_text'),
        'funder_award_id': project_num,
        'funder_name': 'National Institutes of Health',
        'funder_country': 'US',
        'funder_crossref_id': '100000002',  # CrossRef funder ID for NIH
        'amount': amount,
        'currency': 'USD',
        'funding_type': funding_type,
        'funder_scheme': f"{activity} - {project.get('activity_code_desc', '')}".strip(' -'),
        'start_date': project.get('project_start_date'),
        'end_date': project.get('project_end_date'),
        'start_year': project.get('budget_start_date', '')[:4] if project.get('budget_start_date') else fy,
        'end_year': project.get('budget_end_date', '')[:4] if project.get('budget_end_date') else None,
        'provenance': 'nih_reporter',
        'lead_investigator': pi_info,
        'investigators': [
            {'given_name': p.get('first_name'), 'family_name': p.get('last_name'), 'orcid': p.get('orcid_id')}
            for p in pis
        ] if len(pis) > 1 else None,
        'institution_awarded': [institution] if institution else None,
        'landing_page_url': f"https://reporter.nih.gov/search/results?projectNum={project_num}",
        'topics_raw': project.get('terms', ''),  # Semicolon-separated terms
    }


def _nih_activity_to_type(activity: str) -> str:
    """Map NIH activity codes to funding types."""
    if activity.startswith('R'):
        return 'research'
    elif activity.startswith('K'):
        return 'fellowship'
    elif activity.startswith('T') or activity.startswith('F'):
        return 'training'
    elif activity.startswith('P'):
        return 'center'
    elif activity.startswith('U'):
        return 'cooperative_agreement'
    elif activity.startswith('S'):
        return 'instrumentation'
    return 'other'


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE 2: NSF AWARDS
# https://www.research.gov/awardapi-service/v1/awards.json
# Complete US NSF data: amounts, PIs, institutions, abstracts
# ═══════════════════════════════════════════════════════════════════════════════

NSF_API_URL = "https://api.nsf.gov/services/v1/awards.json"

def ingest_nsf_awards(date_range: tuple = None, batch_size: int = 25):
    """
    Ingest NSF grants from NSF Award Search API.
    
    NSF API provides:
    - Exact award amounts
    - Full abstracts
    - PI names and emails
    - Institution details
    - Program element/reference codes
    - Start/end dates
    - ~600K total awards
    
    API returns max 25 records per request. Paginate by date ranges.
    """
    if date_range is None:
        # Default: 2000 to present
        date_range = ('01/01/2000', '12/31/2026')
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Resume
    last_offset = int(load_progress('nsf', 'last_offset') or '0')
    
    total_inserted = 0
    offset = last_offset
    
    while not shutdown_requested:
        params = {
            'dateStart': date_range[0],
            'dateEnd': date_range[1],
            'printFields': 'id,title,abstractText,amount,startDate,expDate,piFirstName,piLastName,piEmail,pdPIName,coPDPI,awardeeName,awardeeCity,awardeeStateCode,awardeeCountryCode,fundProgramName,primaryProgram,transType,awardee,poName,publicationResearch',
            'offset': offset,
            'rpp': batch_size,
        }
        
        try:
            resp = requests.get(NSF_API_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error(f"[NSF] API error at offset={offset}: {e}")
            time.sleep(5)
            continue
        
        awards_data = data.get('response', {}).get('award', [])
        if not awards_data:
            break
        
        batch = []
        for nsf_award in awards_data:
            award = _nsf_award_to_award(nsf_award)
            if award:
                batch.append(award)
        
        if batch:
            _upsert_awards(cursor, batch)
            conn.commit()
            total_inserted += len(batch)
        
        offset += batch_size
        save_progress('nsf', 'last_offset', str(offset))
        
        if len(awards_data) < batch_size:
            break
        
        time.sleep(RATE_LIMITS['nsf'])
        
        if total_inserted % 1000 == 0:
            log.info(f"[NSF] Progress: {total_inserted} awards ingested")
    
    cursor.close()
    conn.close()
    log.info(f"[NSF] Done. Total awards ingested: {total_inserted}")
    return total_inserted


def _nsf_award_to_award(nsf: dict) -> Optional[dict]:
    """Convert NSF award to our schema."""
    award_id = nsf.get('id')
    if not award_id:
        return None
    
    unique_id = f"nsf:{award_id}"
    
    pi_info = {
        'given_name': nsf.get('piFirstName'),
        'family_name': nsf.get('piLastName'),
        'email': nsf.get('piEmail'),
        'affiliation': nsf.get('awardeeName'),
    }
    
    institution = {
        'name': nsf.get('awardeeName'),
        'city': nsf.get('awardeeCity'),
        'state': nsf.get('awardeeStateCode'),
        'country': nsf.get('awardeeCountryCode', 'US'),
    }
    
    # Co-PIs
    co_pis = []
    if nsf.get('coPDPI'):
        for name in nsf['coPDPI'].split(';'):
            parts = name.strip().split(',')
            if len(parts) >= 2:
                co_pis.append({'family_name': parts[0].strip(), 'given_name': parts[1].strip()})
    
    # Parse dates (MM/DD/YYYY format)
    start_date = _parse_nsf_date(nsf.get('startDate'))
    end_date = _parse_nsf_date(nsf.get('expDate'))
    
    # Map transaction type to funding type
    trans_type = nsf.get('transType', '')
    funding_type = 'cooperative_agreement' if 'Cooperative' in trans_type else 'research'
    
    return {
        'source_id': unique_id,
        'display_name': nsf.get('title'),
        'description': nsf.get('abstractText'),
        'funder_award_id': award_id,
        'funder_name': 'National Science Foundation',
        'funder_country': 'US',
        'funder_crossref_id': '100000001',  # CrossRef funder ID for NSF
        'amount': float(nsf['amount']) if nsf.get('amount') else None,
        'currency': 'USD',
        'funding_type': funding_type,
        'funder_scheme': nsf.get('fundProgramName') or nsf.get('primaryProgram'),
        'start_date': start_date,
        'end_date': end_date,
        'start_year': start_date[:4] if start_date else None,
        'end_year': end_date[:4] if end_date else None,
        'provenance': 'nsf',
        'lead_investigator': pi_info,
        'investigators': co_pis if co_pis else None,
        'institution_awarded': [institution],
        'landing_page_url': f"https://www.nsf.gov/awardsearch/showAward?AWD_ID={award_id}",
    }


def _parse_nsf_date(date_str: str) -> Optional[str]:
    """Parse NSF date (MM/DD/YYYY) to ISO format."""
    if not date_str:
        return None
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
    except:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE 3: EUROPE PMC GRANTS
# https://www.ebi.ac.uk/europepmc/GristAPI/
# European grants with DIRECT paper linkages
# ═══════════════════════════════════════════════════════════════════════════════

EPMC_GRIST_URL = "https://www.ebi.ac.uk/europepmc/GristAPI/rest/get/query="

def ingest_europe_pmc_grants(funders: List[str] = None, batch_size: int = 100):
    """
    Ingest grants from Europe PMC GRIST (Grant Research Intelligence Support Tool).
    
    Europe PMC has grants linked directly to PubMed/PMC publications.
    Key funders: Wellcome Trust, MRC, BBSRC, EPSRC, ERC, Horizon 2020, etc.
    
    GRIST provides:
    - Grant ID, title, abstract
    - Funder name
    - PI details
    - Institution
    - Linked PubMed IDs (direct paper↔grant linkage!)
    """
    if funders is None:
        funders = [
            'WT',        # Wellcome Trust
            'MRC',       # Medical Research Council
            'BBSRC',     # Biotechnology & Biological Sciences
            'EPSRC',     # Engineering & Physical Sciences
            'NERC',      # Natural Environment
            'ESRC',      # Economic & Social
            'ERC',       # European Research Council
            'EC',        # European Commission
            'CIHR',      # Canadian Institutes of Health Research
            'NHMRC',     # National Health & Medical Research Council (AU)
        ]
    
    conn = get_connection()
    cursor = conn.cursor()
    total_inserted = 0
    
    for funder_code in funders:
        if shutdown_requested:
            break
        
        last_page = int(load_progress('europe_pmc', f'{funder_code}_page') or '1')
        page = last_page
        
        log.info(f"[EPMC] Fetching grants for funder: {funder_code}, page {page}")
        
        while not shutdown_requested:
            url = f"{EPMC_GRIST_URL}gid:{funder_code}*&resultType=core&pageSize={batch_size}&page={page}&format=json"
            
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.error(f"[EPMC] Error fetching {funder_code} page {page}: {e}")
                time.sleep(5)
                continue
            
            records = data.get('RecordList', {}).get('Record', [])
            if not records:
                break
            
            batch = []
            for record in records:
                award = _epmc_record_to_award(record, funder_code)
                if award:
                    batch.append(award)
            
            if batch:
                _upsert_awards(cursor, batch)
                conn.commit()
                total_inserted += len(batch)
            
            page += 1
            save_progress('europe_pmc', f'{funder_code}_page', str(page))
            
            if len(records) < batch_size:
                break
            
            time.sleep(RATE_LIMITS['europe_pmc'])
        
        log.info(f"[EPMC] {funder_code} complete. Running total: {total_inserted}")
    
    cursor.close()
    conn.close()
    log.info(f"[EPMC] Done. Total awards ingested: {total_inserted}")
    return total_inserted


def _epmc_record_to_award(record: dict, funder_code: str) -> Optional[dict]:
    """Convert Europe PMC GRIST record to our schema."""
    grant_info = record.get('Grant', {})
    grant_id = grant_info.get('Id')
    if not grant_id:
        return None
    
    unique_id = f"epmc:{funder_code}:{grant_id}"
    
    # Funder name mapping
    funder_names = {
        'WT': 'Wellcome Trust',
        'MRC': 'Medical Research Council',
        'BBSRC': 'Biotechnology and Biological Sciences Research Council',
        'EPSRC': 'Engineering and Physical Sciences Research Council',
        'NERC': 'Natural Environment Research Council',
        'ESRC': 'Economic and Social Research Council',
        'ERC': 'European Research Council',
        'EC': 'European Commission',
        'CIHR': 'Canadian Institutes of Health Research',
        'NHMRC': 'National Health and Medical Research Council',
    }
    
    pi_info = None
    pi = record.get('PI', {})
    if pi:
        pi_info = {
            'given_name': pi.get('ForeName'),
            'family_name': pi.get('LastName'),
            'orcid': pi.get('ORCID'),
            'affiliation': pi.get('Affiliation'),
        }
    
    institution = None
    inst = record.get('Institution', {})
    if inst:
        institution = {
            'name': inst.get('Name'),
            'city': inst.get('City'),
            'country': inst.get('Country'),
        }
    
    return {
        'source_id': unique_id,
        'display_name': grant_info.get('Title'),
        'description': grant_info.get('Abstract'),
        'funder_award_id': grant_id,
        'funder_name': funder_names.get(funder_code, funder_code),
        'funder_country': _funder_country(funder_code),
        'amount': None,  # EPMC doesn't always include amounts
        'currency': None,
        'funding_type': 'research',
        'funder_scheme': grant_info.get('Stream'),
        'start_date': grant_info.get('StartDate'),
        'end_date': grant_info.get('EndDate'),
        'start_year': grant_info.get('StartDate', '')[:4] if grant_info.get('StartDate') else None,
        'end_year': grant_info.get('EndDate', '')[:4] if grant_info.get('EndDate') else None,
        'provenance': 'europe_pmc',
        'lead_investigator': pi_info,
        'institution_awarded': [institution] if institution else None,
        # EPMC gives us linked PubMed IDs — extremely valuable for paper linkage
        'linked_pmids': [
            pub.get('PMID') for pub in record.get('PublicationList', {}).get('Publication', [])
            if pub.get('PMID')
        ],
    }


def _funder_country(code: str) -> str:
    """Map funder codes to country."""
    uk_funders = {'WT', 'MRC', 'BBSRC', 'EPSRC', 'NERC', 'ESRC'}
    if code in uk_funders:
        return 'GB'
    elif code in ('ERC', 'EC'):
        return 'EU'
    elif code == 'CIHR':
        return 'CA'
    elif code == 'NHMRC':
        return 'AU'
    return 'XX'


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE 4: CROSSREF FUNDER REGISTRY
# https://api.crossref.org/funders/{id}/works
# Links DOIs to funders — fills the paper↔funder gap
# ═══════════════════════════════════════════════════════════════════════════════

CROSSREF_API = "https://api.crossref.org"
CROSSREF_EMAIL = "admin@nobleblocks.com"  # For polite pool

def ingest_crossref_funder_links(top_n_funders: int = 200):
    """
    Use CrossRef to link papers to funders.
    
    CrossRef stores funder acknowledgment metadata from publishers.
    When a paper is published, the publisher reports which funder(s) paid.
    This gives us the DOI → funder → award_id linkage.
    
    Strategy:
    - Get top funders by works count
    - For each, fetch their funded works
    - Store the DOI→funder→award_id links in funding_edges
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get top funders from CrossRef
    log.info(f"[CrossRef] Fetching top {top_n_funders} funders")
    
    try:
        resp = requests.get(
            f"{CROSSREF_API}/funders",
            params={'rows': top_n_funders, 'sort': 'works', 'order': 'desc',
                    'mailto': CROSSREF_EMAIL},
            timeout=30
        )
        resp.raise_for_status()
        funders = resp.json()['message']['items']
    except Exception as e:
        log.error(f"[CrossRef] Failed to fetch funders list: {e}")
        return 0
    
    total_links = 0
    
    for funder in funders:
        if shutdown_requested:
            break
        
        funder_id = funder['id']
        funder_name = funder['name']
        funder_doi = funder.get('doi-asserted-by')
        
        last_cursor = load_progress('crossref', f'funder_{funder_id}_cursor')
        
        log.info(f"[CrossRef] Processing funder: {funder_name} ({funder_id})")
        
        cursor_mark = last_cursor or '*'
        batch_count = 0
        max_per_funder = 10000  # Cap per funder to avoid spending days on NIH
        
        while not shutdown_requested and batch_count < max_per_funder:
            try:
                resp = requests.get(
                    f"{CROSSREF_API}/funders/{funder_id}/works",
                    params={
                        'rows': 100,
                        'cursor': cursor_mark,
                        'select': 'DOI,funder,title,published-print,published-online',
                        'mailto': CROSSREF_EMAIL,
                    },
                    timeout=30
                )
                resp.raise_for_status()
                data = resp.json()['message']
            except Exception as e:
                log.error(f"[CrossRef] Error for funder {funder_id}: {e}")
                time.sleep(5)
                continue
            
            items = data.get('items', [])
            if not items:
                break
            
            # Store DOI→funder links in funding_edges
            links = []
            for item in items:
                doi = item.get('DOI')
                if not doi:
                    continue
                
                # Extract specific award numbers from funder metadata
                for f in item.get('funder', []):
                    if f.get('DOI') and funder_id in f['DOI']:
                        award_ids = f.get('award', [])
                        for award_id in award_ids:
                            links.append({
                                'doi': doi,
                                'funder_crossref_id': funder_id,
                                'funder_name': funder_name,
                                'award_id': award_id,
                            })
                        if not award_ids:
                            links.append({
                                'doi': doi,
                                'funder_crossref_id': funder_id,
                                'funder_name': funder_name,
                                'award_id': None,
                            })
            
            if links:
                _upsert_crossref_links(cursor, links)
                conn.commit()
                total_links += len(links)
                batch_count += len(items)
            
            cursor_mark = data.get('next-cursor')
            if not cursor_mark:
                break
            
            save_progress('crossref', f'funder_{funder_id}_cursor', cursor_mark)
            time.sleep(RATE_LIMITS['crossref'])
        
        log.info(f"[CrossRef] {funder_name}: {batch_count} works processed")
    
    cursor.close()
    conn.close()
    log.info(f"[CrossRef] Done. Total DOI→funder links: {total_links}")
    return total_links


def _upsert_crossref_links(cursor, links: List[dict]):
    """Insert CrossRef DOI→funder links into funding_edges."""
    # First resolve DOIs to paper_ids
    sql = """
    INSERT INTO paper_grants (paper_id, grant_id, funder, funder_id, source)
    SELECT p.id, %(award_id)s, %(funder_name)s, f.id, 'crossref'
    FROM papers p
    LEFT JOIN funders f ON f.crossref_id = %(funder_crossref_id)s
    WHERE p.doi = %(doi)s
    ON CONFLICT DO NOTHING
    """
    # Batch execute
    for link in links:
        try:
            cursor.execute(sql, link)
        except Exception:
            pass  # Skip conflicts silently


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE 5: UKRI GATEWAY TO RESEARCH
# https://gtr.ukri.org/gtr/api/
# All UK Research Council grants (EPSRC, BBSRC, MRC, AHRC, ESRC, NERC, STFC)
# ═══════════════════════════════════════════════════════════════════════════════

UKRI_API = "https://gtr.ukri.org/gtr/api"

def ingest_ukri_grants(batch_size: int = 100):
    """
    Ingest grants from UKRI Gateway to Research.
    
    UKRI provides:
    - Full grant amounts in GBP
    - Detailed abstracts
    - PI and co-I details
    - Institutional links
    - Research outputs (publications, datasets, etc.)
    - ~200K grants total
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    last_page = int(load_progress('ukri', 'last_page') or '1')
    total_inserted = 0
    page = last_page
    
    while not shutdown_requested:
        url = f"{UKRI_API}/projects"
        params = {
            'p': page,
            's': batch_size,
            'f': 'pro.gr',  # Funded projects with grants
        }
        headers = {'Accept': 'application/json'}
        
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error(f"[UKRI] Error at page {page}: {e}")
            time.sleep(5)
            continue
        
        projects = data.get('project', [])
        if not projects:
            break
        
        batch = []
        for project in projects:
            award = _ukri_project_to_award(project)
            if award:
                batch.append(award)
        
        if batch:
            _upsert_awards(cursor, batch)
            conn.commit()
            total_inserted += len(batch)
        
        page += 1
        save_progress('ukri', 'last_page', str(page))
        
        total_pages = data.get('totalPages', 0)
        if page > total_pages:
            break
        
        time.sleep(RATE_LIMITS['ukri'])
        
        if total_inserted % 1000 == 0:
            log.info(f"[UKRI] Progress: {total_inserted}/{total_pages * batch_size} estimated")
    
    cursor.close()
    conn.close()
    log.info(f"[UKRI] Done. Total awards ingested: {total_inserted}")
    return total_inserted


def _ukri_project_to_award(project: dict) -> Optional[dict]:
    """Convert UKRI project to our schema."""
    project_id = project.get('id')
    if not project_id:
        return None
    
    unique_id = f"ukri:{project_id}"
    
    # Determine which research council
    fund_info = project.get('fund', {})
    funder_name = fund_info.get('funder', {}).get('name', 'UKRI')
    
    # UKRI amounts in GBP (valuePounds)
    amount = fund_info.get('valuePounds')
    
    # PI info
    pi_info = None
    pi_link = project.get('principalInvestigator', {})
    if pi_link:
        pi_info = {
            'given_name': pi_link.get('firstName'),
            'family_name': pi_link.get('surname'),
            'orcid': pi_link.get('orcidId'),
        }
    
    return {
        'source_id': unique_id,
        'display_name': project.get('title'),
        'description': project.get('abstractText'),
        'funder_award_id': project.get('grantReference'),
        'funder_name': funder_name,
        'funder_country': 'GB',
        'amount': float(amount) if amount else None,
        'currency': 'GBP',
        'funding_type': _ukri_category_to_type(project.get('grantCategory', '')),
        'funder_scheme': project.get('grantCategory'),
        'start_date': project.get('fund', {}).get('start'),
        'end_date': project.get('fund', {}).get('end'),
        'start_year': project.get('fund', {}).get('start', '')[:4] if project.get('fund', {}).get('start') else None,
        'end_year': project.get('fund', {}).get('end', '')[:4] if project.get('fund', {}).get('end') else None,
        'provenance': 'ukri',
        'lead_investigator': pi_info,
        'institution_awarded': None,
        'landing_page_url': f"https://gtr.ukri.org/projects?ref={project.get('grantReference', '')}",
    }


def _ukri_category_to_type(category: str) -> str:
    """Map UKRI grant categories."""
    cat = category.lower()
    if 'fellowship' in cat:
        return 'fellowship'
    elif 'studentship' in cat or 'training' in cat:
        return 'training'
    elif 'infrastructure' in cat or 'capital' in cat:
        return 'infrastructure'
    return 'research'


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE 6: DIMENSIONS (FREE TIER)
# https://app.dimensions.ai/
# Limited free API: 50 results per query, good for gap-filling
# ═══════════════════════════════════════════════════════════════════════════════

DIMENSIONS_FREE_URL = "https://app.dimensions.ai/api/dsl.json"

def ingest_dimensions_grants(api_key: str = None):
    """
    Use Dimensions free tier to fill gaps.
    
    Dimensions has the largest grants database (~7M grants) but free tier
    is limited to 50 results per query and limited fields.
    
    Strategy: Query for grants not already in our DB by funder+year combos.
    """
    if not api_key:
        api_key = os.environ.get('DIMENSIONS_API_KEY')
        if not api_key:
            log.warning("[Dimensions] No API key configured, skipping")
            return 0
    
    # Dimensions requires DSL queries
    # Example: search grants where funder_name="European Research Council" return grants[all]
    log.info("[Dimensions] Free tier ingestion - limited to gap-filling")
    # Implementation would use their DSL query language
    # Skipping detailed implementation as it requires paid key for bulk
    return 0


# ═══════════════════════════════════════════════════════════════════════════════
# COMMON: UPSERT LOGIC
# ═══════════════════════════════════════════════════════════════════════════════

def _upsert_awards(cursor, batch: List[dict]):
    """
    Upsert awards from any source into the awards table.
    Uses source_id for deduplication across all sources.
    """
    # We need a composite unique constraint: provenance + funder_award_id
    # Or use a computed source_id column
    sql = """
    INSERT INTO awards (
        openalex_id, display_name, description, funder_award_id,
        funder_openalex, amount, currency, funding_type, funder_scheme,
        start_date, end_date, start_year, end_year,
        landing_page_url, provenance, lead_investigator,
        investigators, institution_awarded, funded_outputs_count
    ) VALUES (
        %(source_id)s, %(display_name)s, %(description)s, %(funder_award_id)s,
        %(funder_name)s, %(amount)s, %(currency)s, %(funding_type)s, %(funder_scheme)s,
        %(start_date)s, %(end_date)s, %(start_year_int)s, %(end_year_int)s,
        %(landing_page_url)s, %(provenance)s, %(lead_investigator_json)s,
        %(investigators_json)s, %(institution_awarded_json)s, 0
    )
    ON CONFLICT (openalex_id) DO UPDATE SET
        display_name = COALESCE(EXCLUDED.display_name, awards.display_name),
        description = COALESCE(EXCLUDED.description, awards.description),
        amount = COALESCE(EXCLUDED.amount, awards.amount),
        currency = COALESCE(EXCLUDED.currency, awards.currency),
        lead_investigator = COALESCE(EXCLUDED.lead_investigator, awards.lead_investigator),
        updated_at = NOW()
    """
    
    for award in batch:
        params = {
            'source_id': award['source_id'],
            'display_name': award.get('display_name'),
            'description': award.get('description'),
            'funder_award_id': award.get('funder_award_id'),
            'funder_name': award.get('funder_name'),
            'amount': award.get('amount'),
            'currency': award.get('currency'),
            'funding_type': award.get('funding_type'),
            'funder_scheme': award.get('funder_scheme'),
            'start_date': award.get('start_date'),
            'end_date': award.get('end_date'),
            'start_year_int': int(award['start_year']) if award.get('start_year') else None,
            'end_year_int': int(award['end_year']) if award.get('end_year') else None,
            'landing_page_url': award.get('landing_page_url'),
            'provenance': award.get('provenance'),
            'lead_investigator_json': json.dumps(award['lead_investigator']) if award.get('lead_investigator') else None,
            'investigators_json': json.dumps(award['investigators']) if award.get('investigators') else None,
            'institution_awarded_json': json.dumps(award['institution_awarded']) if award.get('institution_awarded') else None,
        }
        try:
            cursor.execute(sql, params)
        except Exception as e:
            log.debug(f"Upsert error for {award.get('source_id')}: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

def run_all_sources():
    """Run all source ingestors in priority order."""
    results = {}
    
    log.info("=" * 60)
    log.info("MULTI-SOURCE FUNDING INGESTION - Starting")
    log.info("=" * 60)
    
    # Priority 1: NIH Reporter (most complete US biomedical data)
    if not shutdown_requested:
        log.info("\n[1/5] NIH Reporter...")
        results['nih_reporter'] = ingest_nih_reporter(fiscal_years=list(range(2015, 2027)))
    
    # Priority 2: NSF (complete US STEM)
    if not shutdown_requested:
        log.info("\n[2/5] NSF Awards...")
        results['nsf'] = ingest_nsf_awards(date_range=('01/01/2015', '12/31/2026'))
    
    # Priority 3: Europe PMC (EU/UK grants with paper links)
    if not shutdown_requested:
        log.info("\n[3/5] Europe PMC Grants...")
        results['europe_pmc'] = ingest_europe_pmc_grants()
    
    # Priority 4: UKRI (UK research councils, full GBP amounts)
    if not shutdown_requested:
        log.info("\n[4/5] UKRI Gateway to Research...")
        results['ukri'] = ingest_ukri_grants()
    
    # Priority 5: CrossRef funder links (DOI→funder linkage)
    if not shutdown_requested:
        log.info("\n[5/5] CrossRef Funder Links...")
        results['crossref'] = ingest_crossref_funder_links(top_n_funders=100)
    
    log.info("\n" + "=" * 60)
    log.info("INGESTION COMPLETE")
    log.info("=" * 60)
    for source, count in results.items():
        log.info(f"  {source}: {count:,} records")
    log.info(f"  TOTAL: {sum(results.values()):,}")
    
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# COVERAGE COMPARISON (for documentation/strategy)
# ═══════════════════════════════════════════════════════════════════════════════
"""
SOURCE COMPARISON — Why we need all of them:

┌─────────────────┬──────────┬────────┬──────────┬───────────┬────────────┐
│ Source          │ Records  │ $ Amts │ Paper→   │ Abstracts │ Rate Limit │
│                 │          │        │ Grant    │           │            │
├─────────────────┼──────────┼────────┼──────────┼───────────┼────────────┤
│ OpenAlex        │ 13.8M    │ ~30%   │ Yes      │ Some      │ 10/sec     │
│ NIH Reporter    │ ~5M      │ 100%   │ PubMed   │ 100%      │ No formal  │
│ NSF Awards      │ ~600K    │ 100%   │ Some     │ 100%      │ No formal  │
│ Europe PMC      │ ~2M      │ Rare   │ PubMed   │ Some      │ 3/sec      │
│ CrossRef        │ 150M DOIs│ N/A    │ DOI→fund │ N/A       │ 50/sec     │
│ UKRI            │ ~200K    │ 100%   │ Links    │ 100%      │ No formal  │
│ Dimensions      │ ~7M      │ ~60%   │ Yes      │ Some      │ Paid/$$$   │
└─────────────────┴──────────┴────────┴──────────┴───────────┴────────────┘

DEDUPLICATION STRATEGY:
- Each source has unique provenance prefix (nih:, nsf:, ukri:, epmc:, oa:)
- Cross-source matching by: funder_award_id (grant number), DOI, PI name+year
- When same grant found in multiple sources → merge, keep richest data
- Priority for amount: NIH/NSF/UKRI (always exact) > Dimensions > OpenAlex

WHAT OPENALEX LOCKS/MISSES:
1. Award amounts: Only 30% have them, and they acknowledged this is incomplete
2. Award abstracts: Often missing
3. PI details: Limited to name, no email/ORCID
4. Rate limits: 10 req/sec, could tighten
5. Bulk data: CC0 but could restrict at any time
6. Japanese KAKEN: They have it, but only through provenance
7. Real-time updates: Daily bulk, but API lags

WHAT WE ADD BY GOING MULTI-SOURCE:
1. 100% of US federal funding amounts (NIH + NSF = $50B+/year)
2. 100% of UK funding amounts (UKRI = £8B+/year)
3. Direct paper↔grant links from Europe PMC (not just inferred)
4. CrossRef publisher-reported funding acknowledgments (most reliable link)
5. No single point of failure — if OpenAlex restricts, we keep operating
"""

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Multi-source funding data ingestor')
    parser.add_argument('--source', choices=['nih', 'nsf', 'epmc', 'ukri', 'crossref', 'all'],
                       default='all', help='Which source to ingest')
    parser.add_argument('--years', type=str, default='2015-2026',
                       help='Year range (e.g., 2015-2026)')
    
    args = parser.parse_args()
    
    if args.source == 'all':
        run_all_sources()
    elif args.source == 'nih':
        years = list(range(*[int(x) for x in args.years.split('-')]))
        ingest_nih_reporter(fiscal_years=years)
    elif args.source == 'nsf':
        parts = args.years.split('-')
        ingest_nsf_awards(date_range=(f'01/01/{parts[0]}', f'12/31/{parts[1]}'))
    elif args.source == 'epmc':
        ingest_europe_pmc_grants()
    elif args.source == 'ukri':
        ingest_ukri_grants()
    elif args.source == 'crossref':
        ingest_crossref_funder_links()
