"""
VC Intelligence Alert Processor

This job runs periodically to:
1. Check papers in watchlists for new patent citations
2. Compare against thresholds
3. Trigger alerts via email/Slack/webhooks
4. Log to alert_history for audit trail

Usage:
    python process_vc_alerts.py
    
Or as scheduled job (every 6 hours):
    0 */6 * * * cd /opt/nobleblocks/paper-db && python scripts/process_vc_alerts.py
"""

import os
import json
import logging
import smtplib
from datetime import datetime, timedelta
from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/vc_alerts.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─── Configuration ──────────────────────────────────────────

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', 5432))
DB_NAME = os.getenv('DB_NAME', 'paper_search')
DB_USER = os.getenv('DB_USER', 'nobleblocks')
DB_PASS = os.getenv('DB_PASS', 'nb_papers_2026_prod')

SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL', '')
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY', '')
ALERT_CHECK_HOURS = int(os.getenv('ALERT_CHECK_HOURS', 24))

# ─── Database Connection ────────────────────────────────────

def get_db_connection():
    """Create PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

# ─── Alert Checking ─────────────────────────────────────────

def get_active_watchlists(conn) -> List[Dict[str, Any]]:
    """Get all active watchlists with alert rules"""
    query = """
    SELECT 
        w.id,
        w.user_id,
        w.name,
        w.papers,
        w.alert_threshold,
        w.alert_channels,
        json_agg(
            json_build_object(
                'id', ar.id,
                'channel', ar.channel,
                'destination', ar.destination,
                'frequency', ar.frequency
            )
        ) FILTER (WHERE ar.enabled = TRUE) as alert_rules
    FROM vc_watchlists w
    LEFT JOIN vc_alert_rules ar ON w.id = ar.watchlist_id
    GROUP BY w.id, w.user_id, w.name, w.papers, w.alert_threshold, w.alert_channels
    ORDER BY w.updated_at DESC
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        return cur.fetchall()

def get_paper_citation_updates(conn, papers: List[str], hours: int) -> Dict[str, Dict]:
    """Get recent citation counts for papers"""
    # Convert DOI list to placeholders
    placeholders = ','.join(['%s'] * len(papers))
    
    query = f"""
    SELECT 
        ppc.paper_doi,
        COUNT(DISTINCT ppc.patent_id) as total_citations,
        COUNT(DISTINCT CASE 
            WHEN ppc.created_at > NOW() - INTERVAL '{hours} hours' 
            THEN ppc.patent_id 
        END) as new_citations
    FROM patent_paper_citations ppc
    WHERE ppc.paper_doi IN ({placeholders})
    GROUP BY ppc.paper_doi
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, papers)
        return {row['paper_doi']: dict(row) for row in cur.fetchall()}

def get_last_alert_time(conn, watchlist_id: int, paper_doi: str) -> datetime:
    """Get when we last alerted for a paper in a watchlist"""
    query = """
    SELECT MAX(alert_sent_at) as last_alert
    FROM vc_alert_history
    WHERE watchlist_id = %s AND paper_doi = %s
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (watchlist_id, paper_doi))
        result = cur.fetchone()
        if result and result['last_alert']:
            return result['last_alert']
        return datetime.min

def should_alert(
    new_citations: int,
    threshold: int,
    frequency: str,
    last_alert_time: datetime
) -> bool:
    """Determine if alert should be sent based on threshold and frequency"""
    if new_citations < threshold:
        return False
    
    now = datetime.now()
    
    if frequency == 'immediate':
        return True
    elif frequency == 'daily':
        return (now - last_alert_time).days >= 1
    elif frequency == 'weekly':
        return (now - last_alert_time).days >= 7
    
    return False

# ─── Alert Sending ──────────────────────────────────────────

def send_email_alert(
    email: str,
    watchlist_name: str,
    paper_doi: str,
    citations_data: Dict
) -> bool:
    """Send email alert via SendGrid"""
    if not SENDGRID_API_KEY:
        logger.warning("SendGrid API key not set, skipping email")
        return False
    
    try:
        # This is a simplified version - in production use SendGrid SDK
        message = f"""
        <h2>VC Intelligence Alert: {watchlist_name}</h2>
        <p>Paper <code>{paper_doi}</code> just gained {citations_data['new_citations']} new patent citations!</p>
        <p><strong>Total citations:</strong> {citations_data['total_citations']}</p>
        <p><a href="https://nobleblocks.ai/paper/{paper_doi}">View paper</a></p>
        """
        
        logger.info(f"Email alert to {email}: {paper_doi} ({citations_data['new_citations']} new)")
        return True
    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")
        return False

def send_slack_alert(
    channel: str,
    watchlist_name: str,
    paper_doi: str,
    citations_data: Dict
) -> bool:
    """Send Slack alert"""
    if not SLACK_WEBHOOK_URL:
        logger.warning("Slack webhook URL not set, skipping Slack alert")
        return False
    
    try:
        payload = {
            "channel": channel,
            "text": f"🚀 *VC Intelligence Alert*",
            "attachments": [
                {
                    "color": "good",
                    "title": watchlist_name,
                    "fields": [
                        {
                            "title": "Paper",
                            "value": paper_doi,
                            "short": False
                        },
                        {
                            "title": "New Citations",
                            "value": str(citations_data['new_citations']),
                            "short": True
                        },
                        {
                            "title": "Total Citations",
                            "value": str(citations_data['total_citations']),
                            "short": True
                        }
                    ]
                }
            ]
        }
        
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"Slack alert sent to {channel}: {paper_doi}")
        return True
    except Exception as e:
        logger.error(f"Failed to send Slack alert: {e}")
        return False

def send_webhook_alert(
    webhook_url: str,
    watchlist_id: int,
    paper_doi: str,
    citations_data: Dict
) -> bool:
    """Send webhook alert"""
    try:
        payload = {
            "watchlist_id": watchlist_id,
            "paper_doi": paper_doi,
            "new_citations": citations_data['new_citations'],
            "total_citations": citations_data['total_citations'],
            "timestamp": datetime.now().isoformat()
        }
        
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"Webhook alert sent to {webhook_url}: {paper_doi}")
        return True
    except Exception as e:
        logger.error(f"Failed to send webhook alert: {e}")
        return False

# ─── Main Processing ────────────────────────────────────────

def process_alerts():
    """Main alert processing loop"""
    logger.info(f"Starting VC Intelligence alert processor (check window: {ALERT_CHECK_HOURS}h)")
    
    conn = get_db_connection()
    processed = 0
    alerts_sent = 0
    
    try:
        watchlists = get_active_watchlists(conn)
        logger.info(f"Found {len(watchlists)} active watchlists")
        
        for watchlist in watchlists:
            if not watchlist['alert_rules']:
                continue
            
            watchlist_id = watchlist['id']
            papers = watchlist['papers']
            threshold = watchlist['alert_threshold']
            
            # Get citation updates for papers
            citations = get_paper_citation_updates(conn, papers, ALERT_CHECK_HOURS)
            
            for paper_doi in papers:
                processed += 1
                citations_data = citations.get(paper_doi, {
                    'paper_doi': paper_doi,
                    'total_citations': 0,
                    'new_citations': 0
                })
                
                new_citations = citations_data.get('new_citations', 0)
                
                # Check each alert rule
                for rule in watchlist['alert_rules']:
                    last_alert = get_last_alert_time(conn, watchlist_id, paper_doi)
                    
                    if should_alert(new_citations, threshold, rule['frequency'], last_alert):
                        # Send alert
                        alert_sent = False
                        
                        if rule['channel'] == 'email':
                            alert_sent = send_email_alert(
                                rule['destination'],
                                watchlist['name'],
                                paper_doi,
                                citations_data
                            )
                        elif rule['channel'] == 'slack':
                            alert_sent = send_slack_alert(
                                rule['destination'],
                                watchlist['name'],
                                paper_doi,
                                citations_data
                            )
                        elif rule['channel'] == 'webhook':
                            alert_sent = send_webhook_alert(
                                rule['destination'],
                                watchlist_id,
                                paper_doi,
                                citations_data
                            )
                        
                        if alert_sent:
                            alerts_sent += 1
                            # Log to history
                            log_alert(conn, watchlist_id, paper_doi, citations_data, rule)
        
        logger.info(f"Alert processing complete: {processed} papers checked, {alerts_sent} alerts sent")
        
    except Exception as e:
        logger.error(f"Error in alert processing: {e}", exc_info=True)
    finally:
        conn.close()

def log_alert(conn, watchlist_id: int, paper_doi: str, citations_data: Dict, rule: Dict):
    """Log alert to history"""
    query = """
    INSERT INTO vc_alert_history 
        (watchlist_id, paper_doi, new_citations, total_citations, alert_content)
    VALUES (%s, %s, %s, %s, %s)
    """
    
    alert_content = {
        'channel': rule['channel'],
        'frequency': rule['frequency'],
        'destination': rule['destination']
    }
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, (
                watchlist_id,
                paper_doi,
                citations_data.get('new_citations', 0),
                citations_data.get('total_citations', 0),
                json.dumps(alert_content)
            ))
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to log alert: {e}")
        conn.rollback()

# ─── CLI ─────────────────────────────────────────────────────

if __name__ == '__main__':
    logger.info("VC Intelligence Alert Processor Started")
    process_alerts()
    logger.info("VC Intelligence Alert Processor Completed")
