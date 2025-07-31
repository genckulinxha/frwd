import logging
from bs4 import BeautifulSoup, ParserRejectedMarkup
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from models import Law, LawRelation
from pipeline.utils import safe_strip
from urllib.parse import urljoin
import time
from typing import Optional, List

logger = logging.getLogger(__name__)
BASE_URL = "https://gzk.rks-gov.net/"

RELATION_TYPES = [
    "shfuqizon", "ndryshon", "ndryshohet",
    "plotëson", "plotësohet", "ndryshon pjesërisht", "shfuqizon pjesërisht"
]

def backfill_relations(session: Session, batch_size: int = 50):
    """Backfill law relations with enhanced error handling."""
    
    if not session:
        logger.error("❌ No database session provided")
        raise ValueError("Database session is required")
    
    try:
        laws = session.query(Law).filter_by(unprocessed=False).all()
        logger.info(f"🔗 Starting relation backfill for {len(laws)} laws")
        
        if not laws:
            logger.info("✅ No processed laws found for relation backfill")
            return
        
        processed_count = 0
        error_count = 0
        skipped_count = 0
        relations_created = 0
        
        # Process in batches to avoid memory issues
        for i in range(0, len(laws), batch_size):
            batch = laws[i:i + batch_size]
            logger.info(f"📦 Processing batch {i//batch_size + 1} ({len(batch)} laws)")
            
            batch_processed = 0
            batch_errors = 0
            batch_skipped = 0
            batch_relations = 0
            
            for law in batch:
                try:
                    result = process_law_relations(session, law)
                    if result["status"] == "processed":
                        batch_processed += 1
                        processed_count += 1
                        batch_relations += result["relations_count"]
                        relations_created += result["relations_count"]
                    elif result["status"] == "skipped":
                        batch_skipped += 1
                        skipped_count += 1
                    else:
                        batch_errors += 1
                        error_count += 1
                        
                except Exception as e:
                    logger.error(f"❌ Unexpected error processing law ActID={law.act_id}: {e}")
                    logger.exception("Detailed error:")
                    batch_errors += 1
                    error_count += 1
                    
                    # Rollback any pending changes for this law
                    try:
                        session.rollback()
                    except Exception as rollback_e:
                        logger.error(f"❌ Error during rollback for ActID={law.act_id}: {rollback_e}")
                
                # Small delay to avoid overwhelming the server
                time.sleep(0.5)
            
            logger.info(f"   ✅ Batch {i//batch_size + 1} complete: {batch_processed} processed, {batch_errors} errors, {batch_skipped} skipped, {batch_relations} relations")
            
            # Commit batch periodically
            try:
                session.commit()
                logger.debug(f"   💾 Batch {i//batch_size + 1} committed to database")
            except SQLAlchemyError as e:
                logger.error(f"❌ Error committing batch {i//batch_size + 1}: {e}")
                session.rollback()
        
        logger.info(f"✅ Relation backfill complete: {processed_count} processed, {error_count} errors, {skipped_count} skipped, {relations_created} relations created")
        
    except SQLAlchemyError as e:
        logger.error(f"❌ Database error in backfill_relations: {e}")
        session.rollback()
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error in backfill_relations: {e}")
        logger.exception("Detailed error:")
        raise

def process_law_relations(session: Session, law: Law) -> dict:
    """Process relations for a single law with comprehensive error handling."""
    
    try:
        if not law.detail_url:
            logger.warning(f"⚠️ No detail URL for ActID={law.act_id}, skipping")
            return {"status": "skipped", "relations_count": 0}
        
        logger.debug(f"🔍 Processing relations for ActID={law.act_id}")
        
        # Fetch HTML content with retries
        html_content = fetch_html_with_retries(law.detail_url, max_retries=3)
        if not html_content:
            logger.error(f"❌ Failed to fetch HTML for ActID={law.act_id}")
            return {"status": "error", "relations_count": 0}
        
        # Parse HTML content
        try:
            soup = BeautifulSoup(html_content, "html.parser")
        except ParserRejectedMarkup as e:
            logger.error(f"❌ HTML parsing failed for ActID={law.act_id}: {e}")
            return {"status": "error", "relations_count": 0}
        
        # Extract relations
        relations_count = 0
        try:
            container = soup.select_one("#MainContent_drNActRelated")
            if not container:
                logger.debug(f"   ℹ️ No relations container found for ActID={law.act_id}")
                return {"status": "processed", "relations_count": 0}

            boxes = container.select("div.act_link_box_1")
            logger.debug(f"   📋 Found {len(boxes)} potential relations for ActID={law.act_id}")
            
            for box in boxes:
                try:
                    relation_result = process_relation_box(session, law, box)
                    if relation_result:
                        relations_count += 1
                        logger.debug(f"   ↪️ Created relation: {relation_result}")
                        
                except Exception as e:
                    logger.warning(f"   ⚠️ Error processing relation box for ActID={law.act_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Error extracting relations for ActID={law.act_id}: {e}")
            return {"status": "error", "relations_count": 0}

        # Commit changes for this law
        try:
            session.commit()
            logger.debug(f"   ✅ Relations committed for ActID={law.act_id}: {relations_count} relations")
            return {"status": "processed", "relations_count": relations_count}
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Database error saving relations for ActID={law.act_id}: {e}")
            session.rollback()
            return {"status": "error", "relations_count": 0}
            
    except Exception as e:
        logger.error(f"❌ Failed to process relations for ActID={law.act_id}: {e}")
        logger.exception("Detailed error:")
        session.rollback()
        return {"status": "error", "relations_count": 0}

def process_relation_box(session: Session, source_law: Law, box) -> Optional[str]:
    """Process a single relation box."""
    
    try:
        # Extract link
        link = box.select_one("a[href*='ActID=']")
        if not link:
            logger.debug("   ⚠️ No link found in relation box")
            return None
            
        href = link.get("href")
        if not href:
            logger.debug("   ⚠️ No href found in relation link")
            return None
            
        related_act_id = extract_act_id(href)
        if not related_act_id:
            logger.debug(f"   ⚠️ Could not extract act_id from href: {href}")
            return None

        # Check/create target law record
        target_law = session.query(Law).filter_by(act_id=related_act_id).first()
        if not target_law:
            try:
                target_law = Law(
                    act_id=related_act_id,
                    detail_url=urljoin(BASE_URL, href),
                    category=source_law.category,
                    unprocessed=True
                )
                session.add(target_law)
                session.flush()  # Get the ID
                logger.debug(f"   [+] Created new target law ActID={related_act_id}")
                
            except IntegrityError as e:
                logger.warning(f"   ⚠️ Integrity error creating target law ActID={related_act_id}: {e}")
                session.rollback()
                # Try to find the existing record
                target_law = session.query(Law).filter_by(act_id=related_act_id).first()
                if not target_law:
                    logger.error(f"   ❌ Could not find or create target law ActID={related_act_id}")
                    return None

        # Extract relation type
        try:
            label_elem = box.select_one("span.span_margin")
            if not label_elem:
                logger.debug("   ⚠️ No label element found in relation box")
                return None
                
            label = safe_strip(label_elem).lower()
            if not label:
                logger.debug("   ⚠️ Empty label in relation box")
                return None
                
            relation_type = detect_relation_type(label)
            
        except Exception as e:
            logger.warning(f"   ⚠️ Error extracting relation type: {e}")
            return None

        # Check if relation already exists
        try:
            exists = session.query(LawRelation).filter_by(
                source_id=source_law.id,
                target_id=target_law.id,
                relation_type=relation_type
            ).first()
            
            if exists:
                logger.debug(f"   ℹ️ Relation already exists: {source_law.act_id} {relation_type} {target_law.act_id}")
                return None
                
        except SQLAlchemyError as e:
            logger.error(f"   ❌ Database error checking existing relation: {e}")
            return None

        # Create new relation
        try:
            relation = LawRelation(
                source_id=source_law.id,
                target_id=target_law.id,
                relation_type=relation_type,
                comment=label
            )
            session.add(relation)
            relation_desc = f"{source_law.act_id} {relation_type} {target_law.act_id}"
            logger.info(f"   ↪️ Created relation: {relation_desc}")
            return relation_desc
            
        except IntegrityError as e:
            logger.warning(f"   ⚠️ Integrity error creating relation: {e}")
            session.rollback()
            return None
        except SQLAlchemyError as e:
            logger.error(f"   ❌ Database error creating relation: {e}")
            session.rollback()
            return None
            
    except Exception as e:
        logger.error(f"   ❌ Error processing relation box: {e}")
        return None

def fetch_html_with_retries(url: str, max_retries: int = 3, timeout: int = 15) -> Optional[str]:
    """Fetch HTML content with retry logic."""
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
            
        except Timeout:
            logger.warning(f"⏳ Timeout fetching {url} (attempt {attempt + 1}/{max_retries})")
        except ConnectionError:
            logger.warning(f"🔌 Connection error fetching {url} (attempt {attempt + 1}/{max_retries})")
        except RequestException as e:
            logger.warning(f"🌐 Request error fetching {url} (attempt {attempt + 1}/{max_retries}): {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error fetching {url} (attempt {attempt + 1}/{max_retries}): {e}")
            
        if attempt < max_retries - 1:
            delay = 2 ** attempt  # Exponential backoff
            logger.info(f"⏳ Retrying in {delay} seconds...")
            time.sleep(delay)
    
    logger.error(f"❌ Failed to fetch {url} after {max_retries} attempts")
    return None

def extract_act_id(href: str) -> Optional[int]:
    """Extract act_id from URL."""
    
    if not href:
        return None
        
    try:
        if "ActID=" in href:
            act_id_str = href.split("ActID=")[1].split("&")[0]
            return int(act_id_str)
        else:
            logger.warning(f"⚠️ No ActID parameter found in href: {href}")
            return None
            
    except (IndexError, ValueError) as e:
        logger.warning(f"⚠️ Error extracting act_id from href {href}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error extracting act_id from href {href}: {e}")
        return None

def detect_relation_type(label: str) -> str:
    """Detect relation type from label text."""
    
    if not label:
        return "related"
        
    try:
        label_lower = label.lower().strip()
        for relation_type in RELATION_TYPES:
            if relation_type in label_lower:
                return relation_type
        return "related"
        
    except Exception as e:
        logger.warning(f"⚠️ Error detecting relation type from label '{label}': {e}")
        return "related"
