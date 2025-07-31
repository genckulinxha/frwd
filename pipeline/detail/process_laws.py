import logging
import requests
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from requests.exceptions import RequestException, Timeout, ConnectionError
from models import Law
from pipeline.utils import parse_date, safe_strip
from bs4 import BeautifulSoup, ParserRejectedMarkup
from datetime import datetime
from pdfminer.high_level import extract_text
from pdfminer.pdfparser import PDFSyntaxError
from pipeline.download_pdf import download_pdf
import time
import os

logger = logging.getLogger(__name__)

def process_unprocessed_laws(session: Session, batch_size: int = 50):
    """Process unprocessed laws with enhanced error handling."""
    
    if not session:
        logger.error("❌ No database session provided")
        raise ValueError("Database session is required")
    
    try:
        laws = session.query(Law).filter_by(unprocessed=True).all()
        logger.info(f"📄 Found {len(laws)} unprocessed laws to enrich")
        
        if not laws:
            logger.info("✅ No unprocessed laws found")
            return
        
        processed_count = 0
        error_count = 0
        skipped_count = 0
        
        # Process in batches to avoid memory issues
        for i in range(0, len(laws), batch_size):
            batch = laws[i:i + batch_size]
            logger.info(f"📦 Processing batch {i//batch_size + 1} ({len(batch)} laws)")
            
            batch_processed = 0
            batch_errors = 0
            batch_skipped = 0
            
            for law in batch:
                try:
                    result = process_single_law(session, law)
                    if result == "processed":
                        batch_processed += 1
                        processed_count += 1
                    elif result == "skipped":
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
            
            logger.info(f"   ✅ Batch {i//batch_size + 1} complete: {batch_processed} processed, {batch_errors} errors, {batch_skipped} skipped")
            
            # Commit batch periodically
            try:
                session.commit()
                logger.debug(f"   💾 Batch {i//batch_size + 1} committed to database")
            except SQLAlchemyError as e:
                logger.error(f"❌ Error committing batch {i//batch_size + 1}: {e}")
                session.rollback()
        
        logger.info(f"✅ Processing complete: {processed_count} processed, {error_count} errors, {skipped_count} skipped")
        
    except SQLAlchemyError as e:
        logger.error(f"❌ Database error in process_unprocessed_laws: {e}")
        session.rollback()
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error in process_unprocessed_laws: {e}")
        logger.exception("Detailed error:")
        raise

def process_single_law(session: Session, law: Law) -> str:
    """Process a single law with comprehensive error handling."""
    
    try:
        if not law.detail_url:
            logger.warning(f"⚠️ No detail URL for ActID={law.act_id}, skipping")
            return "skipped"
            
        logger.info(f"🔍 Enriching ActID={law.act_id}")
        
        # Fetch HTML content with retries
        html_content = fetch_html_with_retries(law.detail_url, max_retries=3)
        if not html_content:
            logger.error(f"❌ Failed to fetch HTML for ActID={law.act_id}")
            return "error"
        
        # Parse HTML content
        try:
            soup = BeautifulSoup(html_content, "html.parser")
        except ParserRejectedMarkup as e:
            logger.error(f"❌ HTML parsing failed for ActID={law.act_id}: {e}")
            return "error"
        
        # Extract metadata with error handling
        try:
            law.title = safe_strip(soup.select_one("div.act_detail_title_a a")) or law.title
            law.law_number = safe_strip(soup.select_one("#MainContent_lblDActNo"))
            law.institution = safe_strip(soup.select_one("#MainContent_lblDInstSpons"))
            law.publish_date = parse_date(safe_strip(soup.select_one("#MainContent_lblDPubDate")))
            law.gazette_number = safe_strip(soup.select_one("#MainContent_lblDGZK"))
            
            logger.debug(f"   📋 Extracted metadata for ActID={law.act_id}")
        except Exception as e:
            logger.warning(f"⚠️ Error extracting metadata for ActID={law.act_id}: {e}")
            # Continue processing even if metadata extraction fails
        
        # Download and process PDF
        try:
            pdf_path = download_pdf(law)
            if pdf_path and os.path.exists(pdf_path):
                law.pdf_path = pdf_path
                law.pdf_downloaded = True
                logger.info(f"   📄 PDF downloaded for ActID={law.act_id}")
                
                # Extract text from PDF
                try:
                    pdf_text = extract_text(pdf_path)
                    if pdf_text and pdf_text.strip():
                        law.pdf_text = pdf_text
                        law.pdf_text_extracted_at = datetime.utcnow()
                        logger.info(f"   📝 Extracted text content from PDF for ActID={law.act_id}")
                    else:
                        logger.warning(f"   ⚠️ PDF text extraction returned empty content for ActID={law.act_id}")
                        law.pdf_text = None
                        
                except PDFSyntaxError as e:
                    logger.warning(f"   ⚠️ PDF syntax error for ActID={law.act_id}: {e}")
                    law.pdf_text = None
                except Exception as e:
                    logger.warning(f"   ⚠️ Failed to extract text from PDF for ActID={law.act_id}: {e}")
                    law.pdf_text = None
            else:
                logger.warning(f"   ⚠️ PDF download failed for ActID={law.act_id}")
                law.pdf_downloaded = False
                law.pdf_path = None
                
        except Exception as e:
            logger.error(f"❌ PDF processing failed for ActID={law.act_id}: {e}")
            law.pdf_downloaded = False
            law.pdf_path = None
            law.pdf_text = None
        
        # Update processing status
        law.processed_at = datetime.utcnow()
        law.unprocessed = False
        
        try:
            session.add(law)
            session.commit()
            logger.info(f"   ✅ Enriched ActID={law.act_id}")
            return "processed"
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Database error saving law ActID={law.act_id}: {e}")
            session.rollback()
            return "error"
            
    except Exception as e:
        logger.error(f"❌ Failed to enrich ActID={law.act_id}: {e}")
        logger.exception("Detailed error:")
        session.rollback()
        return "error"

def fetch_html_with_retries(url: str, max_retries: int = 3, timeout: int = 15) -> str:
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
