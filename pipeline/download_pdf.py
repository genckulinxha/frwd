import logging
import os
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError
from bs4 import BeautifulSoup, ParserRejectedMarkup
from typing import Optional
import time

logger = logging.getLogger(__name__)

def download_pdf(law_obj, max_retries: int = 3, timeout: int = 30) -> Optional[str]:
    """Download PDF for a law with comprehensive error handling."""
    
    if not law_obj:
        logger.error("❌ No law object provided")
        return None
        
    act_id = law_obj.act_id
    detail_url = law_obj.detail_url
    
    if not act_id or not detail_url:
        logger.error(f"❌ Missing required data: act_id={act_id}, detail_url={detail_url}")
        return None
    
    # Ensure data directory exists
    try:
        os.makedirs("data", exist_ok=True)
    except OSError as e:
        logger.error(f"❌ Failed to create data directory: {e}")
        return None
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })

    logger.debug(f"[ActID={act_id}] Starting PDF download from: {detail_url}")
    
    # Load detail page with retries
    detail_response = None
    for attempt in range(max_retries):
        try:
            logger.debug(f"[ActID={act_id}] Loading detail page (attempt {attempt + 1}/{max_retries})")
            detail_response = session.get(detail_url, timeout=timeout)
            detail_response.raise_for_status()
            break
            
        except Timeout:
            logger.warning(f"[ActID={act_id}] Timeout loading detail page (attempt {attempt + 1}/{max_retries})")
        except ConnectionError:
            logger.warning(f"[ActID={act_id}] Connection error loading detail page (attempt {attempt + 1}/{max_retries})")
        except RequestException as e:
            logger.warning(f"[ActID={act_id}] Request error loading detail page (attempt {attempt + 1}/{max_retries}): {e}")
        except Exception as e:
            logger.error(f"[ActID={act_id}] Unexpected error loading detail page (attempt {attempt + 1}/{max_retries}): {e}")
            
        if attempt < max_retries - 1:
            delay = 2 ** attempt  # Exponential backoff
            logger.info(f"[ActID={act_id}] Retrying in {delay} seconds...")
            time.sleep(delay)
    
    if not detail_response:
        logger.error(f"[ActID={act_id}] Failed to load detail page after {max_retries} attempts")
        return None
    
    # Parse HTML content
    try:
        soup = BeautifulSoup(detail_response.text, "html.parser")
    except ParserRejectedMarkup as e:
        logger.error(f"[ActID={act_id}] HTML parsing rejected: {e}")
        return None
    except Exception as e:
        logger.error(f"[ActID={act_id}] HTML parsing error: {e}")
        return None
    
    # Find PDF download button
    pdf_button = soup.find("input", {"id": lambda x: x and "imgDownload" in x})
    if not pdf_button:
        logger.warning(f"[ActID={act_id}] No PDF download button found")
        return None
    
    # Extract required form fields
    try:
        form_fields = extract_form_fields(soup)
        if not form_fields:
            logger.warning(f"[ActID={act_id}] Failed to extract form fields")
            return None
    except Exception as e:
        logger.error(f"[ActID={act_id}] Error extracting form fields: {e}")
        return None

    logger.debug(f"[ActID={act_id}] Submitting POST for PDF download...")

    # Prepare form data
    data = {
        "__EVENTTARGET": pdf_button["name"],
        "__EVENTARGUMENT": "",
        **form_fields
    }

    # Download PDF with retries
    download_response = None
    for attempt in range(max_retries):
        try:
            logger.debug(f"[ActID={act_id}] Downloading PDF (attempt {attempt + 1}/{max_retries})")
            download_response = session.post(detail_url, data=data, timeout=timeout)
            download_response.raise_for_status()
            
            # Check if we got a PDF response
            content_type = download_response.headers.get("content-type", "").lower()
            if "pdf" not in content_type and len(download_response.content) < 1000:
                logger.warning(f"[ActID={act_id}] Response doesn't appear to be a PDF (content-type: {content_type})")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    logger.error(f"[ActID={act_id}] Failed to get valid PDF after all attempts")
                    return None
            
            break
            
        except Timeout:
            logger.warning(f"[ActID={act_id}] Timeout downloading PDF (attempt {attempt + 1}/{max_retries})")
        except ConnectionError:
            logger.warning(f"[ActID={act_id}] Connection error downloading PDF (attempt {attempt + 1}/{max_retries})")
        except RequestException as e:
            logger.warning(f"[ActID={act_id}] Request error downloading PDF (attempt {attempt + 1}/{max_retries}): {e}")
        except Exception as e:
            logger.error(f"[ActID={act_id}] Unexpected error downloading PDF (attempt {attempt + 1}/{max_retries}): {e}")
            
        if attempt < max_retries - 1:
            delay = 2 ** attempt  # Exponential backoff
            logger.info(f"[ActID={act_id}] Retrying PDF download in {delay} seconds...")
            time.sleep(delay)

    if not download_response:
        logger.error(f"[ActID={act_id}] Failed to download PDF after {max_retries} attempts")
        return None

    # Determine filename
    try:
        filename = f"{act_id}.pdf"
        content_disposition = download_response.headers.get("content-disposition", "")
        if content_disposition and "filename=" in content_disposition:
            try:
                filename = content_disposition.split("filename=")[-1].strip('"')
                # Sanitize filename
                filename = "".join(c for c in filename if c.isalnum() or c in "._- ")
                if not filename.endswith(".pdf"):
                    filename = f"{act_id}.pdf"
            except Exception as e:
                logger.warning(f"[ActID={act_id}] Error parsing filename from headers: {e}")
                filename = f"{act_id}.pdf"
        
        file_path = os.path.join("data", filename)
        
    except Exception as e:
        logger.error(f"[ActID={act_id}] Error determining filename: {e}")
        file_path = os.path.join("data", f"{act_id}.pdf")

    # Save PDF file
    try:
        with open(file_path, "wb") as f:
            f.write(download_response.content)
        
        # Verify file was written correctly
        if not os.path.exists(file_path):
            logger.error(f"[ActID={act_id}] PDF file was not created: {file_path}")
            return None
            
        file_size = os.path.getsize(file_path)
        if file_size < 100:  # Minimum reasonable PDF size
            logger.warning(f"[ActID={act_id}] PDF file seems too small ({file_size} bytes): {file_path}")
            return None
            
        logger.info(f"[ActID={act_id}] PDF saved successfully: {file_path} ({file_size} bytes)")
        return file_path
        
    except OSError as e:
        logger.error(f"[ActID={act_id}] File system error saving PDF: {e}")
        return None
    except Exception as e:
        logger.error(f"[ActID={act_id}] Unexpected error saving PDF: {e}")
        return None

def extract_form_fields(soup: BeautifulSoup) -> Optional[dict]:
    """Extract required form fields from ASP.NET page."""
    
    if not soup:
        logger.error("❌ No soup object provided")
        return None
        
    try:
        fields = {}
        
        # Extract required ASP.NET fields
        viewstate = soup.find("input", {"name": "__VIEWSTATE"})
        if viewstate and viewstate.get("value"):
            fields["__VIEWSTATE"] = viewstate["value"]
        else:
            logger.error("❌ __VIEWSTATE field not found")
            return None
            
        viewstate_gen = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
        if viewstate_gen and viewstate_gen.get("value"):
            fields["__VIEWSTATEGENERATOR"] = viewstate_gen["value"]
        else:
            logger.error("❌ __VIEWSTATEGENERATOR field not found")
            return None
            
        event_validation = soup.find("input", {"name": "__EVENTVALIDATION"})
        if event_validation and event_validation.get("value"):
            fields["__EVENTVALIDATION"] = event_validation["value"]
        else:
            logger.error("❌ __EVENTVALIDATION field not found")
            return None
            
        logger.debug("✅ Successfully extracted all form fields")
        return fields
        
    except Exception as e:
        logger.error(f"❌ Error extracting form fields: {e}")
        return None
