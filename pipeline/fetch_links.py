import logging
import time
from urllib.parse import urljoin
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError
from bs4 import BeautifulSoup, ParserRejectedMarkup
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)
BASE_DOMAIN = "https://gzk.rks-gov.net/"

def fetch_category_links(category: str, base_url: str) -> List[Dict]:
    """Fetch all links from a category with comprehensive error handling."""
    
    if not category or not base_url:
        logger.error("❌ Category and base_url are required")
        return []
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    
    all_links = []
    page_num = 1
    consecutive_errors = 0
    max_consecutive_errors = 3

    logger.info(f"[{category}] Starting to fetch links from {base_url}")
    
    # First GET to extract __VIEWSTATE etc.
    try:
        res = fetch_page_with_retries(session, base_url, timeout=15)
        if not res:
            logger.error(f"[{category}] Failed to fetch initial page")
            return []
            
        soup = parse_html_safely(res.text)
        if not soup:
            logger.error(f"[{category}] Failed to parse initial page HTML")
            return []
            
        initial_links = extract_links(soup)
        all_links.extend(initial_links)
        logger.info(f"[{category}] Initial page: found {len(initial_links)} links")
        
        consecutive_errors = 0  # Reset error counter on success
        
    except Exception as e:
        logger.error(f"[{category}] Critical error fetching initial page: {e}")
        logger.exception("Detailed error:")
        return []

    # Process subsequent pages
    while True:
        try:
            logger.info(f"[{category}] Scraped {len(all_links)} links so far (page {page_num})")

            # Find "Next" button
            next_btn = soup.find("a", id=lambda x: x and x.endswith("lbNext"))
            if not next_btn:
                logger.info(f"[{category}] No Next button found, pagination complete.")
                break

            # Extract hidden ASP.NET form fields
            try:
                data = extract_hidden_fields(soup)
                if not data:
                    logger.error(f"[{category}] Failed to extract hidden form fields")
                    break
                    
                data["__EVENTTARGET"] = next_btn["id"]
                data["__EVENTARGUMENT"] = ""
                
            except Exception as e:
                logger.error(f"[{category}] Error extracting form fields: {e}")
                break

            # POST to next page
            try:
                res = session.post(base_url, data=data, timeout=15)
                res.raise_for_status()
                
            except RequestException as e:
                logger.error(f"[{category}] Failed to POST to next page: {e}")
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"[{category}] Too many consecutive errors, stopping pagination")
                    break
                time.sleep(2)  # Wait before retry
                continue

            # Parse response
            soup = parse_html_safely(res.text)
            if not soup:
                logger.error(f"[{category}] Failed to parse page {page_num + 1} HTML")
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    break
                continue
                
            new_links = extract_links(soup)
            if not new_links:
                logger.info(f"[{category}] No new links found on page {page_num + 1}, stopping.")
                break

            all_links.extend(new_links)
            page_num += 1
            consecutive_errors = 0  # Reset error counter on success
            
            # Be polite to the server
            time.sleep(1.5)

        except Exception as e:
            logger.error(f"[{category}] Unexpected error on page {page_num + 1}: {e}")
            logger.exception("Detailed error:")
            consecutive_errors += 1
            if consecutive_errors >= max_consecutive_errors:
                logger.error(f"[{category}] Too many consecutive errors, stopping pagination")
                break
            time.sleep(2)  # Wait before retry

    logger.info(f"[{category}] Finished: collected {len(all_links)} total links across {page_num} pages")
    return all_links

def fetch_page_with_retries(session: requests.Session, url: str, timeout: int = 15, max_retries: int = 3) -> Optional[requests.Response]:
    """Fetch a page with retry logic."""
    
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
            
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

def parse_html_safely(html_content: str) -> Optional[BeautifulSoup]:
    """Parse HTML content with error handling."""
    
    if not html_content:
        logger.error("❌ No HTML content provided")
        return None
        
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        return soup
        
    except ParserRejectedMarkup as e:
        logger.error(f"❌ HTML parsing rejected: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ HTML parsing error: {e}")
        return None

def extract_links(soup: BeautifulSoup) -> List[Dict]:
    """Extract law links from parsed HTML."""
    
    if not soup:
        logger.error("❌ No soup object provided")
        return []
        
    links = []
    try:
        link_elements = soup.select("a[href^='ActDetail.aspx?ActID=']")
        
        for a in link_elements:
            try:
                href = urljoin(BASE_DOMAIN, a.get("href", ""))
                act_id = extract_act_id(href)
                
                if act_id:
                    links.append({
                        "act_id": act_id,
                        "detail_url": href,
                    })
                else:
                    logger.warning(f"⚠️ Could not extract act_id from href: {href}")
                    
            except Exception as e:
                logger.warning(f"⚠️ Error processing link element: {e}")
                continue
                
    except Exception as e:
        logger.error(f"❌ Error extracting links: {e}")
        return []
    
    logger.debug(f"✅ Extracted {len(links)} valid links")
    return links

def extract_hidden_fields(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    """Extract hidden form fields from ASP.NET page."""
    
    if not soup:
        logger.error("❌ No soup object provided")
        return None
        
    try:
        fields = {}
        
        # Extract required ASP.NET fields
        viewstate = soup.select_one("input[name='__VIEWSTATE']")
        if viewstate and viewstate.get("value"):
            fields["__VIEWSTATE"] = viewstate["value"]
        else:
            logger.error("❌ __VIEWSTATE field not found")
            return None
            
        viewstate_gen = soup.select_one("input[name='__VIEWSTATEGENERATOR']")
        if viewstate_gen and viewstate_gen.get("value"):
            fields["__VIEWSTATEGENERATOR"] = viewstate_gen["value"]
        else:
            logger.error("❌ __VIEWSTATEGENERATOR field not found")
            return None
            
        event_validation = soup.select_one("input[name='__EVENTVALIDATION']")
        if event_validation and event_validation.get("value"):
            fields["__EVENTVALIDATION"] = event_validation["value"]
        else:
            logger.error("❌ __EVENTVALIDATION field not found")
            return None
            
        logger.debug("✅ Successfully extracted all hidden fields")
        return fields
        
    except Exception as e:
        logger.error(f"❌ Error extracting hidden fields: {e}")
        return None

def extract_act_id(url: str) -> Optional[int]:
    """Extract act_id from URL."""
    
    if not url:
        return None
        
    try:
        # Extract ActID parameter from URL
        if "ActID=" in url:
            act_id_str = url.split("ActID=")[1].split("&")[0]
            return int(act_id_str)
        else:
            logger.warning(f"⚠️ No ActID parameter found in URL: {url}")
            return None
            
    except (IndexError, ValueError) as e:
        logger.warning(f"⚠️ Error extracting act_id from URL {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error extracting act_id from URL {url}: {e}")
        return None
