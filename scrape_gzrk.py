import requests
from requests.exceptions import RequestException, Timeout, ConnectionError
from bs4 import BeautifulSoup, ParserRejectedMarkup
from urllib.parse import urljoin
import time
import logging
from typing import List, Optional, Dict

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://gzk.rks-gov.net/LawInForceList.aspx"
DOMAIN = "https://gzk.rks-gov.net/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": BASE_URL,
}

def extract_law_links(soup: BeautifulSoup) -> List[str]:
    """Extract law links from parsed HTML with error handling."""
    
    if not soup:
        logger.error("❌ No soup object provided")
        return []
    
    try:
        links = []
        link_elements = soup.select("a[href^='ActDetail.aspx?ActID=']")
        
        for a in link_elements:
            try:
                href = a.get("href")
                if href:
                    full_url = urljoin(DOMAIN, href)
                    links.append(full_url)
                else:
                    logger.warning("⚠️ Found link element without href attribute")
                    
            except Exception as e:
                logger.warning(f"⚠️ Error processing link element: {e}")
                continue
        
        logger.debug(f"✅ Extracted {len(links)} law links")
        return links
        
    except Exception as e:
        logger.error(f"❌ Error extracting law links: {e}")
        return []

def extract_hidden_fields(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    """Extract hidden form fields from ASP.NET page with error handling."""
    
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

def fetch_page_with_retries(session: requests.Session, url: str, data: Optional[Dict] = None, 
                           max_retries: int = 3, timeout: int = 15) -> Optional[requests.Response]:
    """Fetch a page with retry logic."""
    
    for attempt in range(max_retries):
        try:
            if data:
                response = session.post(url, headers=HEADERS, data=data, timeout=timeout)
            else:
                response = session.get(url, headers=HEADERS, timeout=timeout)
            
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

def scrape_all_pages(max_pages: int = 500) -> List[str]:
    """Scrape all pages with comprehensive error handling."""
    
    logger.info("🚀 Starting to scrape all law pages...")
    
    session = requests.Session()
    all_links = []
    page_num = 1
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    try:
        # Initial request to get the first page
        logger.info("📄 Fetching initial page...")
        response = fetch_page_with_retries(session, BASE_URL)
        if not response:
            logger.error("❌ Failed to fetch initial page")
            return []
        
        soup = parse_html_safely(response.text)
        if not soup:
            logger.error("❌ Failed to parse initial page HTML")
            return []
        
        # Extract hidden fields for language switch
        initial_hidden_fields = extract_hidden_fields(soup)
        if not initial_hidden_fields:
            logger.error("❌ Failed to extract hidden fields from initial page")
            return []
        
        # Switch to English language
        logger.info("🌐 Switching to English language...")
        english_data = {
            **initial_hidden_fields,
            "__EVENTTARGET": "ctl00$ctlLang1$lbEnglish",
            "__EVENTARGUMENT": "",
        }
        
        english_response = fetch_page_with_retries(session, BASE_URL, data=english_data)
        if not english_response:
            logger.error("❌ Failed to switch to English language")
            return []
        
        # Parse the English version of the page
        soup = parse_html_safely(english_response.text)
        if not soup:
            logger.error("❌ Failed to parse English page HTML")
            return []
        
        logger.info("✅ Successfully switched to English language")
        
        # Extract links from first page (now in English)
        initial_links = extract_law_links(soup)
        all_links.extend(initial_links)
        logger.info(f"✅ Initial page: found {len(initial_links)} links")
        
        # Extract hidden fields for subsequent requests
        hidden_fields = extract_hidden_fields(soup)
        if not hidden_fields:
            logger.error("❌ Failed to extract hidden fields from English page")
            return all_links  # Return what we have so far
        
        consecutive_errors = 0  # Reset error counter
        
        # Process subsequent pages
        while page_num < max_pages:
            try:
                logger.info(f"📄 Processing page {page_num + 1}... (scraped {len(all_links)} links so far)")
                
                # Find "Next" button
                next_btn = soup.find("a", id="MainContent_gvLawInForce_lbNext")
                if not next_btn or "href" not in next_btn.attrs:
                    logger.info("✅ No Next button found, pagination complete")
                    break
                
                # Prepare form data
                try:
                    data = {
                        **hidden_fields,
                        "__EVENTTARGET": "ctl00$MainContent$gvLawInForce$ctl23$lbNext",
                        "__EVENTARGUMENT": "",
                    }
                except Exception as e:
                    logger.error(f"❌ Error preparing form data: {e}")
                    break
                
                # Fetch next page
                response = fetch_page_with_retries(session, BASE_URL, data=data)
                if not response:
                    logger.error(f"❌ Failed to fetch page {page_num + 1}")
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error("❌ Too many consecutive errors, stopping")
                        break
                    continue
                
                # Parse response
                soup = parse_html_safely(response.text)
                if not soup:
                    logger.error(f"❌ Failed to parse page {page_num + 1} HTML")
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error("❌ Too many consecutive errors, stopping")
                        break
                    continue
                
                # Extract links from this page
                page_links = extract_law_links(soup)
                if not page_links:
                    logger.info(f"✅ No new links found on page {page_num + 1}, stopping")
                    break
                
                all_links.extend(page_links)
                logger.info(f"✅ Page {page_num + 1}: found {len(page_links)} links")
                
                # Update hidden fields for next request
                new_hidden_fields = extract_hidden_fields(soup)
                if new_hidden_fields:
                    hidden_fields = new_hidden_fields
                else:
                    logger.warning(f"⚠️ Could not extract hidden fields from page {page_num + 1}")
                
                page_num += 1
                consecutive_errors = 0  # Reset error counter on success
                
                # Be polite to the server
                time.sleep(1.5)
                
            except Exception as e:
                logger.error(f"❌ Unexpected error on page {page_num + 1}: {e}")
                logger.exception("Detailed error:")
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("❌ Too many consecutive errors, stopping")
                    break
                time.sleep(2)  # Wait before retry
        
        logger.info(f"✅ Scraping complete: collected {len(all_links)} total links across {page_num} pages")
        return all_links
        
    except Exception as e:
        logger.error(f"❌ Critical error during scraping: {e}")
        logger.exception("Detailed error:")
        return all_links  # Return what we have so far
    
    finally:
        # Close session
        try:
            session.close()
            logger.debug("🔒 Session closed")
        except Exception as e:
            logger.warning(f"⚠️ Error closing session: {e}")

if __name__ == "__main__":
    try:
        links = scrape_all_pages()
        
        if links:
            logger.info(f"✅ Successfully scraped {len(links)} law links")
            
            # Print first few links as examples
            logger.info("📋 Sample links:")
            for i, link in enumerate(links[:5]):
                logger.info(f"  {i+1}. {link}")
            
            if len(links) > 5:
                logger.info(f"  ... and {len(links) - 5} more")
            
            # Optionally save to file
            try:
                with open("scraped_links.txt", "w") as f:
                    for link in links:
                        f.write(link + "\n")
                logger.info("💾 Links saved to scraped_links.txt")
            except Exception as e:
                logger.error(f"❌ Error saving links to file: {e}")
                
        else:
            logger.error("❌ No links were scraped")
            
    except KeyboardInterrupt:
        logger.info("⏹️ Scraping interrupted by user")
    except Exception as e:
        logger.error(f"❌ Critical error in main: {e}")
        logger.exception("Detailed error:")
        exit(1)
