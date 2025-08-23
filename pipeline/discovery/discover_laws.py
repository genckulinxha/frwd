import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from models import Law
from pipeline.base import BasePipelineProcessor, ValidationMixin, PipelineError
from pipeline.config import CONFIG, RetryConfig, BatchConfig
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class DiscoveryProcessor(BasePipelineProcessor, ValidationMixin):
    """Improved law discovery processor."""
    
    @classmethod
    def get_model_class(cls):
        """Return the model class this processor works with."""
        return Law
    
    def get_retry_config(self) -> RetryConfig:
        return CONFIG.discovery_retry
    
    def get_batch_config(self) -> BatchConfig:
        return CONFIG.discovery_batch
    
    def get_items_to_process(self) -> List[Any]:
        """Get categories to process."""
        return list(CONFIG.category_urls.items())
    
    def process_single_item(self, item: Any) -> Dict[str, Any]:
        """Process a single category and store ALL its links."""
        category, url = item
        
        try:
            logger.info(f"🔍 Starting discovery for category: {category}")
            
            # Fetch all links from this category
            links = self._fetch_category_links(category, url)
            if not links:
                logger.warning(f"No links found for category: {category}")
                return {"status": "skipped", "category": category, "links_found": 0}
            
            logger.info(f"📋 Found {len(links)} links for category {category}")
            
            # Store ALL links for this category
            stats = self._store_all_category_links(category, links)
            
            logger.info(f"✅ Category {category} complete: {stats['new']} new, {stats['updated']} updated, {stats['errors']} errors")
            
            return {
                "status": "processed",
                "category": category,
                "links_found": len(links),
                "links_stored": stats['new'] + stats['updated'],
                **stats
            }
            
        except Exception as e:
            logger.error(f"Error processing category {category}: {e}")
            logger.exception("Detailed error:")
            return {"status": "error", "category": category, "error": str(e)}
    
    def _fetch_category_links(self, category: str, base_url: str) -> List[Dict[str, Any]]:
        """Fetch all links from a category."""
        all_links = []
        page_count = 0
        max_pages = 200  # Reasonable safety limit
        consecutive_empty_pages = 0
        
        logger.info(f"[{category}] Starting to fetch links from {base_url}")
        
        with self.get_http_client() as client:
            try:
                # Get initial page
                response = client.get(base_url)
                soup = client.parse_html(response.text)
                
                # Language switching is now handled automatically by HttpClient
                
                # Extract links from initial page (now in English)
                initial_links = self._extract_links_from_page(soup)
                all_links.extend(initial_links)
                page_count += 1
                
                logger.info(f"[{category}] Page 1: {len(initial_links)} links")
                
                # Process pagination
                while page_count < max_pages:
                    # Check for next button and if it's enabled
                    next_button = soup.find("a", id=lambda x: x and x.endswith("lbNext"))
                    if not next_button:
                        logger.info(f"[{category}] No next button found, pagination complete")
                        break
                    
                    # Check if next button is disabled
                    if self._is_next_button_disabled(soup, next_button):
                        logger.info(f"[{category}] Next button is disabled, pagination complete")
                        break
                    
                    try:
                        # Extract form data for pagination
                        form_data = self._extract_form_data(soup, next_button)
                        
                        # POST to next page
                        response = client.post(base_url, data=form_data)
                        soup = client.parse_html(response.text)
                        
                        # Extract links from new page
                        new_links = self._extract_links_from_page(soup)
                        
                        # Only stop if we get NO links (empty page)
                        if not new_links:
                            consecutive_empty_pages += 1
                            logger.warning(f"[{category}] Page {page_count + 1}: No links found (consecutive empty: {consecutive_empty_pages})")
                            
                            if consecutive_empty_pages >= 3:
                                logger.info(f"[{category}] Too many consecutive empty pages, stopping pagination")
                                break
                        else:
                            consecutive_empty_pages = 0
                            all_links.extend(new_links)
                            page_count += 1
                            
                            logger.info(f"[{category}] Page {page_count}: {len(new_links)} links (total: {len(all_links)})")
                        
                    except Exception as e:
                        logger.warning(f"Error processing page {page_count + 1} for {category}: {e}")
                        break
                
                if page_count >= max_pages:
                    logger.warning(f"[{category}] Reached maximum page limit ({max_pages}), stopping")
                        
            except Exception as e:
                logger.error(f"Error fetching links for category {category}: {e}")
                raise
        
        # Deduplicate by act_id (normal when pages have overlapping content)
        unique_links = {}
        duplicate_count = 0
        
        for link in all_links:
            act_id = link.get("act_id")
            if act_id and act_id not in unique_links:
                unique_links[act_id] = link
            elif act_id in unique_links:
                duplicate_count += 1
        
        final_links = list(unique_links.values())
        
        if duplicate_count > 0:
            logger.info(f"[{category}] Found {duplicate_count} duplicate entries across pages (normal)")
        
        logger.info(f"[{category}] Discovery complete: {len(final_links)} unique links from {page_count} pages")
        
        return final_links
    
    def _extract_links_from_page(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract law links from a page."""
        links = []
        
        try:
            link_elements = soup.select("a[href^='ActDetail.aspx?ActID=']")
            
            for element in link_elements:
                try:
                    href = element.get("href", "")
                    if not href:
                        continue
                    
                    full_url = urljoin(CONFIG.base_url, href)
                    act_id = self._extract_act_id_from_url(full_url)
                    
                    if act_id:
                        links.append({
                            "act_id": act_id,
                            "detail_url": full_url
                        })
                        
                except Exception as e:
                    logger.warning(f"Error processing link element: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting links from page: {e}")
        
        return links
    
    def _extract_act_id_from_url(self, url: str) -> Optional[int]:
        """Extract act_id from URL."""
        try:
            if "ActID=" in url:
                act_id_str = url.split("ActID=")[1].split("&")[0]
                return self.validate_act_id(act_id_str)
        except Exception:
            pass
        return None
    
    def _extract_form_data(self, soup: BeautifulSoup, next_button) -> Dict[str, str]:
        """Extract form data for pagination."""
        data = {}
        
        # Extract required ASP.NET fields
        for field_name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]:
            field = soup.find("input", {"name": field_name})
            if field and field.get("value"):
                data[field_name] = field["value"]
        
        # Extract the actual control ID from the href attribute
        href = next_button.get('href', '')
        if '__doPostBack' in href:
            try:
                # href format: javascript:__doPostBack('ctl00$MainContent$gvLawInForce$ctl23$lbNext','')
                start = href.find("'") + 1
                end = href.find("'", start)
                actual_control_id = href[start:end]
                data["__EVENTTARGET"] = actual_control_id
                logger.debug(f"Using control ID from href: {actual_control_id}")
            except Exception as e:
                logger.warning(f"Error extracting control ID from href: {e}, falling back to id attribute")
                data["__EVENTTARGET"] = next_button["id"]
        else:
            data["__EVENTTARGET"] = next_button["id"]
        
        data["__EVENTARGUMENT"] = ""
        
        return data
    
    def _is_next_button_disabled(self, soup: BeautifulSoup, next_button) -> bool:
        """Check if the next button is disabled."""
        try:
            # Check for disabled attribute
            if next_button.get("disabled"):
                return True
            
            # Check for CSS classes that indicate disabled state
            classes = next_button.get("class", [])
            if isinstance(classes, str):
                classes = classes.split()
            
            disabled_indicators = ["disabled", "aspNetDisabled", "inactive"]
            if any(indicator in classes for indicator in disabled_indicators):
                return True
            
            # Check for href attribute - disabled buttons often have javascript:__doPostBack instead of real href
            href = next_button.get("href", "")
            if not href or href.startswith("javascript:__doPostBack") and "disabled" in href.lower():
                return True
            
            # Check parent elements for disabled indicators
            parent = next_button.parent
            if parent:
                parent_classes = parent.get("class", [])
                if isinstance(parent_classes, str):
                    parent_classes = parent_classes.split()
                
                if any(indicator in parent_classes for indicator in disabled_indicators):
                    return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Error checking if next button is disabled: {e}")
            return False
    
    def _store_all_category_links(self, category: str, links: List[Dict[str, Any]]) -> Dict[str, int]:
        """Store ALL links for a category with robust error handling."""
        stats = {"new": 0, "updated": 0, "errors": 0}
        
        logger.info(f"[{category}] Storing {len(links)} links to database...")
        
        # Process links in smaller batches for better transaction management
        batch_size = 100
        total_batches = (len(links) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(links))
            batch_links = links[start_idx:end_idx]
            
            logger.debug(f"[{category}] Processing batch {batch_num + 1}/{total_batches} ({len(batch_links)} links)")
            
            batch_stats = self._store_link_batch(category, batch_links)
            
            # Add batch stats to total
            stats["new"] += batch_stats["new"]
            stats["updated"] += batch_stats["updated"] 
            stats["errors"] += batch_stats["errors"]
            
            logger.debug(f"[{category}] Batch {batch_num + 1} complete: {batch_stats}")
        
        logger.info(f"[{category}] Storage complete: {stats}")
        return stats
    
    def _store_link_batch(self, category: str, batch_links: List[Dict[str, Any]]) -> Dict[str, int]:
        """Store a batch of links with transaction management."""
        batch_stats = {"new": 0, "updated": 0, "errors": 0}
        now = datetime.utcnow()
        
        for link_data in batch_links:
            try:
                act_id = link_data["act_id"]
                detail_url = link_data["detail_url"]
                
                # Use a savepoint for individual link processing
                savepoint = self.session.begin_nested()
                
                try:
                    # Check if law exists
                    existing_law = self.session.query(Law).filter_by(act_id=act_id).first()
                    
                    if existing_law:
                        # Update existing law
                        existing_law.last_seen_at = now
                        if existing_law.detail_url != detail_url:
                            existing_law.detail_url = detail_url
                            logger.debug(f"[{category}] Updated URL for ActID={act_id}")
                        batch_stats["updated"] += 1
                    else:
                        # Create new law
                        new_law = Law(
                            act_id=act_id,
                            category=category,
                            detail_url=detail_url,
                            last_seen_at=now,
                            unprocessed=True
                        )
                        self.session.add(new_law)
                        batch_stats["new"] += 1
                        logger.debug(f"[{category}] Created new law ActID={act_id}")
                    
                    # Commit the savepoint
                    savepoint.commit()
                    
                except IntegrityError as e:
                    # Rollback savepoint and try to update existing record
                    savepoint.rollback()
                    logger.debug(f"[{category}] Integrity error for ActID={act_id}, attempting update")
                    
                    try:
                        existing_law = self.session.query(Law).filter_by(act_id=act_id).first()
                        if existing_law:
                            existing_law.last_seen_at = now
                            if existing_law.detail_url != detail_url:
                                existing_law.detail_url = detail_url
                            batch_stats["updated"] += 1
                            logger.debug(f"[{category}] Updated existing law ActID={act_id} after conflict")
                        else:
                            batch_stats["errors"] += 1
                            logger.warning(f"[{category}] Could not find law ActID={act_id} after integrity error")
                    except Exception as update_e:
                        batch_stats["errors"] += 1
                        logger.error(f"[{category}] Failed to update existing law ActID={act_id}: {update_e}")
                        
                except Exception as e:
                    savepoint.rollback()
                    batch_stats["errors"] += 1
                    logger.error(f"[{category}] Error storing law ActID={act_id}: {e}")
                    
            except Exception as e:
                batch_stats["errors"] += 1
                logger.error(f"[{category}] Unexpected error processing link {link_data}: {e}")
        
        # Commit the batch
        try:
            self.session.commit()
            logger.debug(f"[{category}] Batch committed successfully")
        except Exception as e:
            logger.error(f"[{category}] Error committing batch: {e}")
            self.session.rollback()
            # Mark all items in this batch as errors
            batch_stats["errors"] = len(batch_links)
            batch_stats["new"] = 0
            batch_stats["updated"] = 0
        
        return batch_stats


def discover_laws():
    """Main entry point for law discovery."""
    from db import get_session
    
    session = get_session()
    try:
        processor = DiscoveryProcessor(session)
        processor.run()
    finally:
        session.close() 