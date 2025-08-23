import logging
from datetime import datetime
from typing import Dict, List, Any, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

from sqlalchemy.orm import Session

from .models import EULaw
from .base import BasePipelineProcessor
from .config import CONFIG, RetryConfig, BatchConfig

logger = logging.getLogger(__name__)

DIRECTORY_URL = (
    "https://eur-lex.europa.eu/browse/directories/legislation.html?"
    "classification=in-force&displayProfile=allRelAllConsDocProfile"
)


class EUDiscoveryProcessor(BasePipelineProcessor):
    @classmethod
    def get_model_class(cls):
        return EULaw

    def get_retry_config(self) -> RetryConfig:
        return CONFIG.discovery_retry

    def get_batch_config(self) -> BatchConfig:
        return CONFIG.discovery_batch

    def get_items_to_process(self) -> List[Any]:
        # Prefer crawling the Directory page to get canonical listing URLs for
        # all categories and subcategories
        try:
            listing_urls = self._get_category_listing_urls()
            if listing_urls:
                return [listing_urls[0]]  # <-- limit to first listing only
        except Exception as e:
            logger.warning(f"Falling back to category codes due to error: {e}")
        # Fallback: iterate top-level category codes using templated list_url
        items: List[Tuple[str, int]] = []
        for code in CONFIG.category_codes:
            items.append((code, 1))
        return [items[0]]  # <-- limit to first code only

    def process_single_item(self, item: Any) -> Dict[str, Any]:
        # Accept either a (category, start_page) tuple or a direct listing URL
        if isinstance(item, tuple):
            category, start_page = item
            total_links = 0
            total_new = 0
            total_updated = 0
            page = start_page
            while True:
                list_url = CONFIG.list_url.format(category=category, page=page)
                logger.info(f"EU list: category {category} page {page}")
                with self.get_http_client() as client:
                    res = client.get(list_url)
                    soup = client.parse_html(res.text)

                links = self._extract_result_links(soup)
                logger.info(f"Category {category} page {page}: extracted {len(links)} links")
                if links:
                    stats = self._store_links(links)
                    total_links += len(links)
                    total_new += stats["new"]
                    total_updated += stats["updated"]
                    logger.info(f"Category {category} page {page}: stored new={stats['new']} updated={stats['updated']} errors={stats['errors']}")
                    break  # <-- stop after page 1
                if not self._has_next_page(soup):
                    break
                page += 1
            logger.info(f"Category {category} complete: pages={page - start_page + 1} extracted={total_links} new={total_new} updated={total_updated}")
            return {"status": "processed", "category": category, "links_stored": total_new + total_updated}
        else:
            base_listing_url: str = str(item)
            total_links = 0
            total_new = 0
            total_updated = 0
            page = 1
            while True:
                list_url = self._with_page_param(base_listing_url, page)
                with self.get_http_client() as client:
                    res = client.get(list_url)
                    soup = client.parse_html(res.text)
                links = self._extract_result_links(soup)
                logger.info(f"Listing page {page}: extracted {len(links)} links")
                if links:
                    stats = self._store_links(links)
                    total_links += len(links)
                    total_new += stats["new"]
                    total_updated += stats["updated"]
                    logger.info(f"Listing page {page}: stored new={stats['new']} updated={stats['updated']} errors={stats['errors']}")
                    break  # <-- stop after page 1
                if not self._has_next_page(soup):
                    break
                page += 1
            logger.info(f"Listing complete: pages={page} extracted={total_links} new={total_new} updated={total_updated}")
            return {"status": "processed", "listing": base_listing_url, "links_stored": total_new + total_updated}

    def _get_category_listing_urls(self) -> List[str]:
        urls: List[str] = []
        with self.get_http_client() as client:
            res = client.get(DIRECTORY_URL)
            soup = client.parse_html(res.text)
        tree = soup.select_one("ul#tree")
        if not tree:
            return urls
        for a in tree.find_all("a", href=True, class_="gotoResultLink"):
            href = a["href"]
            full = urljoin("https://eur-lex.europa.eu/browse/directories/", href)
            # Normalize to https://eur-lex.europa.eu/search.html?... form
            if "/search.html" in full:
                urls.append(full)
        # De-duplicate
        return list(dict.fromkeys(urls))

    def _with_page_param(self, url: str, page: int) -> str:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        qs["page"] = [str(page)]
        new_query = urlencode(qs, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

    def _extract_result_links(self, soup) -> List[Dict[str, Any]]:
        links: List[Dict[str, Any]] = []
        # Only capture the main law link per result: div.SearchResult h2 > a.title
        for result in soup.select('div.SearchResult'):
            a = result.select_one('h2 > a.title[href]')
            if not a:
                continue
            href = a.get('href', '')
            if '/legal-content/' in href and 'CELEX:' in href:
                full = urljoin('https://eur-lex.europa.eu', href)
                title = a.get_text(strip=True)
                links.append({'title': title, 'url': full})
        # Fallback (rare layouts): pick top-level a.title outside of results list
        if not links:
            for a in soup.select('a.title[href]'):
                href = a['href']
                if '/legal-content/' in href and 'CELEX:' in href:
                    full = urljoin('https://eur-lex.europa.eu', href)
                    title = a.get_text(strip=True)
                    links.append({'title': title, 'url': full})
        return links

    def _extract_celex_from_url(self, url: str) -> str:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        if 'uri' in qs:
            vals = qs['uri']
            if vals:
                val = vals[0]
                if val.startswith('CELEX:'):
                    return val.split(':', 1)[1]
                return val
        parts = parsed.path.split('/')
        for part in parts:
            if part.startswith('CELEX:'):
                return part.split(':', 1)[1]
        return ''

    def _has_next_page(self, soup) -> bool:
        # Detect explicit "Next Page" button
        for a in soup.find_all("a", href=True):
            title_attr = (a.get("title") or "").strip().lower()
            if title_attr == "next page":
                return True
            text = a.get_text(strip=True).lower()
            if text in {"next", "next »", "next ›"}:
                return True
            href = a["href"]
            if "&page=" in href:
                # Presence of a page link suggests more pages
                return True
        return False

    def _store_links(self, links: List[Dict[str, Any]]) -> Dict[str, int]:
        stats = {"new": 0, "updated": 0, "errors": 0}
        for link in links:
            try:
                url = link["url"]
                celex_id = self._extract_celex_from_url(url)
                if not celex_id:
                    continue
                existing = (
                    self.session.query(EULaw).filter_by(celex_id=celex_id).one_or_none()
                )
                if existing:
                    existing.last_seen_at = datetime.utcnow()
                    existing.title = existing.title or link.get("title")
                    existing.detail_url = url
                    stats["updated"] += 1
                else:
                    obj = EULaw(
                        celex_id=celex_id,
                        title=link.get("title"),
                        detail_url=url,
                        last_seen_at=datetime.utcnow(),
                    )
                    self.session.add(obj)
                    stats["new"] += 1
                self.session.flush()
            except Exception as e:
                logger.error(f"Error storing EU link {link}: {e}")
                self.session.rollback()
                stats["errors"] += 1
        self.session.commit()
        return stats


def discover_eu_laws():
    from .db import get_session

    session = get_session()
    try:
        processor = EUDiscoveryProcessor(session)
        processor.run()
    finally:
        session.close() 