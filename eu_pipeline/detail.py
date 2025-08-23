import logging
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin, urlparse, parse_qs

from sqlalchemy.orm import Session
from pdfminer.high_level import extract_text
from pdfminer.pdfparser import PDFSyntaxError

from .models import EULaw
from .base import BasePipelineProcessor
from .config import CONFIG, RetryConfig, BatchConfig

logger = logging.getLogger(__name__)


class EUDetailProcessor(BasePipelineProcessor):
    @classmethod
    def get_model_class(cls):
        return EULaw

    def get_retry_config(self) -> RetryConfig:
        return CONFIG.detail_retry

    def get_batch_config(self) -> BatchConfig:
        return CONFIG.detail_batch

    def get_items_to_process(self) -> List[Any]:
        items = self.session.query(EULaw).filter_by(unprocessed=True).all()
        logger.info(f"Detail: {len(items)} unprocessed EU laws")
        return items

    def process_single_item(self, item: Any) -> Dict[str, Any]:
        law: EULaw = item
        logger.debug(f"Detail: start celex={law.celex_id} url={law.detail_url}")
        if not law.detail_url:
            return {"status": "skipped", "celex_id": law.celex_id}

        meta_ok = self._process_metadata(law)
        text_ok = self._process_text(law)
        if text_ok:
            law.processed_at = datetime.utcnow()
            law.unprocessed = False
        self.session.commit()
        logger.info(f"Detail: celex={law.celex_id} meta={meta_ok} text={text_ok}")
        return {"status": "processed", "celex_id": law.celex_id, "meta": meta_ok, "text": text_ok}

    def _build_all_url(self, law: EULaw) -> str:
        if law.celex_id:
            return f"https://eur-lex.europa.eu/legal-content/EN/ALL/?uri=CELEX:{law.celex_id}"
        celex = self._extract_celex_from_url(law.detail_url)
        return f"https://eur-lex.europa.eu/legal-content/EN/ALL/?uri=CELEX:{celex}" if celex else law.detail_url

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

    def _process_metadata(self, law: EULaw) -> bool:
        try:
            with self.get_http_client() as client:
                res = client.get(law.detail_url, timeout=8, allow_redirects=True)
                soup = client.parse_html(res.text)

            title_el = soup.select_one('#title') or soup.select_one('#englishTitle')
            if not title_el:
                title_el = soup.select_one('.eli-main-title') or soup.find('h1')
            if title_el:
                law.title = title_el.get_text(strip=True)
            else:
                logger.debug("Detail(meta): title not found on EN/TXT")

            def find_meta_value(label: str) -> Optional[str]:
                for dt in soup.select('dl.NMetadata dt'):
                    if label.lower() in dt.get_text(strip=True).lower():
                        dd = dt.find_next_sibling('dd')
                        if dd:
                            return dd.get_text(strip=True)
                return None

            form_val = find_meta_value('Form')
            if form_val:
                law.law_type = form_val
            date_doc = find_meta_value('Date of document')
            if date_doc:
                try:
                    from datetime import datetime as _dt
                    law.publish_date = _dt.strptime(date_doc[:10], '%d/%m/%Y').date()
                except Exception:
                    pass

            if not form_val and not date_doc:
                all_url = self._build_all_url(law)
                with self.get_http_client() as client:
                    res_all = client.get(all_url, timeout=8, allow_redirects=True)
                    soup_all = client.parse_html(res_all.text)
                def find_meta_value_all(label: str) -> Optional[str]:
                    for dt in soup_all.select('dl.NMetadata dt'):
                        if label.lower() in dt.get_text(strip=True).lower():
                            dd = dt.find_next_sibling('dd')
                            if dd:
                                return dd.get_text(strip=True)
                    return None
                fv = find_meta_value_all('Form')
                if fv:
                    law.law_type = fv
                dv = find_meta_value_all('Date of document')
                if dv:
                    try:
                        from datetime import datetime as _dt
                        law.publish_date = _dt.strptime(dv[:10], '%d/%m/%Y').date()
                    except Exception:
                        pass

            return True
        except Exception as e:
            logger.warning(f"Detail(meta): error celex={law.celex_id}: {e}")
            return False

    def _extract_text_from_soup(self, soup) -> Optional[str]:
        # If HTML unavailable message present, signal to use PDF
        unavailable = soup.find(string=lambda t: isinstance(t, str) and 'HTML format is unavailable' in t)
        if unavailable:
            return None
        container = (
            soup.select_one('#PP4Contents div.eli-container') or
            soup.select_one('#PP4Contents #text #textTabContent') or
            soup.select_one('div#text') or
            soup.select_one('div#textTabContent')
        )
        if container:
            return container.get_text('\n', strip=True)
        return None

    def _process_text(self, law: EULaw) -> bool:
        # EN/TXT
        try:
            with self.get_http_client() as client:
                res = client.get(law.detail_url, timeout=8, allow_redirects=True)
                soup = client.parse_html(res.text)
            text_content = self._extract_text_from_soup(soup)
            if text_content and len(text_content) > 500:
                law.pdf_text = text_content
                law.pdf_text_extracted_at = datetime.utcnow()
                return True
        except Exception as e:
            logger.debug(f"Detail(text): EN/TXT error celex={law.celex_id}: {e}")

        # EN/ALL fallback
        try:
            all_url = self._build_all_url(law)
            with self.get_http_client() as client:
                res = client.get(all_url, timeout=8, allow_redirects=True)
                soup = client.parse_html(res.text)
            text_content = self._extract_text_from_soup(soup)
            if text_content and len(text_content) > 500:
                law.pdf_text = text_content
                law.pdf_text_extracted_at = datetime.utcnow()
                return True
        except Exception as e:
            logger.debug(f"Detail(text): EN/ALL error celex={law.celex_id}: {e}")

        # PDF fallback (also used when HTML format unavailable)
        try:
            with self.get_http_client() as client:
                res = client.get(law.detail_url, timeout=8, allow_redirects=True)
                soup = client.parse_html(res.text)
            # Try direct EN PDF button first
            pdf_a = soup.select_one('a#format_language_table_PDF_EN[href]')
            pdf_link = urljoin('https://eur-lex.europa.eu', pdf_a['href']) if pdf_a else None
            if not pdf_link:
                # General scan fallback
                for a in soup.find_all('a', href=True):
                    href = a['href'].lower()
                    if href.endswith('.pdf') or 'format=pdf' in href:
                        pdf_link = urljoin('https://eur-lex.europa.eu', a['href'])
                        break
            if not pdf_link:
                return False
            os.makedirs(CONFIG.data_directory, exist_ok=True)
            pdf_path = os.path.join(CONFIG.data_directory, f"{law.celex_id or 'doc'}.pdf")
            with self.get_http_client() as client:
                resp = client.get(pdf_link, timeout=15, allow_redirects=True)
                with open(pdf_path, 'wb') as f:
                    f.write(resp.content)
            law.pdf_downloaded = True
            law.pdf_path = pdf_path
            try:
                text_content = extract_text(pdf_path)
                if text_content and text_content.strip():
                    law.pdf_text = text_content
                    law.pdf_text_extracted_at = datetime.utcnow()
                    return True
            except PDFSyntaxError:
                logger.warning(f"Detail(text): PDFSyntaxError for {pdf_path}")
                return False
            except Exception as e:
                logger.warning(f"Detail(text): PDF extract error {pdf_path}: {e}")
                return False
            return False
        except Exception as e:
            logger.warning(f"Detail(text): PDF fallback error celex={law.celex_id}: {e}")
            return False


def process_unprocessed_eu_laws(session: Session):
    processor = EUDetailProcessor(session)
    processor.run() 