import logging
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from threading import Lock

from sqlalchemy.orm import Session
from pdfminer.high_level import extract_text
from pdfminer.pdfparser import PDFSyntaxError

from models import Law
from pipeline.base import BasePipelineProcessor, ValidationMixin, PipelineError
from pipeline.config import CONFIG, RetryConfig, BatchConfig
from pipeline.utils import parse_date, safe_strip

logger = logging.getLogger(__name__)


class OCRManager:
    """Singleton OCR manager for thread-safe model sharing."""
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.reader = None
            self.reader_lock = Lock()
            self._initialized = True
    
    def get_reader(self):
        """Get OCR reader instance (thread-safe)."""
        if self.reader is None:
            with self.reader_lock:
                if self.reader is None:
                    self._initialize_reader()
        return self.reader
    
    def _initialize_reader(self):
        """Initialize OCR reader with proper configuration."""
        try:
            import easyocr
            import torch
            import os
            import ssl
            import certifi
            
            # Set SSL certificates for model download
            os.environ['SSL_CERT_FILE'] = certifi.where()
            os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
            
            # Auto-detect GPU availability
            use_gpu = torch.cuda.is_available()
            if not use_gpu and hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
                use_gpu = True  # Use MPS for Apple Silicon
            
            logger.info(f"Initializing OCR reader with {'GPU' if use_gpu else 'CPU'} acceleration")
            
            # Initialize OCR reader (supports Albanian and English)
            try:
                self.reader = easyocr.Reader(['en', 'sq'], gpu=use_gpu)
                logger.info("OCR reader initialized with Albanian and English support")
            except Exception as e:
                logger.warning(f"Failed to initialize OCR reader with Albanian support: {e}")
                # Fallback to English only
                self.reader = easyocr.Reader(['en'], gpu=use_gpu)
                logger.info("OCR reader initialized with English support only")
                
        except Exception as e:
            logger.error(f"Failed to initialize OCR reader: {e}")
            self.reader = None


class DetailProcessor(BasePipelineProcessor, ValidationMixin):
    """Improved law detail processor."""
    
    def __init__(self, session: Session):
        super().__init__(session)
        self.ocr_manager = OCRManager()
    
    @classmethod
    def get_model_class(cls):
        """Return the model class this processor works with."""
        return Law
    
    def get_retry_config(self) -> RetryConfig:
        return CONFIG.detail_retry
    
    def get_batch_config(self) -> BatchConfig:
        return CONFIG.detail_batch
    
    def get_items_to_process(self) -> List[Any]:
        """Get unprocessed laws."""
        return self.session.query(Law).filter_by(unprocessed=True).all()
    
    def process_single_item(self, item: Any) -> Dict[str, Any]:
        """Process a single law."""
        law = item
        
        try:
            if not law.detail_url:
                logger.warning(f"No detail URL for ActID={law.act_id}")
                return {"status": "skipped", "act_id": law.act_id}
            
            # Process law metadata
            metadata_success = self._process_metadata(law)
            
            # Process PDF
            pdf_success = self._process_pdf(law)
            
            # Only mark as processed if PDF text extraction succeeds
            if pdf_success:
                law.processed_at = datetime.utcnow()
                law.unprocessed = False
                status = "processed"
            else:
                # Keep as unprocessed if PDF text extraction fails
                law.unprocessed = True
                status = "partial"
            
            return {
                "status": status,
                "act_id": law.act_id,
                "metadata_success": metadata_success,
                "pdf_success": pdf_success
            }
            
        except Exception as e:
            logger.error(f"Error processing law ActID={law.act_id}: {e}")
            return {"status": "error", "act_id": law.act_id, "error": str(e)}
    
    def _process_metadata(self, law: Law) -> bool:
        """Process law metadata from detail page."""
        try:
            with self.get_http_client() as client:
                response = client.get(law.detail_url)
                soup = client.parse_html(response.text)
                
                # Extract metadata fields
                law.title = self._extract_title(soup) or law.title
                law.law_number = self._extract_law_number(soup)
                law.institution = self._extract_institution(soup)
                law.publish_date = self._extract_publish_date(soup)
                law.gazette_number = self._extract_gazette_number(soup)
                
                logger.debug(f"Extracted metadata for ActID={law.act_id}")
                return True
                
        except Exception as e:
            logger.warning(f"Failed to extract metadata for ActID={law.act_id}: {e}")
            return False
    
    def _process_pdf(self, law: Law) -> bool:
        """Process PDF download and text extraction."""
        try:
            # Download PDF
            pdf_path = self._download_pdf(law)
            if not pdf_path:
                return False
            
            # Extract text from PDF (required for success)
            text_success = self._extract_pdf_text(law, pdf_path)
            
            law.pdf_path = pdf_path
            law.pdf_downloaded = True
            
            if text_success:
                logger.info(f"PDF processed successfully for ActID={law.act_id}")
                return True
            else:
                logger.warning(f"PDF downloaded but text extraction failed for ActID={law.act_id}")
                return False
            
        except Exception as e:
            logger.warning(f"Failed to process PDF for ActID={law.act_id}: {e}")
            law.pdf_downloaded = False
            return False
    
    def _download_pdf(self, law: Law) -> Optional[str]:
        """Download PDF file."""
        try:
            # Ensure data directory exists
            os.makedirs(CONFIG.data_directory, exist_ok=True)
            
            # Check if PDF file already exists
            filename = f"{law.act_id}.pdf"
            file_path = os.path.join(CONFIG.data_directory, filename)
            
            if os.path.exists(file_path) and os.path.getsize(file_path) > 100:
                logger.debug(f"PDF file already exists for ActID={law.act_id}: {file_path}")
                return file_path
            
            with self.get_http_client() as client:
                # Get detail page to find PDF download button
                response = client.get(law.detail_url)
                soup = client.parse_html(response.text)
                
                # Find PDF download button
                pdf_button = soup.find("input", {"id": lambda x: x and "imgDownload" in x})
                if not pdf_button:
                    logger.warning(f"No PDF download button found for ActID={law.act_id}")
                    return None
                
                # Extract form data
                form_data = self._extract_pdf_form_data(soup, pdf_button)
                
                # Download PDF
                pdf_response = client.post(law.detail_url, data=form_data)
                
                # Validate PDF response
                if not self._is_valid_pdf_response(pdf_response):
                    logger.warning(f"Invalid PDF response for ActID={law.act_id}")
                    return None
                
                # Save PDF file
                with open(file_path, "wb") as f:
                    f.write(pdf_response.content)
                
                # Verify file
                if not os.path.exists(file_path) or os.path.getsize(file_path) < 100:
                    logger.warning(f"PDF file validation failed for ActID={law.act_id}")
                    return None
                
                logger.debug(f"PDF downloaded for ActID={law.act_id}: {file_path}")
                return file_path
                
        except Exception as e:
            logger.error(f"Error downloading PDF for ActID={law.act_id}: {e}")
            return None
    
    def _extract_pdf_text(self, law: Law, pdf_path: str) -> bool:
        """Extract text from document file (PDF, Word, or HTML)."""
        try:
            # Check what type of file we actually have
            file_type = self._get_file_type(pdf_path)
            
            if file_type == 'pdf':
                return self._extract_text_from_pdf(law, pdf_path)
            elif file_type == 'word':
                return self._extract_text_from_word(law, pdf_path)
            elif file_type == 'html':
                return self._extract_text_from_html(law, pdf_path)
            else:
                logger.warning(f"Unsupported file type '{file_type}' for ActID={law.act_id}: {pdf_path}")
                return False
                
        except Exception as e:
            logger.warning(f"Failed to extract text from document for ActID={law.act_id}: {e}")
            return False
    
    def _extract_text_from_pdf(self, law: Law, pdf_path: str) -> bool:
        """Extract text from PDF file using text extraction and OCR as fallback."""
        try:
            # First try regular text extraction
            pdf_text = extract_text(pdf_path)
            if pdf_text and pdf_text.strip():
                law.pdf_text = pdf_text
                law.pdf_text_extracted_at = datetime.utcnow()
                logger.debug(f"Extracted text from PDF for ActID={law.act_id}")
                return True
            
            # If regular extraction fails, check if it's an image-based PDF
            try:
                import fitz
                doc = fitz.open(pdf_path)
                if len(doc) > 0:
                    page = doc[0]
                    images = len(page.get_images())
                    text_blocks = len(page.get_text_blocks())
                    
                    if images > 0 and text_blocks == 0:
                        logger.info(f"PDF for ActID={law.act_id} is image-based - skipping OCR for now (will process later)")
                        doc.close()
                        
                        # Try OCR extraction
                        # ocr_text = self._extract_text_with_ocr(pdf_path)
                        # if ocr_text and ocr_text.strip():
                        #     law.pdf_text = ocr_text
                        #     law.pdf_text_extracted_at = datetime.utcnow()
                        #     logger.info(f"Successfully extracted text using OCR for ActID={law.act_id}")
                        #     return True
                        # else:
                        #     logger.warning(f"OCR extraction failed for ActID={law.act_id}")
                        #     return False
                    else:
                        doc.close()
                        logger.warning(f"PDF text extraction returned empty content for ActID={law.act_id}")
                        return False
                else:
                    doc.close()
                    logger.warning(f"PDF text extraction returned empty content for ActID={law.act_id}")
                    return False
            except ImportError:
                logger.warning(f"PDF text extraction returned empty content for ActID={law.act_id} (install PyMuPDF for better analysis)")
                return False
            except Exception as e:
                logger.warning(f"Error analyzing PDF for ActID={law.act_id}: {e}")
                return False
                
        except PDFSyntaxError as e:
            logger.warning(f"PDF syntax error for ActID={law.act_id}: {e}")
            return False
        except Exception as e:
            logger.warning(f"Failed to extract text from PDF for ActID={law.act_id}: {e}")
            return False
    
    def _extract_text_with_ocr(self, pdf_path: str) -> Optional[str]:
        """Extract text from PDF using OCR."""
        try:
            import fitz
            from PIL import Image
            import io
            
            # Get the shared OCR reader
            reader = self.ocr_manager.get_reader()
            if reader is None:
                logger.error("OCR reader not available")
                return None
            
            doc = fitz.open(pdf_path)
            all_text = []
            
            logger.info(f"Starting OCR processing for {len(doc)} pages in {pdf_path}")
            
            # Process each page
            for page_num in range(len(doc)):
                page = doc[page_num]
                
                # Convert page to image
                mat = fitz.Matrix(2, 2)  # 2x zoom for better OCR
                pix = page.get_pixmap(matrix=mat)
                img_data = pix.tobytes("png")
                
                # Convert to PIL Image
                img = Image.open(io.BytesIO(img_data))
                
                # Perform OCR (reader is thread-safe for inference)
                results = reader.readtext(img_data)
                
                # Extract text from results
                page_text = []
                for (bbox, text, confidence) in results:
                    if confidence > 0.5:  # Filter out low-confidence results
                        page_text.append(text)
                
                if page_text:
                    all_text.append(" ".join(page_text))
                
                # Log progress for large documents
                if (page_num + 1) % 5 == 0:
                    logger.debug(f"OCR processed {page_num + 1}/{len(doc)} pages")
            
            doc.close()
            
            logger.info(f"OCR processing complete for {pdf_path}: {len(all_text)} pages with text")
            
            if all_text:
                return "\n\n".join(all_text)
            else:
                return None
                
        except ImportError as e:
            logger.error(f"OCR dependencies not available: {e}")
            return None
        except Exception as e:
            logger.error(f"OCR extraction failed for {pdf_path}: {e}")
            return None
    
    def _extract_title(self, soup) -> Optional[str]:
        """Extract law title from HTML."""
        try:
            title_elem = soup.select_one("div.act_detail_title_a a")
            return self.sanitize_text(safe_strip(title_elem)) if title_elem else None
        except Exception:
            return None
    
    def _extract_law_number(self, soup) -> Optional[str]:
        """Extract law number from HTML."""
        try:
            elem = soup.select_one("#MainContent_lblDActNo")
            return self.sanitize_text(safe_strip(elem)) if elem else None
        except Exception:
            return None
    
    def _extract_institution(self, soup) -> Optional[str]:
        """Extract institution from HTML."""
        try:
            elem = soup.select_one("#MainContent_lblDInstSpons")
            return self.sanitize_text(safe_strip(elem)) if elem else None
        except Exception:
            return None
    
    def _extract_publish_date(self, soup) -> Optional[datetime]:
        """Extract publish date from HTML."""
        try:
            elem = soup.select_one("#MainContent_lblDPubDate")
            date_text = safe_strip(elem) if elem else None
            return parse_date(date_text) if date_text else None
        except Exception:
            return None
    
    def _extract_gazette_number(self, soup) -> Optional[str]:
        """Extract gazette number from HTML."""
        try:
            elem = soup.select_one("#MainContent_lblDGZK")
            return self.sanitize_text(safe_strip(elem)) if elem else None
        except Exception:
            return None
    
    def _extract_pdf_form_data(self, soup, pdf_button) -> Dict[str, str]:
        """Extract form data for PDF download."""
        data = {}
        
        # Extract required ASP.NET fields
        for field_name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]:
            field = soup.find("input", {"name": field_name})
            if field and field.get("value"):
                data[field_name] = field["value"]
        
        # Add PDF button event
        data["__EVENTTARGET"] = pdf_button["name"]
        data["__EVENTARGUMENT"] = ""
        
        return data
    
    def _is_valid_pdf_response(self, response) -> bool:
        """Check if response contains valid document content (PDF, Word, or HTML)."""
        if not response or not response.content:
            return False
        
        content = response.content
        if len(content) < 8:
            return False
        
        # Check for PDF files
        if content.startswith(b'%PDF-'):
            return True
        
        # Check for Microsoft Office compound document (old .doc format)
        if content.startswith(b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1'):
            logger.debug("Response contains Word document")
            return True
        
        # Check for HTML content
        content_lower = content.lower()
        if content_lower.startswith(b'<!doctype html') or content_lower.startswith(b'<html'):
            logger.debug("Response contains HTML document")
            return True
        
        # Check for XML (some newer Word docs)
        if content.startswith(b'<?xml'):
            logger.debug("Response contains XML document")
            return True
        
        # Log what we actually received for debugging
        content_preview = content[:100].decode('utf-8', errors='ignore')
        logger.debug(f"Response doesn't contain recognized document format. Got: {content_preview}")
        return False
    
    def _get_file_type(self, file_path: str) -> str:
        """Determine the actual file type based on content."""
        try:
            if not os.path.exists(file_path):
                return 'unknown'
            
            # Check file size
            if os.path.getsize(file_path) < 8:
                return 'unknown'
            
            # Read first few bytes to check magic bytes
            with open(file_path, 'rb') as f:
                header = f.read(200)  # Read more bytes to handle whitespace
                
                # Check for PDF
                if header.startswith(b'%PDF-'):
                    return 'pdf'
                
                # Check for Microsoft Office compound document (old .doc format)
                # Magic bytes: D0 CF 11 E0 A1 B1 1A E1
                if header.startswith(b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1'):
                    return 'word'
                
                # Check for HTML (handle whitespace at beginning)
                header_lower = header.lower().strip()
                if header_lower.startswith(b'<!doctype html') or header_lower.startswith(b'<html'):
                    return 'html'
                
                # Check for XML (some newer Word docs)
                if header.startswith(b'<?xml'):
                    return 'xml'
            
            return 'unknown'
            
        except Exception as e:
            logger.warning(f"Error determining file type for {file_path}: {e}")
            return 'unknown'
    
    def _extract_text_from_word(self, law: Law, file_path: str) -> bool:
        """Extract text from Word document using antiword."""
        try:
            import subprocess
            
            # Use antiword to extract text from .doc file
            result = subprocess.run(
                ['antiword', file_path],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0 and result.stdout.strip():
                law.pdf_text = result.stdout.strip()
                law.pdf_text_extracted_at = datetime.utcnow()
                logger.info(f"Extracted text from Word document for ActID={law.act_id}")
                return True
            else:
                logger.warning(f"antiword failed for ActID={law.act_id}: {result.stderr}")
                return False
                
        except FileNotFoundError:
            logger.error(f"antiword not found. Install with: sudo apt-get install antiword")
            return False
        except subprocess.TimeoutExpired:
            logger.warning(f"antiword timeout for ActID={law.act_id}")
            return False
        except Exception as e:
            logger.warning(f"Error extracting text from Word document for ActID={law.act_id}: {e}")
            return False
    
    def _extract_text_from_html(self, law: Law, file_path: str) -> bool:
        """Extract text from HTML file."""
        try:
            from bs4 import BeautifulSoup
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'html.parser')
            text = soup.get_text(separator='\n', strip=True)
            
            if text and text.strip():
                law.pdf_text = text
                law.pdf_text_extracted_at = datetime.utcnow()
                logger.info(f"Extracted text from HTML document for ActID={law.act_id}")
                return True
            else:
                logger.warning(f"No text found in HTML document for ActID={law.act_id}")
                return False
                
        except Exception as e:
            logger.warning(f"Error extracting text from HTML document for ActID={law.act_id}: {e}")
            return False


def process_unprocessed_laws(session: Session, batch_size: int = 50):
    """Main entry point for law processing."""
    try:
        processor = DetailProcessor(session)
        processor.run()
    finally:
        pass  # Session managed by caller 