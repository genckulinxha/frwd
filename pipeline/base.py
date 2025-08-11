import logging
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Iterator, Optional, List, Dict, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError
from bs4 import BeautifulSoup, ParserRejectedMarkup
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from .config import CONFIG, PipelineStats, RetryConfig, BatchConfig

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Base exception for pipeline operations."""
    pass


class RetryManager:
    """Handles retry logic with exponential backoff."""
    
    def __init__(self, config: RetryConfig):
        self.config = config
    
    def retry_with_backoff(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic and exponential backoff."""
        last_exception = None
        
        for attempt in range(self.config.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                if attempt < self.config.max_retries - 1:
                    delay = min(
                        self.config.base_delay * (self.config.exponential_base ** attempt),
                        self.config.max_delay
                    )
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay}s: {e}")
                    time.sleep(delay)
                else:
                    logger.error(f"All {self.config.max_retries} attempts failed")
        
        raise last_exception


class HttpClient:
    """HTTP client with retry logic and consistent headers."""
    
    def __init__(self, retry_config: RetryConfig):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": CONFIG.user_agent
        })
        self.retry_manager = RetryManager(retry_config)
        self._english_switched = False
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """GET request with retry logic and automatic English language switching."""
        kwargs.setdefault('timeout', self.retry_manager.config.timeout)
        
        def _get():
            response = self.session.get(url, **kwargs)
            response.raise_for_status()
            
            # Auto-switch to English for gzk.rks-gov.net sites
            if not self._english_switched and "gzk.rks-gov.net" in url:
                self._switch_to_english(response, url)
                # After switching, make a fresh request to get English content
                response = self.session.get(url, **kwargs)
                response.raise_for_status()
            
            return response
        
        return self.retry_manager.retry_with_backoff(_get)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """POST request with retry logic."""
        kwargs.setdefault('timeout', self.retry_manager.config.timeout)
        
        def _post():
            response = self.session.post(url, **kwargs)
            response.raise_for_status()
            return response
        
        return self.retry_manager.retry_with_backoff(_post)
    
    def parse_html(self, html_content: str) -> BeautifulSoup:
        """Parse HTML with error handling."""
        if not html_content:
            raise ValueError("No HTML content provided")
        
        try:
            return BeautifulSoup(html_content, "html.parser")
        except ParserRejectedMarkup as e:
            raise PipelineError(f"HTML parsing rejected: {e}")
        except Exception as e:
            raise PipelineError(f"HTML parsing error: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if hasattr(self.session, 'close'):
            self.session.close()
    
    def _switch_to_english(self, response: requests.Response, base_url: str):
        """Switch the website to English language."""
        try:
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Check if already in English
            active_lang = soup.find("a", class_="lang_main_active")
            if active_lang and "English" in active_lang.get_text():
                self._english_switched = True
                logger.debug("Already in English language")
                return
        
            # Extract ALL hidden form fields
            form_data = {}
            form = soup.find("form")
            if form:
                for inp in form.find_all("input", type="hidden"):
                    name = inp.get("name", "")
                    value = inp.get("value", "")
                    if name:
                        form_data[name] = value
            
            # Set language switch event with exact parameters
            form_data["__EVENTTARGET"] = "ctl00$ctlLang1$lbEnglish"
            form_data["__EVENTARGUMENT"] = ""
            
            # Use browser-like headers (no XMLHttpRequest)
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://gzk.rks-gov.net",
                "Referer": base_url,
                "Cache-Control": "max-age=0",
                "Upgrade-Insecure-Requests": "1"
            }
            
            # Post language switch request and follow redirects
            switch_response = self.session.post(base_url, data=form_data, headers=headers, allow_redirects=True)
            switch_response.raise_for_status()
            
            # Mark as switched - the caller will make a fresh request
            self._english_switched = True
            
        except Exception as e:
            logger.warning(f"Failed to switch to English language: {e}")
            # Mark as switched anyway to avoid infinite loops
            self._english_switched = True


class BatchProcessor:
    """Handles batch processing with statistics tracking."""
    
    def __init__(self, config: BatchConfig, session: Session):
        self.config = config
        self.session = session
        self.stats = PipelineStats()
    
    def process_batch(self, items: List[Any], processor: Callable[[Any], Dict[str, Any]]) -> PipelineStats:
        """Process a batch of items with the given processor function."""
        batch_stats = PipelineStats()
        
        for i, item in enumerate(items):
            try:
                result = processor(item)
                self._update_stats(batch_stats, result)
                
                # Commit periodically
                if (i + 1) % self.config.commit_frequency == 0:
                    self._commit_with_error_handling()
                
                # Log progress
                if (i + 1) % self.config.progress_log_frequency == 0:
                    logger.info(f"Processed {i + 1}/{len(items)} items in batch")
                    
            except Exception as e:
                logger.error(f"Error processing item {i}: {e}")
                batch_stats.total_errors += 1
                self._rollback_with_error_handling()
        
        # Final commit
        self._commit_with_error_handling()
        return batch_stats
    
    def _update_stats(self, stats: PipelineStats, result: Dict[str, Any]):
        """Update statistics based on processing result."""
        status = result.get('status', 'error')
        
        if status == 'processed':
            stats.total_processed += 1
        elif status == 'new':
            stats.total_new += 1
        elif status == 'updated':
            stats.total_updated += 1
        elif status == 'skipped':
            stats.total_skipped += 1
        else:
            stats.total_errors += 1
    
    def _commit_with_error_handling(self):
        """Commit with error handling."""
        try:
            self.session.commit()
        except SQLAlchemyError as e:
            logger.error(f"Database commit error: {e}")
            self.session.rollback()
            raise
    
    def _rollback_with_error_handling(self):
        """Rollback with error handling."""
        try:
            self.session.rollback()
        except SQLAlchemyError as e:
            logger.error(f"Database rollback error: {e}")


class ThreadedBatchProcessor(BatchProcessor):
    """Threaded version of BatchProcessor for parallel processing."""
    
    def __init__(self, config: BatchConfig, session_factory: Callable[[], Session], max_workers: int = 4):
        # Don't call super().__init__ since we handle session differently
        self.config = config
        self.session_factory = session_factory
        self.max_workers = max_workers
        self.stats = PipelineStats()
        self.stats_lock = Lock()
    
    def process_batch(self, items: List[Any], processor_factory: Callable[[Session], Callable[[Any], Dict[str, Any]]]) -> PipelineStats:
        """Process a batch of items (IDs or objects) using threading."""
        batch_stats = PipelineStats()
        
        # Create smaller chunks for threading
        chunk_size = max(1, len(items) // self.max_workers)
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
        
        logger.info(f"Processing {len(items)} items in {len(chunks)} chunks using {self.max_workers} threads")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunks to the thread pool
            future_to_chunk = {
                executor.submit(self._process_chunk, chunk, processor_factory): chunk
                for chunk in chunks
            }
            
            # Process completed chunks
            for future in as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                try:
                    chunk_stats = future.result()
                    with self.stats_lock:
                        batch_stats.add_stats(chunk_stats)
                except Exception as e:
                    logger.error(f"Error processing chunk: {e}")
                    with self.stats_lock:
                        batch_stats.total_errors += len(chunk)
        
        return batch_stats
    
    def _process_chunk(self, items: List[Any], processor_factory: Callable[[Session], Callable[[Any], Dict[str, Any]]]) -> PipelineStats:
        """Process a chunk of items in a single thread."""
        chunk_stats = PipelineStats()
        session = None
        
        try:
            # Create a new session for this thread
            session = self.session_factory()
            processor = processor_factory(session)
            
            # Check if items are IDs (integers) or actual items
            if items and isinstance(items[0], (int, str)):
                # Items are IDs - need to load from database
                # Get the model class from the processor
                if hasattr(processor, '__self__') and hasattr(processor.__self__, 'get_model_class'):
                    model_class = processor.__self__.get_model_class()
                else:
                    # Fallback - assume it's Law
                    from models import Law
                    model_class = Law
                
                for i, item_id in enumerate(items):
                    try:
                        # Load the item in this thread's session
                        item = session.query(model_class).filter_by(id=item_id).first()
                        if item is None:
                            logger.warning(f"Item with ID {item_id} not found in thread")
                            chunk_stats.total_errors += 1
                            continue
                        
                        result = processor(item)
                        self._update_stats(chunk_stats, result)
                        
                        # Commit periodically
                        if (i + 1) % self.config.commit_frequency == 0:
                            session.commit()
                            
                    except Exception as e:
                        logger.error(f"Error processing item in thread: {e}")
                        chunk_stats.total_errors += 1
                        try:
                            session.rollback()
                        except Exception:
                            pass
            else:
                # Items are actual objects (like tuples) - use directly
                for i, item in enumerate(items):
                    try:
                        result = processor(item)
                        self._update_stats(chunk_stats, result)
                        
                        # Commit periodically
                        if (i + 1) % self.config.commit_frequency == 0:
                            session.commit()
                            
                    except Exception as e:
                        logger.error(f"Error processing item in thread: {e}")
                        chunk_stats.total_errors += 1
                        try:
                            session.rollback()
                        except Exception:
                            pass
            
            # Final commit for this chunk
            session.commit()
            
        except Exception as e:
            logger.error(f"Error in thread chunk processing: {e}")
            if session:
                try:
                    session.rollback()
                except Exception:
                    pass
            chunk_stats.total_errors += len(items)
        finally:
            if session:
                try:
                    session.close()
                except Exception:
                    pass
        
        return chunk_stats


class BasePipelineProcessor(ABC):
    """Abstract base class for all pipeline processors."""
    
    def __init__(self, session: Session):
        self.session = session
        self.stats = PipelineStats()
    
    @abstractmethod
    def get_retry_config(self) -> RetryConfig:
        """Get retry configuration for this processor."""
        pass
    
    @abstractmethod
    def get_batch_config(self) -> BatchConfig:
        """Get batch configuration for this processor."""
        pass
    
    @abstractmethod
    def process_single_item(self, item: Any) -> Dict[str, Any]:
        """Process a single item. Must return dict with 'status' key."""
        pass
    
    @abstractmethod
    def get_items_to_process(self) -> List[Any]:
        """Get list of items to process."""
        pass
    
    def get_session_factory(self) -> Callable[[], Session]:
        """Get a session factory for creating new sessions in threads."""
        # Import here to avoid circular imports
        from db import get_session
        return get_session
    
    def create_processor_instance(self, session: Session) -> 'BasePipelineProcessor':
        """Create a new processor instance with the given session."""
        # Create a new instance of the same class
        return self.__class__(session)
    
    def run(self):
        """Main entry point for the processor."""
        logger.info(f"Starting {self.__class__.__name__}")
        
        try:
            items = self.get_items_to_process()
            logger.info(f"Found {len(items)} items to process")
            
            if not items:
                logger.info("No items to process")
                return
            
            batch_config = self.get_batch_config()
            
            # Choose between threaded and non-threaded processing
            if CONFIG.enable_threading and len(items) > 1:
                logger.info(f"Using threaded processing with {CONFIG.max_workers} workers")
                session_factory = self.get_session_factory()
                batch_processor = ThreadedBatchProcessor(
                    batch_config, 
                    session_factory, 
                    max_workers=CONFIG.max_workers
                )
                
                # Create processor factory for threading
                def processor_factory(session: Session) -> Callable[[Any], Dict[str, Any]]:
                    processor_instance = self.create_processor_instance(session)
                    return processor_instance.process_single_item
                
                # Process in threaded batches
                for i in range(0, len(items), batch_config.batch_size):
                    batch = items[i:i + batch_config.batch_size]
                    batch_num = i // batch_config.batch_size + 1
                    
                    logger.info(f"Processing batch {batch_num} ({len(batch)} items)")
                    
                    try:
                        # Check if items have ID attributes for threading
                        if batch and hasattr(batch[0], 'id'):
                            # Extract IDs for threading (for database objects)
                            batch_ids = [item.id for item in batch]
                            batch_stats = batch_processor.process_batch(batch_ids, processor_factory)
                        else:
                            # Pass items directly for non-database objects (like tuples)
                            batch_stats = batch_processor.process_batch(batch, processor_factory)
                        
                        self.stats.add_stats(batch_stats)
                        
                        logger.info(f"Batch {batch_num} complete: {batch_stats}")
                        
                    except Exception as e:
                        logger.error(f"Error processing batch {batch_num}: {e}")
                        self.stats.total_errors += len(batch)
                    
                    # Server delay between batches
                    if i + batch_config.batch_size < len(items):
                        time.sleep(CONFIG.server_delay)
            else:
                # Use non-threaded processing
                logger.info("Using single-threaded processing")
                batch_processor = BatchProcessor(batch_config, self.session)
                
                # Process in batches
                for i in range(0, len(items), batch_config.batch_size):
                    batch = items[i:i + batch_config.batch_size]
                    batch_num = i // batch_config.batch_size + 1
                    
                    logger.info(f"Processing batch {batch_num} ({len(batch)} items)")
                    
                    try:
                        batch_stats = batch_processor.process_batch(batch, self.process_single_item)
                        self.stats.add_stats(batch_stats)
                        
                        logger.info(f"Batch {batch_num} complete: {batch_stats}")
                        
                    except Exception as e:
                        logger.error(f"Error processing batch {batch_num}: {e}")
                        self.stats.total_errors += len(batch)
                    
                    # Server delay between batches
                    if i + batch_config.batch_size < len(items):
                        time.sleep(CONFIG.server_delay)
            
            logger.info(f"{self.__class__.__name__} complete: {self.stats}")
            
        except Exception as e:
            logger.error(f"Critical error in {self.__class__.__name__}: {e}")
            raise PipelineError(f"Pipeline processor failed: {e}")
    
    @contextmanager
    def get_http_client(self) -> Iterator[HttpClient]:
        """Context manager for HTTP client."""
        client = HttpClient(self.get_retry_config())
        try:
            yield client
        finally:
            client.session.close()


class ValidationMixin:
    """Mixin for common validation functions."""
    
    def validate_act_id(self, act_id: Any) -> Optional[int]:
        """Validate and convert act_id to integer."""
        if act_id is None:
            return None
        
        try:
            if isinstance(act_id, int):
                return act_id if act_id > 0 else None
            
            if isinstance(act_id, str):
                clean_id = act_id.strip()
                if clean_id:
                    int_id = int(clean_id)
                    return int_id if int_id > 0 else None
            
            return None
        except (ValueError, TypeError):
            return None
    
    def validate_url(self, url: str) -> bool:
        """Validate URL format."""
        if not url or not isinstance(url, str):
            return False
        
        return url.startswith(('http://', 'https://'))
    
    def sanitize_text(self, text: str) -> str:
        """Sanitize text input."""
        if not text:
            return ""
        
        return text.strip()[:1000]  # Limit length 