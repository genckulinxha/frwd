import logging
from typing import Any, Iterator, Optional, List, Dict, Callable
from contextlib import contextmanager
import time
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError
from bs4 import BeautifulSoup, ParserRejectedMarkup
from sqlalchemy.orm import Session

from .config import CONFIG, RetryConfig, BatchConfig

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    pass


class RetryManager:
    def __init__(self, config: RetryConfig):
        self.config = config

    def retry_with_backoff(self, func: Callable[[], Any]) -> Any:
        attempt = 0
        delay = self.config.backoff_factor
        last_exception: Optional[Exception] = None
        while attempt < self.config.max_retries:
            try:
                logger.debug(f"HTTP attempt {attempt+1}/{self.config.max_retries}, timeout={self.config.timeout}s")
                start = time.time()
                result = func()
                elapsed = time.time() - start
                logger.debug(f"HTTP attempt {attempt+1} succeeded in {elapsed:.2f}s")
                return result
            except (RequestException, Timeout, ConnectionError) as e:
                last_exception = e
                attempt += 1
                logger.warning(f"HTTP attempt {attempt}/{self.config.max_retries} failed: {e}")
                if attempt >= self.config.max_retries:
                    break
                logger.debug(f"Sleeping {delay:.2f}s before retry")
                time.sleep(delay)
                delay *= self.config.backoff_factor
        if last_exception:
            logger.error(f"HTTP failed after {self.config.max_retries} attempts: {last_exception}")
            raise last_exception
        raise PipelineError("Retry attempts exhausted")


class HttpClient:
    def __init__(self, retry_config: RetryConfig):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": CONFIG.user_agent})
        self.retry_manager = RetryManager(retry_config)

    def get(self, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", self.retry_manager.config.timeout)

        def _get():
            logger.debug(f"GET {url} timeout={kwargs.get('timeout')} allow_redirects={kwargs.get('allow_redirects', True)}")
            response = self.session.get(url, **kwargs)
            status = response.status_code
            elapsed = getattr(response, 'elapsed', None)
            elapsed_s = elapsed.total_seconds() if elapsed else None
            cl = response.headers.get('Content-Length')
            redirects = len(response.history) if getattr(response, 'history', None) else 0
            logger.debug(f"GET done {url} status={status} redirects={redirects} elapsed={elapsed_s}s content_length={cl}")
            response.raise_for_status()
            return response

        return self.retry_manager.retry_with_backoff(_get)

    def post(self, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", self.retry_manager.config.timeout)

        def _post():
            logger.debug(f"POST {url} timeout={kwargs.get('timeout')}")
            response = self.session.post(url, **kwargs)
            status = response.status_code
            elapsed = getattr(response, 'elapsed', None)
            elapsed_s = elapsed.total_seconds() if elapsed else None
            cl = response.headers.get('Content-Length')
            logger.debug(f"POST done {url} status={status} elapsed={elapsed_s}s content_length={cl}")
            response.raise_for_status()
            return response

        return self.retry_manager.retry_with_backoff(_post)

    def parse_html(self, html_content: str) -> BeautifulSoup:
        if not html_content:
            raise ValueError("No HTML content provided")
        try:
            return BeautifulSoup(html_content, "html.parser")
        except ParserRejectedMarkup as e:
            raise PipelineError(f"HTML parsing rejected: {e}")
        except Exception as e:
            raise PipelineError(f"HTML parsing error: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self.session, "close"):
            self.session.close()


class PipelineStats:
    total_processed: int
    total_errors: int

    def __init__(self):
        self.total_processed = 0
        self.total_errors = 0

    def add_stats(self, other: "PipelineStats"):
        self.total_processed += other.total_processed
        self.total_errors += other.total_errors

    def __repr__(self) -> str:
        return f"processed={self.total_processed} errors={self.total_errors}"


class BasePipelineProcessor:
    def __init__(self, session: Session):
        self.session = session
        self.stats = PipelineStats()

    @classmethod
    def get_model_class(cls):
        raise NotImplementedError

    def get_retry_config(self) -> RetryConfig:
        raise NotImplementedError

    def get_batch_config(self) -> BatchConfig:
        raise NotImplementedError

    def get_items_to_process(self) -> List[Any]:
        raise NotImplementedError

    def process_single_item(self, item: Any) -> Dict[str, Any]:
        raise NotImplementedError

    @contextmanager
    def get_http_client(self) -> Iterator["HttpClient"]:
        client = HttpClient(self.get_retry_config())
        try:
            yield client
        finally:
            client.session.close()

    def run(self):
        items = self.get_items_to_process()
        logger.info(f"Processor {self.__class__.__name__}: {len(items)} items to process")
        if not items:
            logger.info("No items to process")
            return
        batch_size = self.get_batch_config().batch_size
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} items")
            for item in batch:
                try:
                    logger.debug(f"Processing item: {item}")
                    result = self.process_single_item(item)
                    logger.info(f"Processed item result: {result}")
                    self.stats.total_processed += 1
                except Exception as e:
                    logger.error(f"Error processing item: {e}")
                    self.stats.total_errors += 1
        logger.info(f"{self.__class__.__name__} complete: {self.stats}") 