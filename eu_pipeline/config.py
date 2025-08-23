from dataclasses import dataclass
from typing import Dict, List


@dataclass
class RetryConfig:
    max_retries: int = 3
    timeout: int = 20
    backoff_factor: float = 1.5


@dataclass
class BatchConfig:
    batch_size: int = 50
    progress_log_frequency: int = 100


@dataclass
class PipelineConfig:
    data_directory: str = "data_eu"
    max_consecutive_errors: int = 5
    server_delay: float = 0.7
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    enable_threading: bool = True
    max_workers: int = 4

    discovery_retry: RetryConfig = RetryConfig(max_retries=3, timeout=20)
    detail_retry: RetryConfig = RetryConfig(max_retries=3, timeout=40)
    relations_retry: RetryConfig = RetryConfig(max_retries=3, timeout=20)

    discovery_batch: BatchConfig = BatchConfig(batch_size=100)
    detail_batch: BatchConfig = BatchConfig(batch_size=50)
    relations_batch: BatchConfig = BatchConfig(batch_size=50)

    base_url: str = "https://eur-lex.europa.eu/legal-content/EN/TXT/"
    list_url: str = "https://eur-lex.europa.eu/search.html?name=browse-by:legislation-in-force&type=named&displayProfile=allRelAllConsDocProfile&CC_1_CODED={category}&page={page}"

    # Top-level directory codes 01..20
    category_codes: List[str] = None

    def __post_init__(self):
        if self.category_codes is None:
            self.category_codes = [f"{i:02d}" for i in range(1, 21)]


CONFIG = PipelineConfig() 