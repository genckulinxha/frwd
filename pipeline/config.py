import os
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class RetryConfig:
    """Configuration for retry logic."""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    timeout: int = 30


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    batch_size: int = 50
    commit_frequency: int = 10
    progress_log_frequency: int = 100


@dataclass
class PipelineConfig:
    """Main pipeline configuration."""
    data_directory: str = "data"
    max_consecutive_errors: int = 5
    server_delay: float = 0.5
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    
    # Threading configuration
    enable_threading: bool = True
    max_workers: int = 4
    
    # Retry configurations
    discovery_retry: RetryConfig = RetryConfig(max_retries=3, timeout=15)
    detail_retry: RetryConfig = RetryConfig(max_retries=3, timeout=30)
    relations_retry: RetryConfig = RetryConfig(max_retries=3, timeout=15)
    
    # Batch configurations
    discovery_batch: BatchConfig = BatchConfig(batch_size=100)
    detail_batch: BatchConfig = BatchConfig(batch_size=50)
    relations_batch: BatchConfig = BatchConfig(batch_size=50)
    
    # URLs and constants
    base_url: str = "https://gzk.rks-gov.net/"
    category_urls: Dict[str, str] = None
    
    def __post_init__(self):
        if self.category_urls is None:
            self.category_urls = {
                # "LawInForce": "https://gzk.rks-gov.net/LawInForceList.aspx",
                # "InternationalAgreements": "https://gzk.rks-gov.net/InternationalAgreementsList.aspx",
                # "SubNormActs": "https://gzk.rks-gov.net/ListOfSubNormActs.aspx",
                "LocalInstActs": "https://gzk.rks-gov.net/LocalInstActsList.aspx",
                # "ActsOfConstitutionalCourt": "https://gzk.rks-gov.net/ActsOftheConstitutionalCourtList.aspx",
                # "CourtInstActs": "https://gzk.rks-gov.net/CourtInstActsList.aspx",
                # "SearchIndex119": "https://gzk.rks-gov.net/SearchIn.aspx?Index=1&CatID=119,0",
                # Add other categories as needed
            }


@dataclass
class PipelineStats:
    """Statistics tracking for pipeline operations."""
    total_processed: int = 0
    total_new: int = 0
    total_updated: int = 0
    total_errors: int = 0
    total_skipped: int = 0
    
    def reset(self):
        """Reset all counters."""
        self.total_processed = 0
        self.total_new = 0
        self.total_updated = 0
        self.total_errors = 0
        self.total_skipped = 0
    
    def add_stats(self, other: 'PipelineStats'):
        """Add stats from another instance."""
        self.total_processed += other.total_processed
        self.total_new += other.total_new
        self.total_updated += other.total_updated
        self.total_errors += other.total_errors
        self.total_skipped += other.total_skipped
    
    def __str__(self) -> str:
        return f"Stats(processed={self.total_processed}, new={self.total_new}, updated={self.total_updated}, errors={self.total_errors}, skipped={self.total_skipped})"


# Global configuration instance
CONFIG = PipelineConfig() 