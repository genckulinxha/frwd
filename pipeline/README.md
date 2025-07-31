# Pipeline Architecture Documentation

## Overview

This document describes the improved pipeline architecture for the law processing system. The pipeline is designed to be modular, configurable, and maintainable.

## Architecture

### Core Components

#### 1. Configuration System (`config.py`)
- **Purpose**: Centralized configuration management
- **Features**:
  - Retry configurations for each phase
  - Batch processing settings
  - URL and timeout configurations
  - Statistics tracking

#### 2. Base Classes (`base.py`)
- **Purpose**: Common functionality for all pipeline processors
- **Components**:
  - `BasePipelineProcessor`: Abstract base class for all processors
  - `HttpClient`: HTTP client with retry logic
  - `BatchProcessor`: Handles batch processing with statistics
  - `RetryManager`: Manages retry logic with exponential backoff
  - `ValidationMixin`: Common validation functions

#### 3. Pipeline Processors

##### Discovery Processor (`discovery/discover_laws_v2.py`)
- **Purpose**: Discover and catalog laws from various categories
- **Features**:
  - Pagination handling
  - Deduplication
  - Category-based processing
  - Robust error handling

##### Detail Processor (`detail/process_laws_v2.py`)
- **Purpose**: Extract metadata and download PDFs for laws
- **Features**:
  - Metadata extraction
  - PDF download and validation
  - Text extraction from PDFs
  - Comprehensive error handling

##### Relations Processor (`relations/backfill_relations_v2.py`)
- **Purpose**: Extract and store relationships between laws
- **Features**:
  - Relation type detection
  - Automatic target law creation
  - Duplicate relation prevention
  - Comprehensive error handling

## Key Improvements

### 1. **Eliminated Code Duplication**
- Common functionality extracted to base classes
- Consistent error handling across all processors
- Shared HTTP client with retry logic

### 2. **Configuration Management**
- All settings centralized in `config.py`
- Environment-specific configurations possible
- Easy to modify behavior without code changes

### 3. **Better Error Handling**
- Exponential backoff for HTTP requests
- Graceful degradation on failures
- Comprehensive logging and statistics

### 4. **Improved Testability**
- Clear separation of concerns
- Abstract base classes for easy mocking
- Dependency injection patterns

### 5. **Enhanced Monitoring**
- Detailed statistics tracking
- Progress reporting
- Comprehensive logging

## Usage

### Basic Usage
```python
from pipeline.discovery.discover_laws_v2 import discover_laws
from pipeline.detail.process_laws_v2 import process_unprocessed_laws
from pipeline.relations.backfill_relations_v2 import backfill_relations

# Run discovery
discover_laws()

# Process details
session = get_session()
process_unprocessed_laws(session)

# Backfill relations
backfill_relations(session)
```

### Configuration
```python
from pipeline.config import CONFIG

# Modify retry settings
CONFIG.discovery_retry.max_retries = 5
CONFIG.discovery_retry.timeout = 30

# Modify batch settings
CONFIG.discovery_batch.batch_size = 200
CONFIG.detail_batch.batch_size = 25
```

## Configuration Reference

### RetryConfig
- `max_retries`: Maximum number of retry attempts (default: 3)
- `base_delay`: Base delay between retries in seconds (default: 1.0)
- `max_delay`: Maximum delay between retries in seconds (default: 60.0)
- `exponential_base`: Exponential backoff base (default: 2.0)
- `timeout`: Request timeout in seconds (default: 30)

### BatchConfig
- `batch_size`: Number of items to process in each batch (default: 50)
- `commit_frequency`: How often to commit during batch processing (default: 10)
- `progress_log_frequency`: How often to log progress (default: 100)

### PipelineConfig
- `data_directory`: Directory for storing downloaded files (default: "data")
- `max_consecutive_errors`: Maximum consecutive errors before stopping (default: 5)
- `server_delay`: Delay between requests to avoid overwhelming server (default: 0.5)
- `user_agent`: User agent string for HTTP requests
- `category_urls`: Dictionary of category names to URLs

## Error Handling

### Retry Logic
- Exponential backoff with jitter
- Configurable maximum retries
- Different retry policies for different operations

### Error Recovery
- Graceful degradation on partial failures
- Automatic rollback on database errors
- Comprehensive error logging

### Statistics Tracking
- Total items processed
- Success/failure counts
- Processing time metrics
- Error categorization

## Performance Considerations

### Batch Processing
- Configurable batch sizes
- Periodic commits to avoid long transactions
- Progress tracking for long-running operations

### Rate Limiting
- Configurable delays between requests
- Exponential backoff on failures
- Respectful server interaction

### Memory Management
- Streaming processing for large datasets
- Periodic cleanup of resources
- Efficient data structures

## Monitoring and Logging

### Log Levels
- `DEBUG`: Detailed processing information
- `INFO`: General progress and statistics
- `WARNING`: Recoverable errors and issues
- `ERROR`: Critical errors requiring attention

### Statistics
- Processing counts by type
- Success/failure rates
- Performance metrics
- Error summaries

## Migration Guide

### From Old Pipeline
1. Update imports to use `_v2` versions
2. Review and update configuration settings
3. Test with small batch sizes initially
4. Monitor logs for any issues

### Backward Compatibility
- Old pipeline files remain unchanged
- New pipeline can run alongside old one
- Gradual migration possible

## Best Practices

### Configuration
- Use environment variables for sensitive settings
- Keep configuration files in version control
- Document all configuration changes

### Error Handling
- Always check return values
- Log errors with sufficient context
- Implement appropriate retry strategies

### Performance
- Start with smaller batch sizes
- Monitor system resources
- Adjust timeouts based on network conditions

### Monitoring
- Set up log aggregation
- Monitor error rates
- Track performance metrics

## Troubleshooting

### Common Issues
1. **Connection Timeouts**: Increase timeout values
2. **Rate Limiting**: Increase delays between requests
3. **Memory Issues**: Reduce batch sizes
4. **Database Locks**: Reduce commit frequency

### Debug Mode
```python
import logging
logging.getLogger('pipeline').setLevel(logging.DEBUG)
```

### Performance Tuning
```python
# Increase batch size for better performance
CONFIG.discovery_batch.batch_size = 200

# Reduce server delay if server can handle it
CONFIG.server_delay = 0.1

# Adjust retry settings
CONFIG.discovery_retry.max_retries = 5
CONFIG.discovery_retry.timeout = 45
``` 