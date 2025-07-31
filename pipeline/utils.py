from datetime import datetime
import logging
from typing import Optional
from bs4 import Tag

logger = logging.getLogger(__name__)

def parse_date(text: str) -> Optional[datetime]:
    """Parse date string with comprehensive error handling."""
    
    if not text:
        logger.debug("⚠️ Empty date text provided")
        return None
    
    try:
        # Clean the text first
        clean_text = text.strip()
        if not clean_text:
            logger.debug("⚠️ Date text is empty after stripping")
            return None
        
        # Try the expected format first
        try:
            return datetime.strptime(clean_text, "%d.%m.%Y")
        except ValueError:
            # Try alternative formats
            alternative_formats = [
                "%d/%m/%Y",
                "%d-%m-%Y", 
                "%Y-%m-%d",
                "%d.%m.%y",
                "%d/%m/%y",
                "%d-%m-%y"
            ]
            
            for fmt in alternative_formats:
                try:
                    return datetime.strptime(clean_text, fmt)
                except ValueError:
                    continue
            
            # If all formats fail, log the problematic text
            logger.warning(f"⚠️ Could not parse date: '{clean_text}'")
            return None
            
    except AttributeError as e:
        logger.warning(f"⚠️ AttributeError parsing date '{text}': {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error parsing date '{text}': {e}")
        return None

def safe_strip(elem) -> str:
    """Safely extract and strip text from HTML element with error handling."""
    
    if elem is None:
        logger.debug("⚠️ Element is None")
        return ""
    
    try:
        # Handle different types of elements
        if isinstance(elem, Tag):
            # BeautifulSoup Tag object
            text = elem.get_text(strip=True)
            if text is None:
                logger.debug("⚠️ Element.get_text() returned None")
                return ""
            return text
        elif isinstance(elem, str):
            # Already a string
            return elem.strip()
        elif hasattr(elem, 'text'):
            # Object with text attribute
            text = elem.text
            if text is None:
                logger.debug("⚠️ Element.text is None")
                return ""
            return text.strip()
        elif hasattr(elem, 'get_text'):
            # Object with get_text method
            text = elem.get_text(strip=True)
            if text is None:
                logger.debug("⚠️ Element.get_text() returned None")
                return ""
            return text
        else:
            # Try to convert to string
            text = str(elem).strip()
            logger.debug(f"⚠️ Unknown element type, converted to string: '{text}'")
            return text
            
    except AttributeError as e:
        logger.warning(f"⚠️ AttributeError extracting text from element: {e}")
        return ""
    except Exception as e:
        logger.error(f"❌ Unexpected error extracting text from element: {e}")
        return ""

def validate_act_id(act_id) -> Optional[int]:
    """Validate and convert act_id to integer with error handling."""
    
    if act_id is None:
        logger.debug("⚠️ act_id is None")
        return None
    
    try:
        # If it's already an integer, validate it
        if isinstance(act_id, int):
            if act_id <= 0:
                logger.warning(f"⚠️ Invalid act_id: {act_id} (must be positive)")
                return None
            return act_id
        
        # If it's a string, try to convert
        if isinstance(act_id, str):
            clean_id = act_id.strip()
            if not clean_id:
                logger.warning("⚠️ Empty act_id string")
                return None
            
            try:
                int_id = int(clean_id)
                if int_id <= 0:
                    logger.warning(f"⚠️ Invalid act_id: {int_id} (must be positive)")
                    return None
                return int_id
            except ValueError:
                logger.warning(f"⚠️ Could not convert act_id to integer: '{clean_id}'")
                return None
        
        # Try to convert other types to string first, then to int
        try:
            str_id = str(act_id).strip()
            int_id = int(str_id)
            if int_id <= 0:
                logger.warning(f"⚠️ Invalid act_id: {int_id} (must be positive)")
                return None
            return int_id
        except (ValueError, TypeError):
            logger.warning(f"⚠️ Could not convert act_id to integer: {act_id}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Unexpected error validating act_id '{act_id}': {e}")
        return None

def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe file system operations."""
    
    if not filename:
        logger.debug("⚠️ Empty filename provided")
        return "unnamed"
    
    try:
        # Remove or replace problematic characters
        invalid_chars = '<>:"/\\|?*'
        sanitized = filename
        
        for char in invalid_chars:
            sanitized = sanitized.replace(char, '_')
        
        # Remove leading/trailing whitespace and dots
        sanitized = sanitized.strip('. ')
        
        # Ensure it's not empty after sanitization
        if not sanitized:
            logger.warning(f"⚠️ Filename became empty after sanitization: '{filename}'")
            return "unnamed"
        
        # Limit length to avoid filesystem issues
        max_length = 200
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length]
            logger.debug(f"⚠️ Filename truncated to {max_length} characters")
        
        return sanitized
        
    except Exception as e:
        logger.error(f"❌ Error sanitizing filename '{filename}': {e}")
        return "unnamed"
