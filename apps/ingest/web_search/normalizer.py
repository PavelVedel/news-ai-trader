"""Query normalization for web search"""

import unicodedata
import re


def normalize_query(s: str) -> str:
    """
    Normalize search query for consistent caching and matching
    
    Args:
        s: Raw query string
        
    Returns:
        Normalized query string (lowercase, NFKC, collapsed whitespace)
    """
    # Unicode normalization (NFKC - compatibility, composed)
    s = unicodedata.normalize("NFKC", s)
    
    # Strip leading/trailing whitespace
    s = s.strip()
    
    # Collapse multiple whitespace into single space
    s = re.sub(r"\s+", " ", s)
    
    # Lowercase
    return s.lower()

