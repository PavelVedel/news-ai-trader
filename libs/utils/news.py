from __future__ import annotations
from typing import TypedDict, List, Optional

class NewsItem(TypedDict, total=False):
    id: int
    headline: str
    summary: Optional[str]
    symbols: List[str]
    source: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]
    url: Optional[str]
