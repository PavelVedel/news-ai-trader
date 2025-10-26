"""Rate limiting for web search providers"""

import time
import threading
import random
from typing import Dict, Optional
from datetime import datetime, timezone, timedelta


class RateLimiter:
    """
    Rate limiter for search providers with jitter and backoff
    
    Features:
    - Configurable RPS per provider
    - Random jitter (±30%)
    - Thread-safe
    - Exponential backoff tracking
    """
    
    def __init__(self):
        self._locks: Dict[str, threading.Lock] = {}
        self._last_request: Dict[str, float] = {}
        self._backoff_until: Dict[str, Optional[datetime]] = {}
        
    def _get_lock(self, provider: str) -> threading.Lock:
        """Get or create lock for provider"""
        if provider not in self._locks:
            self._locks[provider] = threading.Lock()
            self._last_request[provider] = 0.0
            self._backoff_until[provider] = None
        return self._locks[provider]
    
    def set_backoff(self, provider: str, delay_minutes: int):
        """Set backoff period for provider"""
        with self._get_lock(provider):
            self._backoff_until[provider] = datetime.now(timezone.utc) + timedelta(minutes=delay_minutes)
    
    def clear_backoff(self, provider: str):
        """Clear backoff period for provider"""
        with self._get_lock(provider):
            self._backoff_until[provider] = None
    
    def wait_if_needed(self, provider: str, rps: float, jitter: tuple[float, float] = (0.7, 1.3)):
        """
        Wait if needed to respect rate limit
        
        Args:
            provider: Provider name
            rps: Requests per second (rate limit)
            jitter: Jitter range (min, max) multiplier
        """
        with self._get_lock(provider):
            # Check backoff
            if self._backoff_until[provider] is not None:
                if datetime.now(timezone.utc) < self._backoff_until[provider]:
                    # Still in backoff
                    raise RateLimitError(
                        f"Provider {provider} is in backoff until {self._backoff_until[provider]}"
                    )
                else:
                    # Backoff expired, clear it
                    self._backoff_until[provider] = None
            
            # Calculate required wait time
            now = time.time()
            elapsed = now - self._last_request[provider]
            min_interval = 1.0 / rps
            
            # Apply jitter
            jitter_mult = random.uniform(*jitter)
            target_interval = min_interval * jitter_mult
            
            if elapsed < target_interval:
                wait_time = target_interval - elapsed
                time.sleep(wait_time)
            
            self._last_request[provider] = time.time()


class RateLimitError(Exception):
    """Raised when provider is rate-limited"""
    pass

