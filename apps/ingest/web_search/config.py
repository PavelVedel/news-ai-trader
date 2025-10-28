"""Configuration for web search providers"""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load .env file from project root
project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / '.env'
if env_path.exists():
    load_dotenv(env_path)
    print(f"Loaded .env from {env_path}")
else:
    print(f".env file not found at {env_path}")

# Google Custom Search Engine API
GOOGLE_CSE_API_KEY: Optional[str] = os.getenv('GOOGLE_CUSTOM_SEARCH_ENGINE_API')
GOOGLE_CSE_ID: Optional[str] = os.getenv('GOOGLE_CUSTOM_SEARCH_ENGINE_ID')

# Provider rate limits (requests per second)
PROVIDER_RATE_LIMITS = {
    'wikipedia': 0.3,  # 0.5 RPS (2 sec intervals)
    'wikidata': 0.3,   # 0.5 RPS
    'duckduckgo': 0.1,  # 0.1 RPS (10 sec intervals, more conservative)
    'google_cse': 0.1,   # Very conservative, 100 queries/day limit
}

# Backoff configuration
BACKOFF_BASE_DELAY_MINUTES = 15
BACKOFF_MAX_DELAY_MINUTES = 60
BACKOFF_MAX_ATTEMPTS = 5

# Jitter configuration (±30%)
JITTER_MIN = 0.7
JITTER_MAX = 1.3

# Google CSE daily quota (free tier)
GOOGLE_CSE_DAILY_LIMIT = 100

# Validate configuration
if GOOGLE_CSE_API_KEY and not GOOGLE_CSE_ID:
    print("Warning: GOOGLE_CSE_ID not set, Google CSE will not work")
elif GOOGLE_CSE_ID and not GOOGLE_CSE_API_KEY:
    print("Warning: GOOGLE_CUSTOM_SEARCH_ENGINE_API not set, Google CSE will not work")

print(f"Google CSE configured: {bool(GOOGLE_CSE_API_KEY and GOOGLE_CSE_ID)}")

