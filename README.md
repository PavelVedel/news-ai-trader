# News AI Trader

LLM-based news analysis system for predicting stock price movements based on financial news analysis.

> ⚠️ **Status**: This project is currently under active development. APIs and workflows may change. Use at your own risk.

## Overview

This project creates a comprehensive pipeline to collect, analyze, and extract insights from financial news to predict potential price changes for speculation. The system combines news data, market data, fundamentals, and LLM-based analysis to identify key actors, events, and their potential market impact.

## Architecture

The system follows a multi-stage pipeline:

```
[REST] → [Market Data] → [Fundamentals] → [LLM Analysis] → [Entity Extraction] → [Web Search]
```

## Workflow

### 1. **[REST] News Collection** (`apps/ingest/rest_news_collector.py`)

Downloads financial news from Alpaca via REST API.

- **Process**: Starts with general news, extracts symbols, recursively collects 50 latest news per symbol
- **Storage**: `data/db/news.db` → `news_raw` table
- **Features**:
  - Fetches up to 50 news items per request
  - Automatic symbol discovery and recursion
  - Deduplication by hash
  - Stores headline, text, symbols, timestamps

### 2. **[REST] Market Data (Klines)** (`apps/market_data/update_market_data.py`)

Loads 1-minute candlestick data from Yahoo Finance.

- **Process**: Fetches klines for all symbols found in news
- **Limitation**: Yahoo provides only last 7 days of 1m data
- **Storage**: 
  - Format: Parquet files per day
  - Location: `data/market_data/yahoo/1m/{symbol}/YYYY-MM-DD.parquet`
- **Usage**: Provides price data for correlation analysis with news timestamps

### 3. **[REST] Fundamentals** (`apps/market_data/update_fundamentals.py`)

Downloads fundamental financial data from Yahoo Finance.

- **Process**: Fetches comprehensive financial metrics for each symbol
- **Storage**: `data/db/news.db` → `fundamentals` table
- **Data Includes**:
  - Valuation metrics (P/E, P/B, P/S, EV/Revenue, etc.)
  - Profitability (ROE, ROA, ROC)
  - Liquidity ratios (Current, Quick, D/E)
  - Dividend metrics
  - Growth indicators
  - Technical analysis data (52-week highs/lows, moving averages)

### 4. **[REST] Company Info** (`apps/market_data/update_infos.py`)

Fetches detailed company information from Yahoo Finance.

- **Process**: Downloads company metadata including names, descriptions, officers
- **Storage**: `data/db/news.db` → `infos` table
- **Data Includes**:
  - Company names (long, short, display)
  - Business summary
  - Officers (with roles)
  - Contact info (website, address, phone)
  - Employee count
  - Sector/industry
  - Exchange information

### 5. **[LLM][Stage A] News Analysis** (`apps/ai/perform_stage_a_news_analyzation.py`)

Extracts structured information from news using LLM.

- **Process**: 
  - Uses LLM to extract: actors (who), events (what), context (how)
  - Identifies symbol mentions in text
  - Detects unresolved entities
  - Determines grounding (does news relate to symbols?)
- **Storage**: `data/db/news.db` → `news_analysis_a` table
- **Pipeline**: `apps/ai/pipelines/news_analyzer_2.py`
- **Output**:
  - `actors`: JSON array of extracted people/organizations
  - `event`: Event details object
  - `symbol_mentions_in_text`: Symbols mentioned in content
  - `symbol_not_mentioned_in_text`: Input symbols not found
  - `unresolved_entities`: Entities needing further search
  - `is_news_grounded`: Boolean (does news relate to symbols)

### 6. **[Stage B] Entity Alias Formation** (`apps/ai/perform_stage_b_entity_alias_formation.py`)

Populates entity tables and creates searchable aliases.

- **Process**: 
  - Extracts entities from `news_analysis_a`
  - Normalizes person names (family, given, initials, prefixes)
  - Creates FTS5 full-text search indexes
  - Builds affiliation relationships
- **Storage**: `data/db/news.db` → Creates tables:
  - `entities`: Entity master table
  - `aliases`: Normalized name variants
  - `affiliations`: Person-Organization relationships
  - `alias_fts*`: FTS5 search indexes
- **Features**:
  - Name normalization (removes titles, handles variations)
  - FTS search for fuzzy entity matching
  - Helper functions: `find_entity_by_symbol`, `find_entity_by_alias`, `find_person_by_name`, `find_person_affiliations`

### 7. **[Web Search] Entity Resolution** (`apps/ingest/web_search/populate_cache.py`)

Searches web for unresolved entities not found in existing data.

- **Process**: 
  - Takes entities from `not_found_entities.xlsx`
  - Skips already cached results
  - Cascades through multiple providers
- **Providers** (in priority order):
  - **Wikipedia**: Primary source for general entities
  - **Wikidata**: Structured data for entities
  - **DuckDuckGo**: Fallback HTML scraping
  - **Google CSE**: Rare usage (quota-limited)
- **Smart Filtering**:
  - Symbol entities: Skips Wikipedia/Wikidata (uses DuckDuckGo + Google)
  - Other entities: Uses all providers (Wikipedia prioritized)
- **Storage**: `data/db/news.db` → `web_search_cache` table
- **Commands**:
  ```bash
  # Default: skip all cached entities
  python apps/ingest/web_search/populate_cache.py
  
  # Retry specific statuses
  python apps/ingest/web_search/populate_cache.py --retry-statuses error pending
  
  # Skip cache entirely
  python apps/ingest/web_search/populate_cache.py --skip-filter
  ```

## Project Structure

```
news-ai-trader/
├── apps/
│   ├── ingest/              # News collection
│   │   ├── alpaca_client/    # Alpaca API client
│   │   ├── web_search/      # Web search for entities
│   │   └── rest_news_collector.py
│   ├── market_data/         # Market data collection
│   │   ├── storage/         # File-based storage (parquet)
│   │   ├── yahoo/           # Yahoo Finance client
│   │   ├── update_market_data.py
│   │   ├── update_fundamentals.py
│   │   ├── update_infos.py
│   │   └── find_anomaly_news.py
│   └── ai/                   # AI/LLM processing
│       ├── inference/        # LLM clients (LM Studio)
│       ├── pipelines/        # Analysis pipelines
│       └── perform_stage_a_news_analyzation.py
│       └── perform_stage_b_entity_alias_formation.py
├── libs/
│   ├── database/            # Database layer
│   │   ├── connection.py
│   │   ├── schema.sql
│   │   ├── entities.sql
│   │   └── news_analysis.sql
│   └── utils/               # Utilities
├── data/
│   ├── db/
│   │   └── news.db          # SQLite database
│   └── market_data/yahoo/1m # Parquet files
├── logs/                    # Application logs
└── README.md
```

## Database Schema

### Core Tables

- **news_raw**: Raw news from Alpaca
- **news_analysis_a**: Stage A LLM analysis results
- **fundamentals**: Financial metrics from Yahoo
- **infos**: Company information
- **entities**: Extracted entities
- **aliases**: Normalized entity names for search
- **affiliations**: Entity relationships
- **web_search_cache**: Cached web search results

## Setup

### Prerequisites

- Python 3.8+
- Alpaca API credentials (for news)
- LM Studio or compatible LLM (for Stage A)

### Installation

```bash
pip install -r requirements.txt
# or
pip install -e .
```

### Configuration

Create `.env` file or configure in `pyproject.toml`:

```python
ALPACA_API_KEY=your_key
ALPACA_API_SECRET=your_secret
GOOGLE_CSE_API_KEY=your_key  # Optional for web search
GOOGLE_CSE_ID=your_id
```

## Usage

### Run Complete Pipeline

```bash
# 1. Collect news
python apps/ingest/rest_news_collector.py

# 2. Download market data
python apps/market_data/update_market_data.py

# 3. Download fundamentals
python apps/market_data/update_fundamentals.py

# 4. Download company info
python apps/market_data/update_infos.py

# 5. Stage A: LLM analysis
python apps/ai/perform_stage_a_news_analyzation.py

# 6. Stage B: Entity extraction
python apps/ai/perform_stage_b_entity_alias_formation.py

# 7. Web search for unresolved entities
python apps/ingest/web_search/populate_cache.py
```

### Batch Update

```bash
python update_all_data.bat  # Windows
```

## Key Features

### Smart Entity Resolution
- Automatically detects entities from news
- Normalizes person names (handles titles, initials, suffixes)
- Creates searchable alias tables
- Web search for unknown entities

### Provider Cascade
- Automatic fallback between search providers
- Rate limiting and backoff handling
- Quota management for paid APIs
- Smart caching to avoid redundant searches

### Market Data Integration
- Correlates news timestamps with price movements
- Only analyzes news with available candle data
- Stores 1m granularity for precise timing

### LLM Analysis
- Extracts structured information from unstructured news
- Identifies actors and events
- Links to company symbols
- Detects grounding (relationship between news and symbols)

## Development

### Database Utilities

```python
from libs.database.connection import DatabaseConnection

db = DatabaseConnection("data/db/news.db")

# Get news
news = db.get_news_by_id(123)

# Get symbols
symbols = db.get_all_symbols()

# Search entities
entity = db.find_entity_by_symbol("AAPL")
person = db.find_person_by_name("Tim Cook", fuzzy=True)
```

### Running Tests

```bash
pytest apps/ingest/tests/
pytest apps/ai/tests/
```

## License

See `LICENSE` file for details.

## Contributing

This is a research project for analyzing financial news with LLMs. Contributions welcome!
