-- News analysis table for storing LLM-processed news data
CREATE TABLE IF NOT EXISTS news_analysis (
  news_id                     INTEGER PRIMARY KEY REFERENCES news_raw(news_id) ON DELETE CASCADE,
  created_at_utc                TEXT,
  headline                    TEXT NOT NULL,
  symbols_input               TEXT NOT NULL,  -- JSON array
  actors                      TEXT,           -- JSON array of actor objects
  event                       TEXT,           -- JSON object with event details
  symbol_mentions_in_text     TEXT,           -- JSON array
  symbol_not_mentioned_in_text TEXT,          -- JSON array
  unresolved_entities         TEXT,           -- JSON array
  is_news_grounded            INTEGER DEFAULT 0,  -- Boolean: 0=false, 1=true
  analyzed_at                 TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_news_analysis_grounded ON news_analysis(is_news_grounded);
