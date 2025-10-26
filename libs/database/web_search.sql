-- Search results cache (title/URL/snippet only)
CREATE TABLE IF NOT EXISTS web_search_cache (
  id INTEGER PRIMARY KEY,
  provider TEXT NOT NULL,                 -- 'wikipedia', 'wikidata', 'duckduckgo', 'cse', etc.
  normalized_query TEXT NOT NULL,         -- normalized query key
  results_json TEXT NOT NULL,             -- JSON: [ { "title":..., "url":..., "snippet":... }, ... ]
  status TEXT NOT NULL,                   -- 'ok' | 'empty' | 'error' | 'ratelimited'
  http_code INTEGER,                      -- if applicable
  error TEXT,                             -- error text (if any)
  fetched_at_utc TEXT NOT NULL,           -- ISO8601
  attempts INTEGER NOT NULL DEFAULT 1,
  backoff_until_utc TEXT,                 -- when to try again (after 429, etc.)
  UNIQUE(provider, normalized_query)
);

-- If necessary, quick search by requests
CREATE VIRTUAL TABLE IF NOT EXISTS web_search_cache_fts
USING fts5(
  normalized_query,
  content='web_search_cache',
  content_rowid='id',
  tokenize='unicode61 remove_diacritics 2',
  prefix='2 3 4'  -- allows fast prefix searching normalized_query LIKE 'micr*'
);

-- Triggers for FTS synchronization
CREATE TRIGGER IF NOT EXISTS web_search_cache_ai AFTER INSERT ON web_search_cache BEGIN
  INSERT INTO web_search_cache_fts(rowid, normalized_query) VALUES (new.id, new.normalized_query);
END;
CREATE TRIGGER IF NOT EXISTS web_search_cache_ad AFTER DELETE ON web_search_cache BEGIN
  INSERT INTO web_search_cache_fts(web_search_cache_fts, rowid, normalized_query) VALUES('delete', old.id, old.normalized_query);
END;
CREATE TRIGGER IF NOT EXISTS web_search_cache_au AFTER UPDATE ON web_search_cache BEGIN
  INSERT INTO web_search_cache_fts(web_search_cache_fts, rowid, normalized_query) VALUES('delete', old.id, old.normalized_query);
  INSERT INTO web_search_cache_fts(rowid, normalized_query) VALUES (new.id, new.normalized_query);
END;
