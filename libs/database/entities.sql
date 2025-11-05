-- =========================================================
-- ENTITIES: single table for all entity types
-- =========================================================
CREATE TABLE IF NOT EXISTS entities (
  entity_id              INTEGER PRIMARY KEY, -- rowid
  entity_type            TEXT NOT NULL CHECK (entity_type IN ('org','person','product','fund','regulator','other')),

  -- Canonical, display-agnostic label
  canonical_full         TEXT,      -- org: registered/long legal name; person: full legal name; others: canonical label
  display_name           TEXT,      -- optional short label for UI (e.g., "Apple", "Tim Cook")

  -- PERSON-specific (nullable otherwise)
  given                  TEXT,
  middle                 TEXT,
  family                 TEXT,
  suffix                 TEXT,

  -- Deterministic normalization fields for persons (used for robust matching from text)
  given_norm             TEXT,
  family_norm            TEXT,
  given_initial          TEXT,
  given_prefix3          TEXT,
  middle_initials        TEXT,
  full_norm_no_honor     TEXT,

  -- ORG-specific lightweight profile (you can move to a separate table if it grows)
  long_business_summary  TEXT,      -- long text description from Yahoo (for org)
  website                TEXT,
  ir_website             TEXT,
  phone                  TEXT,
  address1               TEXT,
  city                   TEXT,
  state                  TEXT,
  zip                    TEXT,
  country                TEXT,
  sector                 TEXT,
  industry               TEXT,
  full_time_employees    INTEGER,

  created_at             TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at             TEXT NOT NULL DEFAULT (datetime('now')),

  -- Minimal type safety:
  CHECK (entity_type <> 'person' OR (given IS NOT NULL AND family IS NOT NULL)),
  CHECK (entity_type <> 'org'    OR (canonical_full IS NOT NULL))
);

-- Updated-at housekeeping
CREATE TRIGGER IF NOT EXISTS trg_entities_updated_at
AFTER UPDATE ON entities
FOR EACH ROW
BEGIN
  UPDATE entities SET updated_at = datetime('now') WHERE entity_id = OLD.entity_id;
END;

-- Indexes to speed up common lookups
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_org_name ON entities(canonical_full) WHERE entity_type='org';
CREATE INDEX IF NOT EXISTS idx_person_family ON entities(family_norm)       WHERE entity_type='person';
CREATE INDEX IF NOT EXISTS idx_person_family_prefix ON entities(family_norm, given_prefix3) WHERE entity_type='person';
CREATE INDEX IF NOT EXISTS idx_person_given_initial ON entities(family_norm, given_initial) WHERE entity_type='person';



-- =========================================================
-- ALIASES: unified mapping from any surface form -> entity
-- =========================================================
CREATE TABLE IF NOT EXISTS aliases (
  alias_id        INTEGER PRIMARY KEY,
  entity_id       INTEGER NOT NULL,
  alias_text      TEXT NOT NULL,        -- raw surface form from a source ("AAPL", "Apple", "Apple Inc.")
  alias_type      TEXT NOT NULL,        -- 'symbol','long_name','short_name','display_name','aka','former_name','ticker_old','person_short', etc.
  lang            TEXT,                 -- ISO 639-1 (optional)
  script          TEXT,                 -- ISO 15924 (optional)
  normalized      TEXT NOT NULL,        -- deterministic lower/NFKD/diacritics-stripped for matching
  source          TEXT,                 -- provenance (e.g., "yahoo_finance")
  confidence      REAL DEFAULT 1.0,     -- 0..1, keep simple for now

  -- Symbol-specific qualifiers (NULL for non-symbol aliases)
  primary_exchange TEXT,                -- e.g., "NASDAQ"
  is_primary       INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0,1)),

  created_at       TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (entity_id) REFERENCES entities(entity_id) ON DELETE CASCADE
);

-- Uniqueness & search
CREATE UNIQUE INDEX IF NOT EXISTS uq_alias_unique ON aliases(entity_id, alias_type, normalized);
CREATE INDEX IF NOT EXISTS idx_alias_norm ON aliases(normalized);
CREATE INDEX IF NOT EXISTS idx_alias_symbol ON aliases(alias_text) WHERE alias_type='symbol';
CREATE INDEX IF NOT EXISTS idx_alias_primary ON aliases(is_primary);



-- =========================================================
-- AFFILIATIONS: relate persons to organizations (for roles/positions)
-- =========================================================
CREATE TABLE IF NOT EXISTS affiliations (
  affiliation_id  INTEGER PRIMARY KEY,
  person_id       INTEGER NOT NULL,     -- references entities(entity_id) where entity_type='person'
  org_id          INTEGER,              -- references entities(entity_id) where entity_type='org' (nullable when only symbol is known)
  symbol_alias_id INTEGER,              -- optional direct pointer to a symbol alias row (for precision)
  role_title      TEXT,                 -- "CEO", "SVP & CFO", etc.
  valid_from      TEXT,                 -- ISO-8601 dates if known
  valid_to        TEXT,
  source          TEXT,
  confidence      REAL DEFAULT 1.0,
  created_at      TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (person_id) REFERENCES entities(entity_id) ON DELETE CASCADE,
  FOREIGN KEY (org_id)    REFERENCES entities(entity_id) ON DELETE SET NULL,
  FOREIGN KEY (symbol_alias_id) REFERENCES aliases(alias_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_aff_person ON affiliations(person_id);
CREATE INDEX IF NOT EXISTS idx_aff_org ON affiliations(org_id);
CREATE INDEX IF NOT EXISTS idx_aff_symbolalias ON affiliations(symbol_alias_id);

-- Prevent duplicate affiliations
CREATE UNIQUE INDEX IF NOT EXISTS uq_affiliation_unique ON affiliations(person_id, org_id, role_title);



-- =========================================================
-- (Optional but recommended) FTS5 for alias-text matching
-- contentless FTS mirrors aliases.alias_text for fast news mention lookup
-- =========================================================
CREATE VIRTUAL TABLE IF NOT EXISTS alias_fts USING fts5(
  alias_text,
  content='',
  tokenize = 'unicode61 remove_diacritics 2'
);

-- Lightweight maintenance triggers for FTS mirror
CREATE TRIGGER IF NOT EXISTS trg_aliases_ai AFTER INSERT ON aliases BEGIN
  INSERT INTO alias_fts(rowid, alias_text) VALUES (NEW.alias_id, NEW.alias_text);
END;
CREATE TRIGGER IF NOT EXISTS trg_aliases_ad AFTER DELETE ON aliases BEGIN
  INSERT INTO alias_fts(alias_fts, rowid, alias_text) VALUES ('delete', OLD.alias_id, OLD.alias_text);
END;
CREATE TRIGGER IF NOT EXISTS trg_aliases_au AFTER UPDATE ON aliases BEGIN
  INSERT INTO alias_fts(alias_fts, rowid, alias_text) VALUES ('delete', OLD.alias_id, OLD.alias_text);
  INSERT INTO alias_fts(rowid, alias_text) VALUES (NEW.alias_id, NEW.alias_text);
END;
