-- =========================================================
--  SQLite schema for: news + LLM parse + per-news candles
--  + labels after 3h + bayes buckets + odds parameters
-- =========================================================

PRAGMA journal_mode=WAL;            -- безопасная конкурентная запись
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys = ON;           -- включить FK-проверки

-- ============================
-- 1) СЫРЬЕ: новости из WS
-- ============================
CREATE TABLE IF NOT EXISTS news_raw (
  news_id         INTEGER PRIMARY KEY,            -- свой ключ
  source          TEXT    NOT NULL,               -- "benzinga", ...
  provider_id     TEXT,                           -- внешний ID новости в источнике
  created_at_utc  TEXT    NOT NULL,               -- ISO8601 UTC из источника
  received_at_utc TEXT    NOT NULL,               -- когда получили мы (UTC)
  headline        TEXT    NOT NULL,
  summary         TEXT,
  symbols_json    TEXT    NOT NULL,               -- JSON-массив тикеров из события
  url             TEXT,
  hash_dedupe     TEXT    NOT NULL UNIQUE,        -- hash(source|headline|floor_minute)
  UNIQUE(source, provider_id)                     -- источник + внешний ID
);

CREATE INDEX IF NOT EXISTS idx_news_raw_created ON news_raw(created_at_utc);
CREATE INDEX IF NOT EXISTS idx_news_raw_source  ON news_raw(source);

-- -- ===================================
-- -- 2) РАЗБОР LLM: структура события
-- -- ===================================
-- CREATE TABLE IF NOT EXISTS news_llm (
--   news_id          INTEGER PRIMARY KEY REFERENCES news_raw(news_id) ON DELETE CASCADE,
--   event_type       TEXT    NOT NULL,             -- "m&a"|"earnings"|...
--   direction_text   TEXT    NOT NULL,             -- "bullish"|"bearish"|"mixed"
--   severity         REAL    NOT NULL,             -- 0..1
--   certainty        REAL    NOT NULL,             -- 0..1
--   primary_symbols_json   TEXT NOT NULL,          -- JSON-массив ["INTC", ...]
--   secondary_symbols_json TEXT,                   -- JSON-массив
--   sector           TEXT    NOT NULL,             -- нормализованный сектор
--   session          TEXT    NOT NULL,             -- "RTH"|"AH"|"PM"
--   liq_bin          TEXT    NOT NULL,             -- "low"|"high"
--   parsed_at_utc    TEXT    NOT NULL
-- );

-- CREATE INDEX IF NOT EXISTS idx_news_llm_bucket
--   ON news_llm(event_type, direction_text, sector, session, liq_bin);

-- -- ==========================================================
-- -- 3) СВЯЗКА «новость × символ» + быстрые признаки ДО новости
-- -- ==========================================================
-- CREATE TABLE IF NOT EXISTS event_symbols (
--   news_id      INTEGER NOT NULL REFERENCES news_raw(news_id) ON DELETE CASCADE,
--   symbol       TEXT    NOT NULL,
--   p0_price     REAL,                        -- цена в момент новости (mid/close)
--   pre_ret_5m   REAL,                        -- лог-доходность за 5м ДО
--   pre_ret_15m  REAL,                        -- лог-доходность за 15м ДО
--   pre_rv_30m   REAL,                        -- реализованная вола за 30м ДО
--   is_ah        INTEGER NOT NULL DEFAULT 0,  -- 0=RTH/PM, 1=AH
--   PRIMARY KEY (news_id, symbol)
-- );

-- CREATE INDEX IF NOT EXISTS idx_event_symbols_symbol ON event_symbols(symbol);

-- -- ==========================================================
-- -- 4) СВЕЧИ, ПРИВЯЗАННЫЕ К КАЖДОЙ НОВОСТИ (около 1000/новость)
-- --    Храним и абсолютное время, и смещение в минутах от новости
-- -- ==========================================================
-- CREATE TABLE IF NOT EXISTS news_candles (
--   news_id      INTEGER NOT NULL REFERENCES news_raw(news_id) ON DELETE CASCADE,
--   symbol       TEXT    NOT NULL,
--   ts_utc       TEXT    NOT NULL,             -- ISO8601 UTC времени свечи
--   minute_offset INTEGER NOT NULL,             -- смещение от времени новости:
--                                              --   0 = свеча, в которую попала новость
--                                              --  -60..-1 ДО новости, +1..+180 ПОСЛЕ и т.д.
--   timeframe    TEXT    NOT NULL DEFAULT '1m', -- "1m" или "5m"
--   open         REAL    NOT NULL,
--   high         REAL    NOT NULL,
--   low          REAL    NOT NULL,
--   close        REAL    NOT NULL,
--   volume       REAL    NOT NULL,
--   role         TEXT    NOT NULL,              -- "pre" | "post" | "span"
--                                              -- pre  = до новости (offset<0)
--                                              -- post = после новости (offset>0)
--                                              -- span = если кладёшь общий диапазон
--   PRIMARY KEY (news_id, symbol, ts_utc)
-- );

-- -- Индексы для быстрых выборок «все свечи по новости/символу» и «по окну offset»
-- CREATE INDEX IF NOT EXISTS idx_news_candles_lookup
--   ON news_candles(news_id, symbol, minute_offset);
-- CREATE INDEX IF NOT EXISTS idx_news_candles_symbol_time
--   ON news_candles(symbol, ts_utc);

-- -- Пример запроса:
-- --  SELECT * FROM news_candles
-- --  WHERE news_id=? AND symbol='INTC' AND minute_offset BETWEEN -60 AND +180
-- --  ORDER BY minute_offset;

-- -- =========================================
-- -- 5) МЕТКИ через 3 часа для пары новость×символ
-- -- =========================================
-- CREATE TABLE IF NOT EXISTS labels_3h (
--   news_id        INTEGER NOT NULL REFERENCES news_raw(news_id) ON DELETE CASCADE,
--   symbol         TEXT    NOT NULL,
--   p3h_price      REAL    NOT NULL,
--   ret_3h         REAL    NOT NULL,          -- (p3h/p0 - 1) или лог-доходность
--   label          TEXT    NOT NULL,          -- "up"|"down"|"flat" по порогу ±1%
--   labeled_at_utc TEXT    NOT NULL,
--   PRIMARY KEY (news_id, symbol)
-- );

-- CREATE INDEX IF NOT EXISTS idx_labels_symbol ON labels_3h(symbol);

-- -- ======================================================
-- -- 6) ЕЖЕДНЕВНЫЕ АГРЕГАТЫ для Bayes-bucket (счётчики)
-- --    Корзина: event_type, direction_text, sector, session, liq_bin
-- --    (опционально добавь severity_bin при достатке данных)
-- -- ======================================================
-- CREATE TABLE IF NOT EXISTS buckets_daily (
--   as_of_date     TEXT    NOT NULL,       -- "YYYY-MM-DD"
--   event_type     TEXT    NOT NULL,
--   direction_text TEXT    NOT NULL,
--   sector         TEXT    NOT NULL,
--   session        TEXT    NOT NULL,
--   liq_bin        TEXT    NOT NULL,
--   severity_bin   TEXT,                   -- NULL | "low"|"mid"|"high"
--   n_up           INTEGER NOT NULL,
--   n_down         INTEGER NOT NULL,
--   n_flat         INTEGER NOT NULL,
--   PRIMARY KEY (as_of_date, event_type, direction_text, sector, session, liq_bin, severity_bin)
-- );

-- CREATE INDEX IF NOT EXISTS idx_buckets_rollup
--   ON buckets_daily(event_type, direction_text, sector, session, liq_bin, severity_bin);

-- -- ======================================================
-- -- 7) ПАРАМЕТРЫ для Odds-adjust и общие настройки
-- -- ======================================================
-- CREATE TABLE IF NOT EXISTS odds_params (
--   name  TEXT PRIMARY KEY,   -- "gamma1","gamma2","gamma3","gamma4","gamma5",
--                             -- "threshold_abs_move","horizon_hours" и т.п.
--   value REAL NOT NULL
-- );

-- -- Можно зафиксировать дефолты:
-- INSERT OR IGNORE INTO odds_params(name,value) VALUES
--  ('gamma1', 1.0),
--  ('gamma2', 0.5),
--  ('gamma3', 0.4),
--  ('gamma4', 0.6),
--  ('gamma5', 0.3),
--  ('threshold_abs_move', 0.01),     -- порог ±1%
--  ('horizon_hours', 3.0);           -- горизонт 3 часа

-- -- ======================================================
-- -- 8) (ОПЦИОНАЛЬНО) ОЧЕРЕДЬ ЗАДАЧ ПОСТАНОВКИ МЕТОК T+3h
-- --    Удобно для фона: сервис раз в N минут добирает "due" задачи
-- -- ======================================================
-- CREATE TABLE IF NOT EXISTS label_jobs (
--   news_id       INTEGER NOT NULL REFERENCES news_raw(news_id) ON DELETE CASCADE,
--   symbol        TEXT    NOT NULL,
--   due_at_utc    TEXT    NOT NULL,        -- created_at + 3h
--   status        TEXT    NOT NULL DEFAULT 'pending', -- "pending"|"done"|"error"
--   last_error    TEXT,
--   PRIMARY KEY (news_id, symbol)
-- );

-- CREATE INDEX IF NOT EXISTS idx_label_jobs_due ON label_jobs(due_at_utc, status);

-- -- ======================================================
-- -- 9) ПОЛЕЗНЫЕ ПРЕДСТАВЛЕНИЯ (VIEW)
-- -- ======================================================

-- -- 9.1. Быстрый доступ к «корзинному ключу» для события
-- CREATE VIEW IF NOT EXISTS v_event_bucket_key AS
-- SELECT
--   n.news_id,
--   nl.event_type,
--   nl.direction_text,
--   nl.sector,
--   nl.session,
--   nl.liq_bin,
--   nl.severity,
--   CASE
--     WHEN nl.severity IS NULL THEN NULL
--     WHEN nl.severity < 0.33 THEN 'low'
--     WHEN nl.severity < 0.66 THEN 'mid'
--     ELSE 'high'
--   END AS severity_bin
-- FROM news_raw n
-- JOIN news_llm nl ON nl.news_id = n.news_id;

-- -- 9.2. База для обучения/аналитики: одна строка на news×symbol
-- CREATE VIEW IF NOT EXISTS v_event_training_row AS
-- SELECT
--   es.news_id,
--   es.symbol,
--   n.created_at_utc,
--   nl.event_type, nl.direction_text, nl.sector, nl.session, nl.liq_bin,
--   nl.severity, nl.certainty,
--   es.p0_price, es.pre_ret_5m, es.pre_ret_15m, es.pre_rv_30m, es.is_ah,
--   l3.ret_3h, l3.label
-- FROM event_symbols es
-- JOIN news_raw n  ON n.news_id = es.news_id
-- LEFT JOIN news_llm nl ON nl.news_id = es.news_id
-- LEFT JOIN labels_3h l3 ON l3.news_id = es.news_id AND l3.symbol = es.symbol;

-- -- ======================================================
-- -- ГОТОВО.
-- -- Подсказки:
-- --   * вставляй свечи по событию в news_candles (до и после) с minute_offset
-- --   * labels_3h заполняй через 3 часа фоновым джобом (label_jobs)
-- --   * buckets_daily заполняй сборкой счётчиков из labels_3h по ключу корзины
-- -- ======================================================

-- ======================================================
-- 10) ФУНДАМЕНТАЛЬНЫЕ ДАННЫЕ С YAHOO FINANCE
-- ======================================================
CREATE TABLE IF NOT EXISTS fundamentals (
  symbol              TEXT PRIMARY KEY,           -- Тикер акции (AAPL, MSFT, etc.)
  
  -- Основные финансовые показатели
  market_cap          REAL,                       -- Рыночная капитализация
  enterprise_value    REAL,                       -- Стоимость предприятия
  pe_ratio            REAL,                       -- P/E коэффициент
  forward_pe          REAL,                       -- Forward P/E
  peg_ratio           REAL,                       -- PEG коэффициент
  price_to_book       REAL,                       -- P/B коэффициент
  price_to_sales      REAL,                       -- P/S коэффициент
  enterprise_to_revenue REAL,                     -- EV/Revenue
  enterprise_to_ebitda REAL,                      -- EV/EBITDA
  
  -- Показатели доходности
  return_on_equity    REAL,                       -- ROE
  return_on_assets    REAL,                       -- ROA
  return_on_capital   REAL,                       -- ROC
  
  -- Показатели ликвидности
  current_ratio       REAL,                       -- Текущий коэффициент
  quick_ratio         REAL,                       -- Быстрый коэффициент
  debt_to_equity      REAL,                       -- D/E коэффициент
  
  -- Дивиденды
  dividend_yield      REAL,                       -- Дивидендная доходность
  dividend_rate       REAL,                       -- Дивидендная ставка
  payout_ratio        REAL,                       -- Коэффициент выплат
  
  -- Технические показатели
  beta                REAL,                       -- Бета коэффициент
  fifty_two_week_high REAL,                      -- 52-недельный максимум
  fifty_two_week_low  REAL,                      -- 52-недельный минимум
  fifty_day_average   REAL,                       -- 50-дневная средняя
  two_hundred_day_average REAL,                   -- 200-дневная средняя
  
  -- Метаданные
  sector              TEXT,                       -- Сектор
  industry            TEXT,                       -- Отрасль
  country             TEXT,                       -- Страна
  currency            TEXT,                       -- Валюта
  
  -- Временные метки
  last_updated        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- Когда обновляли
  data_source         TEXT NOT NULL DEFAULT 'yahoo_finance'     -- Источник данных
);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_fundamentals_sector ON fundamentals(sector);
CREATE INDEX IF NOT EXISTS idx_fundamentals_industry ON fundamentals(industry);
CREATE INDEX IF NOT EXISTS idx_fundamentals_market_cap ON fundamentals(market_cap);
CREATE INDEX IF NOT EXISTS idx_fundamentals_pe_ratio ON fundamentals(pe_ratio);

-- ======================================================
-- 11) ОБЩАЯ ИНФОРМАЦИЯ О КОМПАНИИ (Yahoo Finance: ticker.info)
-- ======================================================
CREATE TABLE IF NOT EXISTS infos (
  symbol                 TEXT PRIMARY KEY,

  -- имена/наименования
  long_name              TEXT,
  short_name             TEXT,
  display_name           TEXT,

  -- сайт/IR/контакты
  website                TEXT,
  ir_website             TEXT,
  phone                  TEXT,

  -- адрес
  address1               TEXT,
  city                   TEXT,
  state                  TEXT,
  zip                    TEXT,
  country                TEXT,

  -- отрасль/сектор
  sector                 TEXT,
  industry               TEXT,

  -- персонал и описание
  full_time_employees    INTEGER,
  long_business_summary  TEXT,

  -- биржа/валюта
  exchange               TEXT,
  currency               TEXT,

  -- ключевые лица (упрощённо: name/title) и полный сырой JSON
  officers_json          TEXT,    -- JSON-массив [{name, title}, ...]
  raw_info_json          TEXT,    -- Полный JSON из ticker.info

  -- служебные поля
  last_updated           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  data_source            TEXT NOT NULL DEFAULT 'yahoo_finance'
);

CREATE INDEX IF NOT EXISTS idx_infos_sector ON infos(sector);
CREATE INDEX IF NOT EXISTS idx_infos_industry ON infos(industry);
CREATE INDEX IF NOT EXISTS idx_infos_country ON infos(country);