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


-- ======================================================
-- 10) ФУНДАМЕНТАЛЬНЫЕ ДАННЫЕ С YAHOO FINANCE
-- ======================================================
CREATE TABLE IF NOT EXISTS fundamentals (
  symbol              TEXT PRIMARY KEY,           -- Тикер акции (AAPL, MSFT, etc.)
  
  -- Основные финансовые показатели
  market_cap          REAL,                       -- Рыночная капитализация
  enterprise_value    REAL,                       -- Стоимость предприятия
  pe_ratio            REAL,                       -- P/E коэффициент (trailingPE)
  forward_pe          REAL,                       -- Forward P/E
  peg_ratio           REAL,                       -- PEG коэффициент
  price_to_book       REAL,                       -- P/B коэффициент
  price_to_sales      REAL,                       -- P/S коэффициент (priceToSalesTrailing12Months)
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
  five_year_avg_dividend_yield REAL,             -- 5-летняя средняя дивидендная доходность
  trailing_annual_dividend_rate REAL,            -- Годовая дивидендная ставка (trailing)
  trailing_annual_dividend_yield REAL,           -- Годовая дивидендная доходность (trailing)
  
  -- Технические показатели
  beta                REAL,                       -- Бета коэффициент
  fifty_two_week_high REAL,                      -- 52-недельный максимум
  fifty_two_week_low  REAL,                      -- 52-недельный минимум
  fifty_day_average   REAL,                       -- 50-дневная средняя
  two_hundred_day_average REAL,                   -- 200-дневная средняя
  fifty_two_week_change_percent REAL,            -- Изменение за 52 недели в %
  fifty_day_average_change REAL,                 -- Изменение от 50-дневной средней
  fifty_day_average_change_percent REAL,         -- Изменение от 50-дневной средней в %
  two_hundred_day_average_change REAL,           -- Изменение от 200-дневной средней
  two_hundred_day_average_change_percent REAL,   -- Изменение от 200-дневной средней в %
  
  -- Дополнительные финансовые показатели
  book_value          REAL,                       -- Балансовая стоимость акции
  total_cash          REAL,                       -- Общая наличность
  total_cash_per_share REAL,                      -- Наличность на акцию
  total_debt          REAL,                       -- Общий долг
  total_revenue       REAL,                       -- Общая выручка
  revenue_per_share   REAL,                       -- Выручка на акцию
  gross_profits       REAL,                       -- Валовая прибыль
  free_cashflow       REAL,                       -- Свободный денежный поток
  operating_cashflow  REAL,                       -- Операционный денежный поток
  ebitda              REAL,                       -- EBITDA
  net_income_to_common REAL,                     -- Чистая прибыль для обычных акционеров
  
  -- Показатели роста
  earnings_growth     REAL,                       -- Рост прибыли
  revenue_growth      REAL,                       -- Рост выручки
  earnings_quarterly_growth REAL,                -- Квартальный рост прибыли
  
  -- Маржинальность
  gross_margins       REAL,                       -- Валовая маржа
  ebitda_margins      REAL,                       -- EBITDA маржа
  operating_margins   REAL,                       -- Операционная маржа
  profit_margins      REAL,                       -- Маржа прибыли
  
  -- Акции и доля
  shares_outstanding  REAL,                       -- Количество акций в обращении
  float_shares        REAL,                       -- Количество акций в свободном обращении
  shares_short        REAL,                       -- Короткие позиции
  shares_short_prior_month REAL,                 -- Короткие позиции в прошлом месяце
  shares_percent_shares_out REAL,                -- Процент акций в коротких позициях
  held_percent_insiders REAL,                    -- Процент акций у инсайдеров
  held_percent_institutions REAL,                -- Процент акций у институтов
  short_ratio         REAL,                       -- Коэффициент коротких позиций
  short_percent_of_float REAL,                   -- Процент коротких позиций от свободного обращения
  
  -- Аналитические оценки
  target_high_price   REAL,                       -- Целевая максимальная цена
  target_low_price    REAL,                       -- Целевая минимальная цена
  target_mean_price   REAL,                       -- Средняя целевая цена
  target_median_price REAL,                       -- Медианная целевая цена
  recommendation_mean REAL,                       -- Средняя рекомендация
  recommendation_key  TEXT,                       -- Ключ рекомендации (buy, hold, sell)
  number_of_analyst_opinions INTEGER,            -- Количество аналитических мнений
  average_analyst_rating TEXT,                   -- Средний аналитический рейтинг
  
  -- Риски ESG
  audit_risk          INTEGER,                    -- Риск аудита
  board_risk          INTEGER,                    -- Риск совета директоров
  compensation_risk   INTEGER,                    -- Риск компенсации
  share_holder_rights_risk INTEGER,              -- Риск прав акционеров
  overall_risk        INTEGER,                    -- Общий риск
  
  -- Временные метки
  last_fiscal_year_end REAL,                     -- Последний финансовый год (timestamp)
  next_fiscal_year_end REAL,                     -- Следующий финансовый год (timestamp)
  most_recent_quarter REAL,                      -- Самый последний квартал (timestamp)
  ex_dividend_date    REAL,                       -- Дата ex-dividend (timestamp)
  dividend_date       REAL,                       -- Дата выплаты дивидендов (timestamp)
  last_dividend_date  REAL,                       -- Последняя дата выплаты дивидендов (timestamp)
  earnings_timestamp  REAL,                       -- Время отчета о прибыли (timestamp)
  earnings_timestamp_start REAL,                 -- Начало периода отчета о прибыли (timestamp)
  earnings_timestamp_end REAL,                   -- Конец периода отчета о прибыли (timestamp)
  
  -- Разделение акций
  last_split_factor   TEXT,                       -- Фактор последнего разделения (например, "4:1")
  last_split_date     REAL,                       -- Дата последнего разделения (timestamp)
  
  -- Метаданные
  sector              TEXT,                       -- Сектор
  industry            TEXT,                       -- Отрасль
  country             TEXT,                       -- Страна
  currency            TEXT,                       -- Валюта
  exchange            TEXT,                       -- Биржа
  quote_type          TEXT,                       -- Тип инструмента (EQUITY, ETF, etc.)
  market_state        TEXT,                       -- Состояние рынка (REGULAR, CLOSED, etc.)
  
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