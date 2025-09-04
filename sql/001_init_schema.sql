-- =====================================================================
-- lead-macro — базовая схема БД (PostgreSQL)
-- =====================================================================

-- Схемы
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS feature;

-- ---------------------------------------------------------------------
-- Справочник источников
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.source (
  source_id  SERIAL PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE
);

-- ---------------------------------------------------------------------
-- Справочник таймсерий
--  - code: тикер/код ряда (должен быть уникален)
--  - asset_class: 'macro' | 'equity' | 'fx' | 'metal' | ...
--  - freq: 'D','W','ME','M','Q','A' и т.п.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.series (
  series_id   SERIAL PRIMARY KEY,
  source_id   INTEGER NOT NULL REFERENCES core.source(source_id) ON DELETE RESTRICT,
  code        TEXT    NOT NULL UNIQUE,             -- ★ важно: уникальный код серии
  asset_class TEXT    NOT NULL,
  freq        TEXT    NOT NULL
);

-- полезные индексы для фильтраций
CREATE INDEX IF NOT EXISTS idx_series_assetclass ON core.series(asset_class);
CREATE INDEX IF NOT EXISTS idx_series_freq       ON core.series(freq);

-- ---------------------------------------------------------------------
-- Наблюдения (единичный столбец value) — для макро-рядов и т.п.
-- PK по (series_id, ts) не допускает дублей
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.observation (
  series_id INTEGER NOT NULL REFERENCES core.series(series_id) ON DELETE CASCADE,
  ts        TIMESTAMPTZ NOT NULL,
  value     DOUBLE PRECISION,
  PRIMARY KEY (series_id, ts)
);

-- индексы (PK уже покрывает series_id, ts)
-- добавим отдельный по ts для возможных глобальных выборок
CREATE INDEX IF NOT EXISTS idx_observation_ts ON core.observation(ts);

-- ---------------------------------------------------------------------
-- Ценовые ряды (OHLCV)
-- PK по (series_id, ts) не допускает дублей
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.price (
  series_id  INTEGER NOT NULL REFERENCES core.series(series_id) ON DELETE CASCADE,
  ts         TIMESTAMPTZ NOT NULL,
  open       DOUBLE PRECISION,
  high       DOUBLE PRECISION,
  low        DOUBLE PRECISION,
  close      DOUBLE PRECISION,
  adj_close  DOUBLE PRECISION,
  volume     BIGINT,
  PRIMARY KEY (series_id, ts)
);

-- индексы (дополнительно к PK)
CREATE INDEX IF NOT EXISTS idx_price_ts ON core.price(ts);

-- ---------------------------------------------------------------------
-- Календарь релизов (для макро)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.release_calendar (
  series_id    INTEGER NOT NULL REFERENCES core.series(series_id) ON DELETE CASCADE,
  period_date  DATE    NOT NULL,          -- дата периода, к которому относится значение
  release_ts   TIMESTAMPTZ,               -- фактическое время публикации (если есть)
  value        DOUBLE PRECISION,
  PRIMARY KEY (series_id, period_date)
);

-- индексы
CREATE INDEX IF NOT EXISTS idx_release_by_ts ON core.release_calendar(release_ts);

-- =====================================================================
-- Конец файла
-- =====================================================================
