-- d8a-de-workbench: the warehouse layout every project uses.
--   raw      what the pipeline lands, as the source gives it (P2)
--   staging  one cleaned, typed model per source table (P3, dbt)
--   marts    the tables people read (P3, dbt)
--   app      the app context's source system: the shop's own database, loaded from CSV by 02_load_app.sh
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS app;

-- Run history: every pipeline run writes one row here (P2 asks for it, P4's DAG reads it).
CREATE TABLE IF NOT EXISTS raw._runs (
  run_id      TEXT PRIMARY KEY,
  pipeline    TEXT NOT NULL,
  started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  status      TEXT NOT NULL DEFAULT 'running',
  rows_in     BIGINT,
  rows_out    BIGINT,
  rows_rejected BIGINT,
  notes       TEXT
);
