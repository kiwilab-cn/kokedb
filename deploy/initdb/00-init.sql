-- Runs once, on first initialization of the postgres data volume
-- (docker-entrypoint-initdb.d). Executed as the superuser against the
-- default `postgres` database.

-- ── Meta store ────────────────────────────────────────────────────────────
-- kokedb keeps its catalog/stats tables here (PG_META_DSN -> .../kokedb).
-- The `system` schema and tables are created by the server on startup
-- (PostgreSQLMetaCatalogProviderList::init_db), so we only need the database.
CREATE DATABASE kokedb;

-- ── Integration-test fixtures ───────────────────────────────────────────────
-- These live in the default `postgres` database and back the DB-backed tests
-- (KOKEDB_TEST_DSN -> .../postgres). Keep them in sync with the table/column
-- names referenced by the #[ignore]d integration tests.
\connect postgres

CREATE SCHEMA IF NOT EXISTS test;

-- data-source::remote_table::test_postgresql_table_provider
--   SELECT * FROM remote_table WHERE column3 > 10
CREATE TABLE IF NOT EXISTS test.newtable (
    column1 text,
    column2 text,
    column3 integer
);
INSERT INTO test.newtable (column1, column2, column3) VALUES
    ('a', 'x', 5),
    ('b', 'y', 15),
    ('c', 'z', 25);

-- task-manager::read_postgres::test_conversion  (public.test1 -> parquet)
CREATE TABLE IF NOT EXISTS public.test1 (
    id integer PRIMARY KEY,
    name text,
    amount numeric
);
INSERT INTO public.test1 (id, name, amount) VALUES
    (1, 'alpha', 100.5),
    (2, 'beta', 200.0)
ON CONFLICT DO NOTHING;

-- task-manager::task_manager::test_task_manager_run_task  (syncs public.demo)
CREATE TABLE IF NOT EXISTS public.demo (
    id integer PRIMARY KEY,
    label text,
    updated_at timestamptz DEFAULT now()
);
INSERT INTO public.demo (id, label) VALUES
    (1, 'one'),
    (2, 'two')
ON CONFLICT DO NOTHING;
