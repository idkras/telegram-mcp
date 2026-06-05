-- Migration 2026-06-05: Telegram schema-per-profile (RCA 2026-06-05, owner directive)
-- JTBD: Когда менеджеры должны видеть ТОЛЬКО чаты Лизы, хотим раздельные Supabase-схемы
--       на аккаунт (tg_lisa.* отдельно от rick_messages_tasks.* = ikrasinsky), чтобы
--       данные не смешивались и доступ выдавался ОДНИМ грантом на схему.
--
-- Bead: pr-rick-q3yh
-- Apply: psql "$SUPABASE_DB_URL" -f <this file>   (idempotent — повторный прогон безопасен)
--
-- ┌─ ONBOARDING НОВОГО АККАУНТА (zero code change, AGENTS.md Generalization-first) ─┐
-- │ supabase_writer._schema_for_profile() уже резолвит профиль → tg_<slug>.          │
-- │ Чтобы создать схему нового аккаунта: скопировать СЕКЦИЮ B ниже, заменить         │
-- │ tg_lisa → tg_<slug> (= вывод _slugify_profile), прогнать этот файл.              │
-- └────────────────────────────────────────────────────────────────────────────────┘

-- ============================================================================
-- SECTION A — дочинить half-applied rick_messages_tasks (ikrasinsky, P0 блокер)
-- runtime_scoping (20260326000001) НЕ добрал telegram_user_id на 2 таблицы →
-- ingest-run logging падал «column telegram_user_id does not exist» → LISTENER-DEAD.
-- НЕ трогаем 137922 существующих строк ikrasinsky — только ADD COLUMN (аддитивно).
-- ============================================================================

ALTER TABLE rick_messages_tasks.telegram_ingest_runs
  ADD COLUMN IF NOT EXISTS telegram_user_id TEXT;

ALTER TABLE rick_messages_tasks.telegram_chats
  ADD COLUMN IF NOT EXISTS telegram_user_id TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='rick_messages_tasks' AND tablename='telegram_ingest_runs'
      AND indexdef ILIKE '%(telegram_user_id, mode, started_at DESC)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_ingest_runs_user_mode ON rick_messages_tasks.telegram_ingest_runs (telegram_user_id, mode, started_at DESC)';
  END IF;
END;
$$;

-- ============================================================================
-- SECTION B — tg_lisa: полная каноническая структура (4 таблицы + индексы +
-- триггеры + set_updated_at), сразу с telegram_user_id. Скопировано из
-- 20250110000001 (base) + 20260326000001 (chat_state) с заменой схемы.
-- ============================================================================

-- Migration 0.4: Telegram TDLib tables for sync_telegram_chats_to_supabase and SupabaseWriter
-- Source: [standards .md]/4. data workflows · engineering · analysis/4.4 tdlib dagster integration standard
-- Apply: Supabase Dashboard → SQL Editor → paste and run (or via supabase db push if you use CLI)
-- Schema: tg_lisa (default SUPABASE_TELEGRAM_SCHEMA); set env to public if you create in public instead.

-- 1) Schema first (so our user owns it and can create function in it)
CREATE SCHEMA IF NOT EXISTS tg_lisa;

-- 2) Helper for updated_at in our schema.
-- Use create-if-missing semantics so reruns do not fail on databases where the
-- function already exists but is owned by another privileged role.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'tg_lisa'
      AND p.proname = 'set_updated_at'
      AND oidvectortypes(p.proargtypes) = ''
  ) THEN
    EXECUTE $fn$
      CREATE FUNCTION tg_lisa.set_updated_at()
      RETURNS TRIGGER
      LANGUAGE plpgsql
      AS $body$
      BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
      END;
      $body$;
    $fn$;
  END IF;
END;
$$;

-- 3) Table: telegram_chats
CREATE TABLE IF NOT EXISTS tg_lisa.telegram_chats (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  chat_id TEXT NOT NULL,
  chat_type TEXT,
  chat_title TEXT,
  chat_username TEXT,

  last_backfill_message_id BIGINT,
  last_backfill_ts TIMESTAMPTZ,
  backfill_completed BOOLEAN NOT NULL DEFAULT FALSE,

  last_seen_message_id BIGINT,
  last_seen_ts TIMESTAMPTZ,

  total_messages_count BIGINT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  last_error TEXT
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'tg_lisa'
      AND tablename = 'telegram_chats'
      AND indexdef ILIKE '%(chat_id)%'
      AND indexdef ILIKE 'CREATE UNIQUE INDEX%'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_telegram_chats_chat_id ON tg_lisa.telegram_chats (chat_id)';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'tg_lisa'
      AND tablename = 'telegram_chats'
      AND indexdef ILIKE '%(backfill_completed, last_backfill_message_id)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_chats_backfill ON tg_lisa.telegram_chats (backfill_completed, last_backfill_message_id)';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'tg_lisa'
      AND c.relname = 'telegram_chats'
      AND t.tgname = 'trg_set_updated_at_on_telegram_chats'
      AND NOT t.tgisinternal
  ) THEN
    EXECUTE '
      CREATE TRIGGER trg_set_updated_at_on_telegram_chats
      BEFORE UPDATE ON tg_lisa.telegram_chats
      FOR EACH ROW
      EXECUTE FUNCTION tg_lisa.set_updated_at()
    ';
  END IF;
END;
$$;

-- 4) Table: telegram_messages_raw
CREATE TABLE IF NOT EXISTS tg_lisa.telegram_messages_raw (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  source TEXT NOT NULL DEFAULT 'telegram',

  telegram_user_id TEXT,
  chat_id TEXT NOT NULL,
  chat_type TEXT,
  message_id BIGINT NOT NULL,

  sender_user_id TEXT,
  sender_name TEXT,
  sender_username TEXT,

  message_ts TIMESTAMPTZ,
  text TEXT,

  raw JSONB NOT NULL
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'tg_lisa'
      AND tablename = 'telegram_messages_raw'
      AND indexdef ILIKE '%(chat_id, message_id)%'
      AND indexdef ILIKE 'CREATE UNIQUE INDEX%'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_telegram_message ON tg_lisa.telegram_messages_raw (chat_id, message_id)';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'tg_lisa'
      AND tablename = 'telegram_messages_raw'
      AND indexdef ILIKE '%(chat_id, message_ts DESC)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_chat_ts ON tg_lisa.telegram_messages_raw (chat_id, message_ts DESC)';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'tg_lisa'
      AND tablename = 'telegram_messages_raw'
      AND indexdef ILIKE '%(sender_user_id)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_sender ON tg_lisa.telegram_messages_raw (sender_user_id)';
  END IF;
END;
$$;

-- 5) Table: telegram_ingest_runs
CREATE TABLE IF NOT EXISTS tg_lisa.telegram_ingest_runs (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  run_id TEXT NOT NULL,
  mode TEXT NOT NULL,

  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,

  processed_chats INTEGER DEFAULT 0,
  inserted_messages INTEGER DEFAULT 0,
  updated_cursors INTEGER DEFAULT 0,

  last_error TEXT,
  error_details JSONB,

  status TEXT NOT NULL DEFAULT 'running'
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'tg_lisa'
      AND tablename = 'telegram_ingest_runs'
      AND indexdef ILIKE '%(mode, started_at DESC)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_ingest_runs_mode ON tg_lisa.telegram_ingest_runs (mode, started_at DESC)';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'tg_lisa'
      AND tablename = 'telegram_ingest_runs'
      AND indexdef ILIKE '%(status, started_at DESC)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_ingest_runs_status ON tg_lisa.telegram_ingest_runs (status, started_at DESC)';
  END IF;
END;
$$;


-- --- per-user scoping (chat_state + ingest_runs.telegram_user_id index) ---
-- Migration 2026-03-26: Telegram runtime scoping and heartbeat foundation
-- JTBD: When multiple Telegram accounts ingest into one Supabase contour,
-- we need per-user chat cursors and runtime markers so catch-up and health
-- checks stay correct for each account independently.

CREATE SCHEMA IF NOT EXISTS tg_lisa;

-- Per-user chat state (keeps cursors scoped by telegram_user_id).
CREATE TABLE IF NOT EXISTS tg_lisa.telegram_chat_state (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  telegram_user_id TEXT NOT NULL,
  chat_id TEXT NOT NULL,

  last_backfill_message_id BIGINT,
  last_backfill_ts TIMESTAMPTZ,
  backfill_completed BOOLEAN NOT NULL DEFAULT FALSE,

  last_seen_message_id BIGINT,
  last_seen_ts TIMESTAMPTZ,

  total_messages_count BIGINT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  last_error TEXT
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'tg_lisa'
      AND tablename = 'telegram_chat_state'
      AND indexdef ILIKE '%(telegram_user_id, chat_id)%'
      AND indexdef ILIKE 'CREATE UNIQUE INDEX%'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_telegram_chat_state_user_chat ON tg_lisa.telegram_chat_state (telegram_user_id, chat_id)';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'tg_lisa'
      AND tablename = 'telegram_chat_state'
      AND indexdef ILIKE '%(telegram_user_id, backfill_completed, last_backfill_message_id)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_chat_state_backfill ON tg_lisa.telegram_chat_state (telegram_user_id, backfill_completed, last_backfill_message_id)';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'tg_lisa'
      AND c.relname = 'telegram_chat_state'
      AND t.tgname = 'trg_set_updated_at_on_telegram_chat_state'
      AND NOT t.tgisinternal
  ) THEN
    EXECUTE '
      CREATE TRIGGER trg_set_updated_at_on_telegram_chat_state
      BEFORE UPDATE ON tg_lisa.telegram_chat_state
      FOR EACH ROW
      EXECUTE FUNCTION tg_lisa.set_updated_at()
    ';
  END IF;
END;
$$;

ALTER TABLE tg_lisa.telegram_ingest_runs
  ADD COLUMN IF NOT EXISTS telegram_user_id TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'tg_lisa'
      AND tablename = 'telegram_ingest_runs'
      AND indexdef ILIKE '%(telegram_user_id, mode, started_at DESC)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_ingest_runs_user_mode ON tg_lisa.telegram_ingest_runs (telegram_user_id, mode, started_at DESC)';
  END IF;
END;
$$;

-- tg_lisa: добрать telegram_user_id (как в rick_messages_tasks после Section A)
ALTER TABLE tg_lisa.telegram_ingest_runs ADD COLUMN IF NOT EXISTS telegram_user_id TEXT;
ALTER TABLE tg_lisa.telegram_chats ADD COLUMN IF NOT EXISTS telegram_user_id TEXT;
