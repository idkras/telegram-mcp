-- Migration 0.4: Telegram TDLib tables for sync_telegram_chats_to_supabase and SupabaseWriter
-- Source: [standards .md]/4. data workflows · engineering · analysis/4.4 tdlib dagster integration standard
-- Apply: Supabase Dashboard → SQL Editor → paste and run (or via supabase db push if you use CLI)
-- Schema: rick_messages_tasks (default SUPABASE_TELEGRAM_SCHEMA); set env to public if you create in public instead.

-- 1) Schema first (so our user owns it and can create function in it)
CREATE SCHEMA IF NOT EXISTS rick_messages_tasks;

-- 2) Helper for updated_at in our schema.
-- Use create-if-missing semantics so reruns do not fail on databases where the
-- function already exists but is owned by another privileged role.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'rick_messages_tasks'
      AND p.proname = 'set_updated_at'
      AND oidvectortypes(p.proargtypes) = ''
  ) THEN
    EXECUTE $fn$
      CREATE FUNCTION rick_messages_tasks.set_updated_at()
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
CREATE TABLE IF NOT EXISTS rick_messages_tasks.telegram_chats (
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
    WHERE schemaname = 'rick_messages_tasks'
      AND tablename = 'telegram_chats'
      AND indexdef ILIKE '%(chat_id)%'
      AND indexdef ILIKE 'CREATE UNIQUE INDEX%'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_telegram_chats_chat_id ON rick_messages_tasks.telegram_chats (chat_id)';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'rick_messages_tasks'
      AND tablename = 'telegram_chats'
      AND indexdef ILIKE '%(backfill_completed, last_backfill_message_id)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_chats_backfill ON rick_messages_tasks.telegram_chats (backfill_completed, last_backfill_message_id)';
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
    WHERE n.nspname = 'rick_messages_tasks'
      AND c.relname = 'telegram_chats'
      AND t.tgname = 'trg_set_updated_at_on_telegram_chats'
      AND NOT t.tgisinternal
  ) THEN
    EXECUTE '
      CREATE TRIGGER trg_set_updated_at_on_telegram_chats
      BEFORE UPDATE ON rick_messages_tasks.telegram_chats
      FOR EACH ROW
      EXECUTE FUNCTION rick_messages_tasks.set_updated_at()
    ';
  END IF;
END;
$$;

-- 4) Table: telegram_messages_raw
CREATE TABLE IF NOT EXISTS rick_messages_tasks.telegram_messages_raw (
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
    WHERE schemaname = 'rick_messages_tasks'
      AND tablename = 'telegram_messages_raw'
      AND indexdef ILIKE '%(chat_id, message_id)%'
      AND indexdef ILIKE 'CREATE UNIQUE INDEX%'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_telegram_message ON rick_messages_tasks.telegram_messages_raw (chat_id, message_id)';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'rick_messages_tasks'
      AND tablename = 'telegram_messages_raw'
      AND indexdef ILIKE '%(chat_id, message_ts DESC)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_chat_ts ON rick_messages_tasks.telegram_messages_raw (chat_id, message_ts DESC)';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'rick_messages_tasks'
      AND tablename = 'telegram_messages_raw'
      AND indexdef ILIKE '%(sender_user_id)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_sender ON rick_messages_tasks.telegram_messages_raw (sender_user_id)';
  END IF;
END;
$$;

-- 5) Table: telegram_ingest_runs
CREATE TABLE IF NOT EXISTS rick_messages_tasks.telegram_ingest_runs (
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
    WHERE schemaname = 'rick_messages_tasks'
      AND tablename = 'telegram_ingest_runs'
      AND indexdef ILIKE '%(mode, started_at DESC)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_ingest_runs_mode ON rick_messages_tasks.telegram_ingest_runs (mode, started_at DESC)';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'rick_messages_tasks'
      AND tablename = 'telegram_ingest_runs'
      AND indexdef ILIKE '%(status, started_at DESC)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_ingest_runs_status ON rick_messages_tasks.telegram_ingest_runs (status, started_at DESC)';
  END IF;
END;
$$;
