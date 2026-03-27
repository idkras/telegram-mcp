-- Migration 2026-03-26: Telegram runtime scoping and heartbeat foundation
-- JTBD: When multiple Telegram accounts ingest into one Supabase contour,
-- we need per-user chat cursors and runtime markers so catch-up and health
-- checks stay correct for each account independently.

CREATE SCHEMA IF NOT EXISTS rick_messages_tasks;

-- Per-user chat state (keeps cursors scoped by telegram_user_id).
CREATE TABLE IF NOT EXISTS rick_messages_tasks.telegram_chat_state (
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
    WHERE schemaname = 'rick_messages_tasks'
      AND tablename = 'telegram_chat_state'
      AND indexdef ILIKE '%(telegram_user_id, chat_id)%'
      AND indexdef ILIKE 'CREATE UNIQUE INDEX%'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_telegram_chat_state_user_chat ON rick_messages_tasks.telegram_chat_state (telegram_user_id, chat_id)';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'rick_messages_tasks'
      AND tablename = 'telegram_chat_state'
      AND indexdef ILIKE '%(telegram_user_id, backfill_completed, last_backfill_message_id)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_chat_state_backfill ON rick_messages_tasks.telegram_chat_state (telegram_user_id, backfill_completed, last_backfill_message_id)';
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
      AND c.relname = 'telegram_chat_state'
      AND t.tgname = 'trg_set_updated_at_on_telegram_chat_state'
      AND NOT t.tgisinternal
  ) THEN
    EXECUTE '
      CREATE TRIGGER trg_set_updated_at_on_telegram_chat_state
      BEFORE UPDATE ON rick_messages_tasks.telegram_chat_state
      FOR EACH ROW
      EXECUTE FUNCTION rick_messages_tasks.set_updated_at()
    ';
  END IF;
END;
$$;

ALTER TABLE rick_messages_tasks.telegram_ingest_runs
  ADD COLUMN IF NOT EXISTS telegram_user_id TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'rick_messages_tasks'
      AND tablename = 'telegram_ingest_runs'
      AND indexdef ILIKE '%(telegram_user_id, mode, started_at DESC)%'
  ) THEN
    EXECUTE 'CREATE INDEX ix_telegram_ingest_runs_user_mode ON rick_messages_tasks.telegram_ingest_runs (telegram_user_id, mode, started_at DESC)';
  END IF;
END;
$$;
