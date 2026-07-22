-- Migration 2026-07-22: telegram_articles — полнотекстовый индекс article/Instant View постов
-- JTBD: Когда в чат приходит пост-статья (telegra.ph, СМИ, каналы с Instant View),
--       хотим искать её по полному телу, а не только по превью, чтобы «найди статью
--       про X» работало по всему корпусу.
--
-- Диагноз 2026-07-22: 136 983 webpage-сообщений, из них лишь 6 369 (4.6%) с телом
-- (cached_page); писатель не дозапрашивал Instant View. Код: article_enrichment.py.
-- Apply: psql "$SUPABASE_DB_URL" -f <this file>   (idempotent — повторный прогон безопасен)
--        либо scripts/apply_telegram_migration.py
--
-- ┌─ ONBOARDING НОВОГО ПРОФИЛЯ (schema-per-profile, см. 20260605000001) ──────────┐
-- │ Скопировать секцию ниже, заменить rick_messages_tasks → tg_<slug>.            │
-- └───────────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS rick_messages_tasks.telegram_articles (
  chat_id          TEXT   NOT NULL,
  message_id       BIGINT NOT NULL,
  telegram_user_id TEXT,
  message_ts       TIMESTAMPTZ,
  url              TEXT,
  title            TEXT,
  description      TEXT,
  article_text     TEXT,
  has_page         BOOLEAN NOT NULL DEFAULT FALSE,
  fetched_at       TIMESTAMPTZ,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (chat_id, message_id)
);

-- FTS по всему читаемому контенту статьи; 'simple' — корпус мультиязычный (ru/en/uk),
-- языковой стеммер дал бы асимметрию между языками.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='rick_messages_tasks' AND tablename='telegram_articles'
      AND indexname='ix_telegram_articles_fts'
  ) THEN
    EXECUTE $ix$
      CREATE INDEX ix_telegram_articles_fts
      ON rick_messages_tasks.telegram_articles
      USING GIN (to_tsvector('simple',
        coalesce(title,'') || ' ' || coalesce(description,'') || ' ' || coalesce(article_text,'')))
    $ix$;
  END IF;
END;
$$;

-- Свежие статьи сверху в выдаче поиска.
CREATE INDEX IF NOT EXISTS ix_telegram_articles_ts
  ON rick_messages_tasks.telegram_articles (message_ts DESC);
