-- Migration: add segment column to ik_telegram_chats and rick_telegram_chats
-- JTBD: понимать тип чата (advising, pilot, договорённости, …), быстро индексировать и находить
-- чаты для выгрузки сообщений. Таблицы уже созданы: ik_telegram_chats (все чаты IK),
-- rick_telegram_chats (только чаты клиентов Rick.ai).
--
-- Segment values (align with telegram-chats-index.md and rickai_roadmap.todo.md):
--   advising       — Advising клиенты Rick.ai
--   pilot          — Пилот
--   dogovorennosti — Договорённости
--   na_soprovozhdenii — На сопровождении
--   partners       — Партнёры
--   internal       — Внутренние Rick (Flow, подстраховка, PM Care, Метрика и т.д.)
--   bot_feedback   — Rick.ai bot feedback
--   community      — Комьюнити Heroes (HOC, PH, Management)
--   other          — прочее
--
-- Apply: Supabase Dashboard → SQL Editor (schema rick_messages_tasks).
-- If your tables are in public, replace rick_messages_tasks with public.

-- ik_telegram_chats: все чаты IK (полная выгрузка)
ALTER TABLE rick_messages_tasks.ik_telegram_chats
  ADD COLUMN IF NOT EXISTS segment TEXT;

COMMENT ON COLUMN rick_messages_tasks.ik_telegram_chats.segment IS 'Segment for indexing: advising, pilot, dogovorennosti, na_soprovozhdenii, partners, internal, bot_feedback, community, other';

-- rick_telegram_chats: только чаты Rick.ai (подмножество для клиентов и внутренних)
ALTER TABLE rick_messages_tasks.rick_telegram_chats
  ADD COLUMN IF NOT EXISTS segment TEXT;

COMMENT ON COLUMN rick_messages_tasks.rick_telegram_chats.segment IS 'Segment for indexing: advising, pilot, dogovorennosti, na_soprovozhdenii, partners, internal, bot_feedback, community, other';

CREATE INDEX IF NOT EXISTS ix_rick_telegram_chats_segment
  ON rick_messages_tasks.rick_telegram_chats (segment);

-- Optional: index on ik_telegram_chats for filtered lists
CREATE INDEX IF NOT EXISTS ix_ik_telegram_chats_segment
  ON rick_messages_tasks.ik_telegram_chats (segment);
