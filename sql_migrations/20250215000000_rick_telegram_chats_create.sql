-- Create rick_telegram_chats (and ik_telegram_chats) if not exist.
-- Same structure as telegram_chats + segment. Apply before 20250216000001_telegram_chats_segment.sql.
-- Schema: rick_messages_tasks.

-- rick_telegram_chats: только чаты Rick.ai (подмножество для lookup по segment)
CREATE TABLE IF NOT EXISTS rick_messages_tasks.rick_telegram_chats (
  LIKE rick_messages_tasks.telegram_chats INCLUDING DEFAULTS INCLUDING CONSTRAINTS
);
ALTER TABLE rick_messages_tasks.rick_telegram_chats ADD COLUMN IF NOT EXISTS segment TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS uq_rick_telegram_chats_chat_id ON rick_messages_tasks.rick_telegram_chats (chat_id);

-- ik_telegram_chats: полная выгрузка всех чатов IK (для последующего копирования в rick_telegram_chats по фильтру)
CREATE TABLE IF NOT EXISTS rick_messages_tasks.ik_telegram_chats (
  LIKE rick_messages_tasks.telegram_chats INCLUDING DEFAULTS INCLUDING CONSTRAINTS
);
ALTER TABLE rick_messages_tasks.ik_telegram_chats ADD COLUMN IF NOT EXISTS segment TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS uq_ik_telegram_chats_chat_id ON rick_messages_tasks.ik_telegram_chats (chat_id);
