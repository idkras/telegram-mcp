#!/usr/bin/env python3
"""
Telegram Chat Exporter with Mac Keychain Integration

Этот скрипт выгружает сообщения из Telegram чатов в удобном формате,
используя безопасное хранение credentials в Mac Keychain.

Основан на задаче из tg.todo.md для выгрузки чата [EasyPay] IFS.

Usage:
    python chat_exporter.py --chat_name "EasyPay" --output_dir ../[clients]/ifscourse.com/
    python chat_exporter.py --chat_id -1002669460604 --output_dir ../[clients]/ifscourse.com/
"""

import argparse
import asyncio
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from telethon import TelegramClient  # type: ignore
from telethon.sessions import StringSession  # type: ignore
from telethon.tl.types import ChannelParticipantsAdmins  # type: ignore

from heroes_platform.shared.import_setup import enable
enable(__file__)

from heroes_platform.shared.credentials_manager import credentials_manager

# Import from telegram-mcp directory (legacy structure)

telegram_mcp_path = Path(__file__).parent.parent / "telegram-mcp"
if str(telegram_mcp_path) not in sys.path:
    sys.path.insert(0, str(telegram_mcp_path))


class ChatExporter:
    """Экспортер чатов с интеграцией Mac Keychain"""

    def __init__(self):
        # self.keychain_manager = credentials_manager()
        self.keychain_manager = None
        self.client = None

    async def initialize_client(self) -> bool:
        """Инициализировать Telegram клиент с credentials из Keychain"""
        try:
            # Получить credentials из Keychain
            credentials = self.keychain_manager.get_credentials()
            if not credentials:
                print("❌ No credentials found in Mac Keychain")
                print(
                    "Please run: python keychain_integration.py --action store_credentials --api_id YOUR_API_ID --api_hash YOUR_API_HASH --session_string YOUR_SESSION_STRING"
                )
                return False

            # Создать клиент
            self.client = TelegramClient(
                StringSession(credentials["session_string"]),
                int(credentials["api_id"]),
                credentials["api_hash"],
            )

            # Запустить клиент
            await self.client.start()
            print("✅ Telegram client initialized successfully")
            return True

        except Exception as e:
            print(f"❌ Failed to initialize client: {e}")
            return False

    async def find_chat_by_name(self, chat_name: str) -> dict[str, Any] | None:
        """Найти чат по названию"""
        try:
            dialogs = await self.client.get_dialogs()

            for dialog in dialogs:
                entity = dialog.entity
                title = getattr(entity, "title", None) or getattr(entity, "first_name", "")

                if title and chat_name.lower() in title.lower():
                    return {
                        "id": entity.id,
                        "title": title,
                        "type": type(entity).__name__,
                        "entity": entity,
                    }

            print(f"❌ Chat with name '{chat_name}' not found")
            return None

        except Exception as e:
            print(f"❌ Error finding chat: {e}")
            return None

    async def get_chat_info(self, chat_id: int) -> dict[str, Any] | None:
        """Получить информацию о чате по ID"""
        try:
            entity = await self.client.get_entity(chat_id)

            # Получить админов чата
            admins = await self.get_chat_admins(entity)

            # Получить владельца чата
            owner = await self.get_chat_owner(entity)

            # Получить настройки истории чата
            history_visible = await self.get_chat_history_settings(entity)

            # Получить участников чата
            participants = await self.get_chat_participants(entity)

            return {
                "id": entity.id,
                "title": getattr(entity, "title", None) or getattr(entity, "first_name", ""),
                "type": type(entity).__name__,
                "entity": entity,
                "admins": admins,
                "owner": owner,
                "history_visible": history_visible,
                "participants": participants,
            }
        except Exception as e:
            print(f"❌ Error getting chat info: {e}")
            return None

    async def get_chat_admins(self, entity) -> list[dict[str, str]]:
        """Получить список админов чата"""
        try:
            if hasattr(entity, "megagroup") or hasattr(entity, "broadcast"):
                # Для групп и каналов
                admins = await self.client.get_participants(entity, filter=ChannelParticipantsAdmins)
                return [
                    {
                        "name": f"{admin.first_name} {admin.last_name or ''}".strip(),
                        "username": (f"@{admin.username}" if admin.username else "Нет username"),
                        "id": admin.id,
                    }
                    for admin in admins
                ]
            return []
        except Exception as e:
            print(f"❌ Error getting admins: {e}")
            return []

    async def get_chat_owner(self, entity) -> dict[str, str]:
        """Получить владельца чата"""
        try:
            if hasattr(entity, "megagroup") or hasattr(entity, "broadcast"):
                # Для групп и каналов
                admins = await self.client.get_participants(entity, filter=ChannelParticipantsAdmins)
                for admin in admins:
                    if hasattr(admin, "admin_rights") and admin.admin_rights and admin.admin_rights.other:
                        return {
                            "name": f"{admin.first_name} {admin.last_name or ''}".strip(),
                            "username": (f"@{admin.username}" if admin.username else "Нет username"),
                            "id": str(admin.id),
                        }
            return {"name": "Неизвестно", "username": "Нет username", "id": "0"}
        except Exception as e:
            print(f"❌ Error getting owner: {e}")
            return {"name": "Ошибка", "username": "Нет username", "id": "0"}

    async def get_chat_history_settings(self, entity) -> bool:
        """Получить настройки истории чата"""
        try:
            if hasattr(entity, "megagroup") or hasattr(entity, "broadcast"):
                # Для групп и каналов
                full_chat = await self.client.get_entity(entity)
                if hasattr(full_chat, "history_available"):
                    return full_chat.history_available
            return False
        except Exception as e:
            print(f"❌ Error getting history settings: {e}")
            return False

    async def get_chat_participants(self, entity) -> list[dict[str, str]]:
        """Получить участников чата"""
        try:
            if hasattr(entity, "megagroup") or hasattr(entity, "broadcast"):
                # Для групп и каналов
                participants = await self.client.get_participants(entity, limit=100)
                return [
                    {
                        "name": f"{participant.first_name} {participant.last_name or ''}".strip(),
                        "username": (f"@{participant.username}" if participant.username else "Нет username"),
                        "id": participant.id,
                    }
                    for participant in participants
                ]
            return []
        except Exception as e:
            print(f"❌ Error getting participants: {e}")
            return []

    async def export_chat_messages(self, chat_id: int, limit: int = 1000) -> list[dict[str, Any]]:
        """Экспортировать сообщения из чата"""
        try:
            entity = await self.client.get_entity(chat_id)
            messages = await self.client.get_messages(entity, limit=limit)

            exported_messages = []

            for message in messages:
                if message and message.text:
                    sender = await message.get_sender()
                    sender_name = "Unknown"

                    if sender:
                        if hasattr(sender, "first_name"):
                            sender_name = sender.first_name
                            if hasattr(sender, "last_name") and sender.last_name:
                                sender_name += f" {sender.last_name}"
                        elif hasattr(sender, "title"):
                            sender_name = sender.title

                    exported_messages.append(
                        {
                            "id": message.id,
                            "date": message.date.isoformat() if message.date else None,
                            "sender": sender_name,
                            "text": message.text,
                            "message_type": "text",
                        }
                    )

            print(f"✅ Exported {len(exported_messages)} messages from chat")
            return exported_messages

        except Exception as e:
            print(f"❌ Error exporting messages: {e}")
            return []

    def format_messages_for_markdown(self, messages: list[dict[str, Any]], chat_info: dict[str, Any]) -> str:
        """Форматировать сообщения для Markdown файла"""
        if not messages:
            return "# No messages found"

        # Группировать сообщения по дням
        messages_by_date = defaultdict(list)

        for message in messages:
            if message["date"]:
                date_str = message["date"][:10]  # YYYY-MM-DD
                messages_by_date[date_str].append(message)

        # Создать Markdown контент
        content = []

        # Заголовок
        content.append(f"# Telegram Chat Export: {chat_info['title']}")
        content.append("")
        content.append(f"**Chat ID:** {chat_info['id']}")
        content.append(f"**Chat Type:** {chat_info['type']}")
        content.append(f"**Export Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        content.append(f"**Total Messages:** {len(messages)}")
        content.append("")

        # Метаданные чата
        content.append("## 📊 Chat Metadata")
        content.append("")

        # Админы
        if "admins" in chat_info and chat_info["admins"]:
            admins_list = []
            for admin in chat_info["admins"]:
                admins_list.append(f"{admin['name']} {admin['username']}")
            content.append(f"**Admins:** {', '.join(admins_list)}")
        else:
            content.append("**Admins:** Не найдены")
        content.append("")

        # Владелец
        if "owner" in chat_info and chat_info["owner"]:
            owner = chat_info["owner"]
            content.append(f"**Owner:** {owner['name']} {owner['username']}")
        else:
            content.append("**Owner:** Неизвестно")
        content.append("")

        # История чата
        if "history_visible" in chat_info:
            history_status = "visible" if chat_info["history_visible"] else "hidden"
            content.append(f"**Chat History:** {history_status}")
        else:
            content.append("**Chat History:** Неизвестно")
        content.append("")

        # Участники
        if "participants" in chat_info and chat_info["participants"]:
            content.append(f"**Participants Count:** {len(chat_info['participants'])}")
            # Показать первых 10 участников
            participants_preview = chat_info["participants"][:10]
            participants_list = []
            for participant in participants_preview:
                participants_list.append(f"{participant['name']} {participant['username']}")
            content.append(f"**Participants:** {', '.join(participants_list)}")
            if len(chat_info["participants"]) > 10:
                content.append(f"... и еще {len(chat_info['participants']) - 10} участников")
        else:
            content.append("**Participants:** Не найдены")
        content.append("")

        # Оглавление
        content.append("## 📋 Table of Contents")
        for date in sorted(messages_by_date.keys(), reverse=True):
            day_messages = messages_by_date[date]
            participants = {msg["sender"] for msg in day_messages}
            content.append(f"- [{date}](#{date}) - {len(day_messages)} messages, {len(participants)} participants")
        content.append("")

        # Сообщения по дням
        for date in sorted(messages_by_date.keys(), reverse=True):
            day_messages = messages_by_date[date]

            content.append(f"## {date}")
            content.append("")

            # Группировать по участникам
            messages_by_sender = defaultdict(list)
            for msg in day_messages:
                messages_by_sender[msg["sender"]].append(msg)

            for sender in sorted(messages_by_sender.keys()):
                sender_messages = messages_by_sender[sender]
                content.append(f"### 👤 {sender}")
                content.append("")

                for msg in sender_messages:
                    time_str = msg["date"][11:16] if msg["date"] else "??:??"
                    content.append(f"**{time_str}**")
                    content.append(f"{msg['text']}")
                    content.append("")

        return "\n".join(content)

    def save_to_file(self, content: str, output_path: Path, filename: str = "chat.md") -> bool:
        """Сохранить контент в файл"""
        try:
            # Создать директорию если не существует
            output_path.mkdir(parents=True, exist_ok=True)

            # Сохранить файл
            file_path = output_path / filename
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)

            print(f"✅ Chat exported to: {file_path}")
            return True

        except Exception as e:
            print(f"❌ Error saving file: {e}")
            return False

    async def export_chat(self, chat_identifier: str, output_dir: str, is_chat_id: bool = False) -> bool:
        """Экспортировать чат"""
        try:
            # Инициализировать клиент
            if not await self.initialize_client():
                return False

            # Найти чат
            if is_chat_id:
                chat_id = int(chat_identifier)
                chat_info = await self.get_chat_info(chat_id)
            else:
                chat_info = await self.find_chat_by_name(chat_identifier)
                if not chat_info:
                    return False
                chat_id = chat_info["id"]

            if not chat_info:
                print(f"❌ Chat not found: {chat_identifier}")
                return False

            print(f"📱 Found chat: {chat_info['title']} (ID: {chat_info['id']})")

            # Экспортировать сообщения
            messages = await self.export_chat_messages(chat_id)

            if not messages:
                print("❌ No messages found in chat")
                return False

            # Форматировать для Markdown
            markdown_content = self.format_messages_for_markdown(messages, chat_info)

            # Сохранить файл
            output_path = Path(output_dir)
            success = self.save_to_file(markdown_content, output_path)

            # Создать README файл
            if success:
                self.create_readme_file(output_path, chat_info, len(messages))

            return success

        except Exception as e:
            print(f"❌ Error exporting chat: {e}")
            return False
        finally:
            if self.client:
                await self.client.disconnect()

    def create_readme_file(
        self,
        output_path: Path,
        chat_info: dict[str, Any],
        message_count: int,
        messages: list[Any] | None = None,
    ):
        """Создать README файл с информацией о проекте"""
        try:
            readme_content = f"""# Telegram Chat Export: {chat_info['title']}

## 📋 Project Information

- **Chat Name:** {chat_info['title']}
- **Chat ID:** {chat_info['id']}
- **Chat Type:** {chat_info['type']}
- **Total Messages:** {message_count}
- **Export Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📁 Files

- `chat.md` - Полная выгрузка сообщений в формате Markdown
- `README.md` - Этот файл с информацией о проекте

## 🔍 Navigation

Откройте файл `chat.md` для просмотра полной истории сообщений.
Сообщения сгруппированы по дням и участникам для удобного чтения.

## 📊 Statistics

- **Сообщений:** {message_count}
- **Дней активности:** {len({msg['date'][:10] for msg in (messages or []) if msg.get('date')})}
- **Участников:** {len({msg['sender'] for msg in (messages or []) if msg.get('sender')})}

## 🔒 Security

Все credentials хранятся в macOS Keychain для безопасного доступа к Telegram API.

## 📝 Usage

Этот экспорт создан с помощью telegram-mcp с интеграцией Mac Keychain.
Для повторного экспорта используйте команду:

```bash
python chat_exporter.py --chat_id {chat_info['id']} --output_dir .
```
"""

            readme_path = output_path / "README.md"
            with open(readme_path, "w", encoding="utf-8") as f:
                f.write(readme_content)

            print(f"✅ README created: {readme_path}")

        except Exception as e:
            print(f"❌ Error creating README: {e}")


def main():
    parser = argparse.ArgumentParser(description="Telegram Chat Exporter with Mac Keychain Integration")
    parser.add_argument("--chat_name", help="Chat name to search for")
    parser.add_argument("--chat_id", help="Chat ID (use negative numbers for groups/channels)")
    parser.add_argument("--output_dir", required=True, help="Output directory for exported files")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum number of messages to export")

    args = parser.parse_args()

    if not args.chat_name and not args.chat_id:
        print("❌ Please provide either --chat_name or --chat_id")
        sys.exit(1)

    exporter = ChatExporter()

    if args.chat_id:
        success = asyncio.run(exporter.export_chat(args.chat_id, args.output_dir, is_chat_id=True))
    else:
        success = asyncio.run(exporter.export_chat(args.chat_name, args.output_dir))

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
