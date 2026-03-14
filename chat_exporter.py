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

# Import from heroes_telegram_mcp package (same repo)

# Same package (heroes_telegram_mcp); legacy "telegram-mcp" path no longer used
telegram_mcp_path = Path(__file__).parent.parent / "heroes_telegram_mcp"
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
                admins = await self.client.get_participants(
                    entity, filter=ChannelParticipantsAdmins
                )
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
                admins = await self.client.get_participants(
                    entity, filter=ChannelParticipantsAdmins
                )
                for admin in admins:
                    if (
                        hasattr(admin, "admin_rights")
                        and admin.admin_rights
                        and admin.admin_rights.other
                    ):
                        return {
                            "name": f"{admin.first_name} {admin.last_name or ''}".strip(),
                            "username": (
                                f"@{admin.username}" if admin.username else "Нет username"
                            ),
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
                        "username": (
                            f"@{participant.username}" if participant.username else "Нет username"
                        ),
                        "id": participant.id,
                    }
                    for participant in participants
                ]
            return []
        except Exception as e:
            print(f"❌ Error getting participants: {e}")
            return []

    async def export_chat_messages(
        self,
        chat_id: int,
        limit: int = 1000,
        download_media: bool = True,
        download_videos: bool = False,
        max_file_size_mb: int = 100,
        output_dir: Path | None = None,
        markdown_dir: Path
        | None = None,  # ✅ Директория где находится markdown файл (для правильных относительных путей)
        min_id: int | None = None,  # ✅ Минимальный ID сообщения (для инкрементального обновления)
    ) -> list[dict[str, Any]]:
        """Экспортировать сообщения из чата с поддержкой медиа"""
        try:
            entity = await self.client.get_entity(chat_id)

            # Получить все сообщения с пагинацией для получения ВСЕХ постов
            # Если limit=None, получаем все сообщения от начала канала
            # Если min_id указан, получаем только сообщения с ID > min_id (инкрементальное обновление)
            if limit is None:
                all_messages = []
                offset_id = 0
                last_batch_ids = set()  # ✅ Защита от зацикливания
                max_iterations = (
                    10000  # ✅ Защита от бесконечного цикла (10000 * 100 = 1M сообщений максимум)
                )
                iteration = 0

                while iteration < max_iterations:
                    iteration += 1
                    # Получаем сообщения порциями по 100
                    # Если min_id указан, используем его для фильтрации
                    # ✅ НЕ передаем min_id вообще, если он None (Telethon не принимает None)
                    get_messages_kwargs = {"entity": entity, "limit": 100, "offset_id": offset_id}
                    if min_id is not None:
                        get_messages_kwargs["min_id"] = min_id

                    batch = await self.client.get_messages(**get_messages_kwargs)
                    if not batch:
                        break

                    # ✅ Проверка на дубликаты - защита от зацикливания
                    batch_ids = {msg.id for msg in batch}
                    if batch_ids.intersection(last_batch_ids):
                        # Получили те же сообщения - выходим
                        print(
                            f"⚠️ Received duplicate messages, stopping pagination (iteration {iteration})"
                        )
                        break
                    last_batch_ids = batch_ids

                    all_messages.extend(batch)
                    if len(batch) < 100:
                        break

                    # offset_id = ID последнего сообщения для следующей порции
                    offset_id = batch[-1].id
                    # Небольшая задержка для избежания rate limiting
                    await asyncio.sleep(0.1)

                if iteration >= max_iterations:
                    print(f"⚠️ Reached maximum iterations ({max_iterations}), stopping pagination")

                messages = all_messages
            else:
                # Если limit указан, получаем только указанное количество
                messages = await self.client.get_messages(entity, limit=limit)
            exported_messages = []

            for message in messages:
                # Получить информацию об отправителе
                sender = await message.get_sender()
                sender_name = "Unknown"

                if sender:
                    if hasattr(sender, "first_name"):
                        sender_name = sender.first_name
                        if hasattr(sender, "last_name") and sender.last_name:
                            sender_name += f" {sender.last_name}"
                    elif hasattr(sender, "title"):
                        sender_name = sender.title

                # Получить ссылку на сообщение в Telegram
                telegram_link = await self._get_message_link(entity, message)

                # ✅ Сохраняем полный raw ответ от Telegram API для Dagster
                # message.to_dict() возвращает полную структуру сообщения со всеми полями
                try:
                    raw_message_dict = message.to_dict()
                except Exception as e:
                    # Fallback: если to_dict() не работает, создаем базовую структуру
                    print(f"⚠️ Warning: message.to_dict() failed for message {message.id}: {e}")
                    raw_message_dict = {
                        "id": message.id,
                        "date": message.date.isoformat() if message.date else None,
                        "message": message.text or "",
                        "from_id": message.from_id.to_dict() if message.from_id else None,
                        "reply_to": message.reply_to.to_dict() if message.reply_to else None,
                        "media": message.media.to_dict() if message.media else None,
                    }

                msg_data = {
                    "id": message.id,
                    "date": message.date.isoformat() if message.date else None,
                    "sender": sender_name,
                    "text": message.text or "",
                    "message_type": "text",
                    "media": None,
                    "files": [],
                    "telegram_link": telegram_link,  # ✅ Ссылка на сообщение в Telegram
                    "raw": raw_message_dict,  # ✅ Полный raw ответ от Telegram API (для Dagster)
                }

                # ✅ ПРИОРИТЕТ 1: Проверка и выгрузка картинок (фото)
                if message.media and hasattr(message.media, "photo"):
                    if download_media and output_dir:
                        photo_path = await self._download_photo(message, output_dir)
                        if photo_path:
                            # ✅ Используем метод для вычисления относительного пути
                            relative_path = self._get_relative_path(
                                photo_path, markdown_dir, output_dir
                            )
                            msg_data["media"] = {"type": "photo", "path": relative_path}
                    else:
                        msg_data["media"] = {"type": "photo", "path": None}

                # ✅ ПРИОРИТЕТ 1: Проверка и выгрузка аудио сообщений
                if message.media and hasattr(message.media, "voice"):
                    if download_media and output_dir:
                        audio_path = await self._download_audio(message, output_dir)
                        if audio_path:
                            # ✅ Используем метод для вычисления относительного пути
                            relative_path = self._get_relative_path(
                                audio_path, markdown_dir, output_dir
                            )
                            msg_data["media"] = {"type": "voice", "path": relative_path}
                    else:
                        msg_data["media"] = {"type": "voice", "path": None}

                # ✅ ПРИОРИТЕТ 2: Проверка и выгрузка документов
                if message.media and hasattr(message.media, "document"):
                    doc = message.media.document
                    file_size_mb = doc.size / (1024 * 1024) if hasattr(doc, "size") else 0

                    if file_size_mb <= max_file_size_mb:
                        if download_media and output_dir:
                            doc_path = await self._download_document(message, output_dir)
                            if doc_path:
                                filename = None
                                if hasattr(doc, "attributes"):
                                    for attr in doc.attributes:
                                        if hasattr(attr, "file_name"):
                                            filename = attr.file_name
                                            break
                                # ✅ Используем метод для вычисления относительного пути
                                relative_path = self._get_relative_path(
                                    doc_path, markdown_dir, output_dir
                                )
                                msg_data["files"].append(
                                    {
                                        "type": "document",
                                        "name": filename or "document",
                                        "size_mb": file_size_mb,
                                        "path": relative_path,
                                    }
                                )
                    else:
                        # ✅ Для невыгруженных файлов добавляем ссылку на Telegram
                        filename = None
                        if hasattr(doc, "attributes"):
                            for attr in doc.attributes:
                                if hasattr(attr, "file_name"):
                                    filename = attr.file_name
                                    break
                        msg_data["files"].append(
                            {
                                "type": "document",
                                "name": filename or "document",
                                "size_mb": file_size_mb,
                                "path": None,
                                "skipped": True,
                                "reason": f"File too large ({file_size_mb:.2f}MB > {max_file_size_mb}MB)",
                                "telegram_link": telegram_link,  # ✅ Ссылка на сообщение в Telegram
                            }
                        )

                # ✅ ПРИОРИТЕТ 2: Проверка и выгрузка видео
                if message.media and hasattr(message.media, "video"):
                    video = message.media.video
                    file_size_mb = video.size / (1024 * 1024) if hasattr(video, "size") else 0

                    msg_data["media"] = {
                        "type": "video",
                        "size_mb": file_size_mb,
                        "path": None,
                        "telegram_link": telegram_link,
                    }

                    if (
                        download_videos
                        and file_size_mb <= max_file_size_mb
                        and download_media
                        and output_dir
                    ):
                        video_path = await self._download_video(message, output_dir)
                        if video_path:
                            # ✅ Используем метод для вычисления относительного пути
                            relative_path = self._get_relative_path(
                                video_path, markdown_dir, output_dir
                            )
                            msg_data["media"]["path"] = relative_path

                # Добавляем сообщение даже если нет текста (только медиа)
                if message.text or msg_data.get("media") or msg_data.get("files"):
                    exported_messages.append(msg_data)

            print(
                f"✅ Exported {len(exported_messages)} messages from chat (total fetched: {len(messages)})"
            )
            return exported_messages

        except Exception as e:
            print(f"❌ Error exporting messages: {e}")
            import traceback

            traceback.print_exc()
            return []

    async def _download_photo(self, message, output_dir: Path) -> Path | None:
        """Скачать фото из сообщения"""
        try:
            photos_dir = output_dir / "photos"
            photos_dir.mkdir(parents=True, exist_ok=True)

            timestamp = int(message.date.timestamp()) if message.date else message.id
            file_path = photos_dir / f"{message.id}_{timestamp}.jpg"

            downloaded_path = await self.client.download_media(message, file=file_path)
            if downloaded_path:
                return Path(downloaded_path)
            return None
        except Exception as e:
            print(f"⚠️ Failed to download photo from message {message.id}: {e}")
            return None

    async def _download_audio(self, message, output_dir: Path) -> Path | None:
        """Скачать аудио сообщение"""
        try:
            audio_dir = output_dir / "audio"
            audio_dir.mkdir(parents=True, exist_ok=True)

            timestamp = int(message.date.timestamp()) if message.date else message.id
            file_path = audio_dir / f"{message.id}_{timestamp}.ogg"

            downloaded_path = await self.client.download_media(message, file=file_path)
            if downloaded_path:
                return Path(downloaded_path)
            return None
        except Exception as e:
            print(f"⚠️ Failed to download audio from message {message.id}: {e}")
            return None

    async def _download_document(self, message, output_dir: Path) -> Path | None:
        """Скачать документ из сообщения"""
        try:
            documents_dir = output_dir / "documents"
            documents_dir.mkdir(parents=True, exist_ok=True)

            # Получить имя файла из атрибутов документа
            filename = None
            if hasattr(message.media, "document") and hasattr(
                message.media.document, "attributes"
            ):
                for attr in message.media.document.attributes:
                    if hasattr(attr, "file_name"):
                        filename = attr.file_name
                        break

            if not filename:
                filename = f"document_{message.id}"

            file_path = documents_dir / filename

            downloaded_path = await self.client.download_media(message, file=file_path)
            if downloaded_path:
                return Path(downloaded_path)
            return None
        except Exception as e:
            print(f"⚠️ Failed to download document from message {message.id}: {e}")
            return None

    async def _download_video(self, message, output_dir: Path) -> Path | None:
        """Скачать видео из сообщения"""
        try:
            videos_dir = output_dir / "videos"
            videos_dir.mkdir(parents=True, exist_ok=True)

            timestamp = int(message.date.timestamp()) if message.date else message.id
            file_path = videos_dir / f"{message.id}_{timestamp}.mp4"

            downloaded_path = await self.client.download_media(message, file=file_path)
            if downloaded_path:
                return Path(downloaded_path)
            return None
        except Exception as e:
            print(f"⚠️ Failed to download video from message {message.id}: {e}")
            return None

    def _get_base_dir_for_relative_path(
        self, markdown_dir: Path | None, output_dir: Path | None
    ) -> Path | None:
        """Получить базовую директорию для вычисления относительных путей"""
        try:
            # ✅ Приоритет 1: markdown_dir (где находится markdown файл)
            if markdown_dir and isinstance(markdown_dir, Path) and markdown_dir.exists():
                return Path(markdown_dir)

            # ✅ Приоритет 2: output_dir.parent (родительская директория медиа файлов)
            if output_dir and isinstance(output_dir, Path) and output_dir.parent.exists():
                return Path(output_dir.parent)

            # ✅ Приоритет 3: output_dir (сама директория медиа файлов)
            if output_dir and isinstance(output_dir, Path):
                return Path(output_dir)

            return None
        except Exception as e:
            print(f"⚠️ Failed to get base dir for relative path: {e}")
            return None

    def _get_relative_path(
        self, file_path: Path, markdown_dir: Path | None, output_dir: Path | None
    ) -> str:
        """Вычислить относительный путь к файлу относительно markdown файла"""
        try:
            if not file_path or not isinstance(file_path, Path):
                return str(file_path) if file_path else ""

            base_dir = self._get_base_dir_for_relative_path(markdown_dir, output_dir)
            if not base_dir:
                return str(file_path)

            # Преобразуем в Path для проверки
            base_dir = Path(base_dir)
            file_path = Path(file_path)

            # Проверяем что пути на одном диске и file_path является подпутем base_dir
            try:
                if file_path.is_relative_to(base_dir):
                    return str(file_path.relative_to(base_dir))
            except (ValueError, AttributeError):
                # Если is_relative_to не работает (разные диски или старая версия Python)
                # Пробуем альтернативный способ
                try:
                    relative = file_path.relative_to(base_dir)
                    return str(relative)
                except ValueError:
                    # Пути на разных дисках или не связаны - используем абсолютный путь
                    return str(file_path)

            return str(file_path)
        except Exception as e:
            print(f"⚠️ Failed to get relative path for {file_path}: {e}")
            return str(file_path) if file_path else ""

    async def _get_message_link(self, entity, message) -> str | None:
        """Получить ссылку на сообщение в Telegram"""
        try:
            # Получить username канала/группы (если есть)
            username = getattr(entity, "username", None)
            chat_id = entity.id

            # Для публичных каналов/групп: https://t.me/{username}/{message_id}
            if username:
                return f"https://t.me/{username}/{message.id}"

            # Для приватных каналов/групп: https://t.me/c/{chat_id}/{message_id}
            # chat_id должен быть положительным числом (убираем минус если есть)
            if chat_id < 0:
                chat_id = abs(chat_id)
            return f"https://t.me/c/{chat_id}/{message.id}"
        except Exception as e:
            print(f"⚠️ Failed to get message link for message {message.id}: {e}")
            return None

    def format_messages_for_markdown(
        self, messages: list[dict[str, Any]], chat_info: dict[str, Any]
    ) -> str:
        """Форматировать сообщения для Markdown файла"""
        if not messages:
            return "# No messages found"

        # Группировать сообщения по дням
        messages_by_date = defaultdict(list)

        for message in messages:
            date_value = message.get("date")
            # ✅ Проверяем что date не None и является строкой
            if date_value and isinstance(date_value, str) and len(date_value) >= 10:
                date_str = date_value[:10]
                messages_by_date[date_str].append(message)
            elif date_value and isinstance(date_value, str):
                # Если дата есть, но короче 10 символов, используем как есть
                date_str = date_value
                messages_by_date[date_str].append(message)
            else:
                # Сообщения без даты добавляем в отдельную группу
                messages_by_date["Unknown Date"].append(message)

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
            participants = {msg.get("sender") or "Unknown" for msg in day_messages}
            content.append(
                f"- [{date}](#{date}) - {len(day_messages)} messages, {len(participants)} participants"
            )
        content.append("")

        # Сообщения по дням
        for date in sorted(messages_by_date.keys(), reverse=True):
            day_messages = messages_by_date[date]

            content.append(f"## {date}")
            content.append("")

            # Группировать по участникам
            messages_by_sender = defaultdict(list)
            for msg in day_messages:
                sender = msg.get("sender") or "Unknown"
                messages_by_sender[sender].append(msg)

            for sender in sorted(
                messages_by_sender.keys(), key=lambda x: (x == "Unknown", x or "")
            ):
                sender_messages = messages_by_sender[sender]
                content.append(f"### 👤 {sender}")
                content.append("")

                for msg in sender_messages:
                    time_str = "??:??"
                    if msg.get("date") and len(msg["date"]) >= 16:
                        time_str = msg["date"][11:16]
                    content.append(f"**{time_str}**")
                    content.append(f"{msg.get('text', '[No text]')}")

                    # ✅ Добавить ссылки на медиа (приоритет 1: картинки и аудио)
                    if msg.get("media"):
                        media = msg["media"]
                        if media["type"] == "photo" and media.get("path"):
                            content.append(f"🖼️ [Photo]({media['path']})")
                        elif media["type"] == "voice" and media.get("path"):
                            content.append(f"🎤 [Voice Message]({media['path']})")
                        elif media["type"] == "video" and media.get("path"):
                            size_info = (
                                f" ({media.get('size_mb', 0):.2f} MB)"
                                if media.get("size_mb")
                                else ""
                            )
                            content.append(f"🎥 [Video]({media['path']}){size_info}")
                        elif media["type"] == "video" and media.get("telegram_link"):
                            size_info = (
                                f" ({media.get('size_mb', 0):.2f} MB)"
                                if media.get("size_mb")
                                else ""
                            )
                            content.append(
                                f"🎥 [Video in Telegram]({media['telegram_link']}){size_info}"
                            )

                    # ✅ Добавить ссылки на файлы
                    if msg.get("files"):
                        for file_info in msg["files"]:
                            if file_info.get("skipped"):
                                # ✅ Для невыгруженных файлов добавляем ссылку на Telegram
                                telegram_link = file_info.get("telegram_link") or msg.get(
                                    "telegram_link"
                                )
                                if telegram_link:
                                    content.append(
                                        f"📎 [{file_info['name']}]({telegram_link}) - {file_info.get('reason', 'Skipped')} (файл в Telegram)"
                                    )
                                else:
                                    content.append(
                                        f"📎 {file_info['name']} - {file_info.get('reason', 'Skipped')}"
                                    )
                            elif file_info.get("path"):
                                size_info = (
                                    f" ({file_info.get('size_mb', 0):.2f} MB)"
                                    if file_info.get("size_mb")
                                    else ""
                                )
                                content.append(
                                    f"📎 [{file_info['name']}]({file_info['path']}){size_info}"
                                )

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

    async def export_chat(
        self, chat_identifier: str, output_dir: str, is_chat_id: bool = False
    ) -> bool:
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

Этот экспорт создан с помощью heroes_telegram_mcp (Telegram MCP) с интеграцией Mac Keychain.
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
    parser = argparse.ArgumentParser(
        description="Telegram Chat Exporter with Mac Keychain Integration"
    )
    parser.add_argument("--chat_name", help="Chat name to search for")
    parser.add_argument("--chat_id", help="Chat ID (use negative numbers for groups/channels)")
    parser.add_argument("--output_dir", required=True, help="Output directory for exported files")
    parser.add_argument(
        "--limit", type=int, default=1000, help="Maximum number of messages to export"
    )

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
