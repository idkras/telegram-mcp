#!/usr/bin/env python3
"""
RELEASE 0: Валидация схемы Supabase для Telegram MCP Laba Deployment

JTBD: Как разработчик, я хочу проверить схему таблицы telegram_messages_raw в Supabase,
чтобы убедиться что структура данных соответствует формату bronze JSON из Telegram MCP.

Когда задан SUPABASE_DB_URL (или Keychain supabase_rick_db_url), валидация идёт через
прямое подключение Postgres (как laba/n8n) — Exposed schemas не требуются.

Стандарт: From-The-End Process Standard v2.9
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

# Добавляем путь для импорта credentials_manager
script_dir = Path(__file__).parent.parent.parent.parent
shared_path = script_dir / "heroes_platform" / "shared"
if shared_path.exists():
    sys.path.insert(0, str(shared_path))

try:
    from credentials_manager import credentials_manager
    from supabase import create_client, Client
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    sys.exit(1)

SUPABASE_URL = "https://supabase.rick.ai"
SUPABASE_SCHEMA = "rick_messages_tasks"  # Same as rick_clients_tasks
TABLE_NAME = "telegram_messages_raw"


def get_postgres_url() -> str | None:
    """Postgres URL для прямого подключения (как apply_telegram_migration / laba/n8n)."""
    url = os.getenv("SUPABASE_DB_URL")
    if url:
        return url
    try:
        result = credentials_manager.get_credential("supabase_rick_db_url")
        if result.success and result.value:
            return result.value
    except Exception:
        pass
    return None


def validate_via_direct_postgres(postgres_url: str) -> Dict[str, Any]:
    """Валидирует схему через прямое Postgres-подключение (без REST, без Exposed schemas)."""
    validation_result = {
        "table_exists": False,
        "columns": {},
        "indexes": [],
        "sample_data": None,
        "errors": [],
    }
    try:
        import psycopg2
    except ImportError:
        validation_result["errors"].append(
            "psycopg2 не установлен. pip install psycopg2-binary"
        )
        return validation_result
    try:
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM rick_messages_tasks.telegram_messages_raw LIMIT 1"
        )
        validation_result["table_exists"] = True
        row = cur.fetchone()
        if row:
            cols = [d[0] for d in cur.description]
            validation_result["sample_data"] = dict(zip(cols, row))
            validation_result["columns"] = {c: str(type(v).__name__) for c, v in zip(cols, row)}
        cur.close()
        conn.close()
    except Exception as e:
        validation_result["errors"].append(f"Ошибка при проверке через Postgres: {e}")
    return validation_result


def get_supabase_client() -> Client | None:
    """Получает Supabase клиент для REST API."""
    try:
        result = credentials_manager.get_credential("supabase_rick_api_key")
        if not result.success:
            raise ValueError(f"Не удалось получить API ключ: {result.error}")

        api_key = result.value
        if not api_key:
            raise ValueError("API ключ пустой")

        supabase: Client = create_client(SUPABASE_URL, api_key)
        return supabase
    except Exception as e:
        print(f"❌ Ошибка при создании Supabase клиента: {e}")
        import traceback

        traceback.print_exc()
        return None


def load_bronze_example() -> Dict[str, Any] | None:
    """Загружает пример bronze JSON файла для сравнения структуры."""
    # Ищем примеры bronze файлов
    bronze_paths = [
        Path("telegram_exported_channels/krasinsky_channels/bronze/ikrasinsky_7581298455_bronze.json"),
        Path("[heroes] telegram.ik.all/easypay_chats/EasyPay._Payments_Stripe_and_other_2306629974.md"),
    ]

    for bronze_path in bronze_paths:
        if bronze_path.exists():
            try:
                if bronze_path.suffix == ".json":
                    with open(bronze_path) as f:
                        data = json.load(f)
                        # Извлекаем пример сообщения из bronze структуры
                        if "messages" in data and len(data["messages"]) > 0:
                            # В bronze файлах структура: {"chat_id": ..., "messages": [{"id": ..., "date": ..., "sender": ..., "text": ..., ...}]}
                            # НО для Supabase нужно использовать message.to_dict() из Telethon для поля raw
                            example_message = data["messages"][0]
                            return {
                                "bronze_example": {
                                    "chat_id": data.get("chat_id"),
                                    "chat_title": data.get("chat_title"),
                                    "message": example_message,  # Структура сообщения из bronze
                                },
                                "bronze_structure": data,
                                "source_file": str(bronze_path),
                            }
                        return {
                            "bronze_structure": data,
                            "source_file": str(bronze_path),
                        }
            except Exception as e:
                print(f"⚠️  Ошибка чтения {bronze_path}: {e}")
                continue

    # Если файлы не найдены, используем структуру из документации
    return {
        "bronze_example": {
            "chat_id": 3570448731,
            "chat_title": "Example Chat",
            "retrieved_at_utc": "2026-01-08T21:44:18.407120Z",
            "raw": {
                "id": 21,
                "date": "2026-01-06T12:31:36+00:00",
                "sender": "Sender Name",
                "text": "Message text",
                "message_type": "text",
                "media": None,
                "files": [],
                "telegram_link": "https://t.me/c/3570448731/21",
            },
        },
        "source_file": "documentation_example",
    }


def validate_supabase_schema(supabase_client: Client) -> Dict[str, Any]:
    """Валидирует схему таблицы telegram_messages_raw в Supabase."""
    print("\n" + "=" * 80)
    print("🔍 ВАЛИДАЦИЯ СХЕМЫ SUPABASE: telegram_messages_raw")
    print("=" * 80)

    validation_result = {
        "table_exists": False,
        "columns": {},
        "indexes": [],
        "sample_data": None,
        "errors": [],
    }

    try:
        # Проверяем существование таблицы через запрос
        print("\n📋 Проверка существования таблицы...")
        tbl = supabase_client.schema(SUPABASE_SCHEMA).from_(TABLE_NAME)
        response = tbl.select("id").limit(1).execute()

        validation_result["table_exists"] = True
        print("✅ Таблица telegram_messages_raw существует")

        # Получаем структуру через запрос метаданных (если доступно)
        print("\n📊 Получение структуры таблицы...")
        # Пытаемся получить пример данных для анализа структуры
        sample_response = (
            supabase_client.schema(SUPABASE_SCHEMA)
            .from_(TABLE_NAME)
            .select("*")
            .limit(1)
            .execute()
        )

        if sample_response.data and len(sample_response.data) > 0:
            validation_result["sample_data"] = sample_response.data[0]
            print("✅ Получен пример данных из таблицы")
            print(f"   Поля в примере: {list(sample_response.data[0].keys())}")
        else:
            print("⚠️  Таблица пуста, используем схему из миграции")

        # Проверяем наличие обязательных полей согласно миграции
        required_fields = [
            "id",
            "created_at",
            "source",
            "telegram_user_id",
            "chat_id",
            "chat_type",
            "message_id",
            "sender_user_id",
            "sender_name",
            "sender_username",
            "message_ts",
            "text",
            "raw",
        ]

        print("\n🔍 Проверка обязательных полей...")
        if validation_result["sample_data"]:
            sample_fields = set(validation_result["sample_data"].keys())
            missing_fields = set(required_fields) - sample_fields
            if missing_fields:
                validation_result["errors"].append(
                    f"Отсутствуют поля: {missing_fields}"
                )
                print(f"⚠️  Отсутствуют поля: {missing_fields}")
            else:
                print("✅ Все обязательные поля присутствуют")
        else:
            print("⚠️  Не удалось проверить поля (таблица пуста)")

    except Exception as e:
        validation_result["errors"].append(f"Ошибка валидации: {e}")
        print(f"❌ Ошибка при валидации: {e}")
        import traceback

        traceback.print_exc()

    return validation_result


def compare_bronze_with_schema(
    bronze_example: Dict[str, Any], supabase_schema: Dict[str, Any]
) -> Dict[str, Any]:
    """Сравнивает структуру bronze данных со схемой Supabase."""
    print("\n" + "=" * 80)
    print("🔄 СРАВНЕНИЕ BRONZE СТРУКТУРЫ СО СХЕМОЙ SUPABASE")
    print("=" * 80)

    comparison = {
        "bronze_fields": [],
        "supabase_fields": [],
        "mapping": {},
        "gaps": [],
        "recommendations": [],
    }

    # Извлекаем поля из bronze примера
    if "bronze_example" in bronze_example:
        bronze_msg = bronze_example["bronze_example"].get("message", {})
        bronze_fields = set(bronze_msg.keys())
        comparison["bronze_fields"] = list(bronze_fields)
        print(f"\n📦 Поля в bronze message: {sorted(bronze_fields)}")
        print(f"   Пример: chat_id={bronze_example['bronze_example'].get('chat_id')}, message_id={bronze_msg.get('id')}")

    # Извлекаем поля из Supabase схемы
    if supabase_schema.get("sample_data"):
        supabase_fields = set(supabase_schema["sample_data"].keys())
        comparison["supabase_fields"] = list(supabase_fields)
        print(f"📊 Поля в Supabase: {sorted(supabase_fields)}")

    # Маппинг полей
    mapping = {
        "chat_id": "chat_id",
        "message_id": "message_id",
        "raw": "raw",
        "date": "message_ts",
        "sender": "sender_name",
        "text": "text",
    }

    comparison["mapping"] = mapping
    print(f"\n🔗 Маппинг полей: {mapping}")

    # Проверяем gaps
    if comparison["bronze_fields"] and comparison["supabase_fields"]:
        bronze_only = bronze_fields - supabase_fields
        supabase_only = supabase_fields - bronze_fields

        if bronze_only:
            comparison["gaps"].append(
                f"Поля только в bronze: {bronze_only}"
            )
            print(f"⚠️  Поля только в bronze: {bronze_only}")

        if supabase_only:
            comparison["gaps"].append(
                f"Поля только в Supabase: {supabase_only}"
            )
            print(f"ℹ️  Поля только в Supabase: {supabase_only}")

    # Рекомендации
    comparison["recommendations"] = [
        "Поле 'raw' должно содержать полный JSONB объект из bronze",
        "Поле 'message_ts' должно быть извлечено из 'raw.date'",
        "Поле 'sender_name' должно быть извлечено из 'raw.sender'",
        "Поле 'chat_id' должно быть извлечено из 'bronze.chat_id'",
        "Поле 'message_id' должно быть извлечено из 'raw.id'",
    ]

    print("\n💡 Рекомендации по маппингу:")
    for rec in comparison["recommendations"]:
        print(f"   - {rec}")

    return comparison


def main():
    """Основная функция валидации."""
    print("=" * 80)
    print("🚀 RELEASE 0: ВАЛИДАЦИЯ СХЕМЫ SUPABASE")
    print("=" * 80)

    # Шаг 1: Подключение — приоритет прямому Postgres (как laba/n8n)
    postgres_url = get_postgres_url()
    supabase_schema = None
    if postgres_url:
        print("\n📡 Шаг 1: Прямое подключение Postgres (SUPABASE_DB_URL / supabase_rick_db_url)")
        print("   Exposed schemas не требуются.")
        print("\n🔍 Валидация схемы через Postgres...")
        supabase_schema = validate_via_direct_postgres(postgres_url)
        if supabase_schema.get("errors"):
            print("⚠️  Ошибки при валидации через Postgres (см. ниже)")
        else:
            print("✅ Таблица rick_messages_tasks.telegram_messages_raw доступна")
    if not postgres_url or (supabase_schema and supabase_schema.get("errors")):
        if not postgres_url:
            print("\n📡 Шаг 1: Подключение к Supabase REST API...")
        supabase_client = get_supabase_client()
        if not supabase_client:
            print("❌ Не удалось подключиться к Supabase (REST). Задайте SUPABASE_DB_URL для прямого Postgres.")
            sys.exit(1)
        if not postgres_url:
            print("✅ Подключение к Supabase успешно")
        print("\n🔍 Валидация схемы через REST API...")
        supabase_schema = validate_supabase_schema(supabase_client)

    # Шаг 2: Загрузить пример bronze данных
    print("\n📦 Шаг 2: Загрузка примера bronze данных...")
    bronze_example = load_bronze_example()
    if bronze_example:
        print(f"✅ Загружен пример из: {bronze_example.get('source_file', 'unknown')}")
        if "bronze_example" in bronze_example:
            print(
                f"   Структура raw: {list(bronze_example['bronze_example'].get('raw', {}).keys())}"
            )

    # Шаг 3: Валидация схемы уже выполнена выше (Postgres или REST)

    # Шаг 4: Сравнить bronze со схемой
    print("\n🔄 Шаг 4: Сравнение структур...")
    comparison = compare_bronze_with_schema(bronze_example, supabase_schema)

    # Итоговый отчет
    print("\n" + "=" * 80)
    print("📊 ИТОГОВЫЙ ОТЧЕТ")
    print("=" * 80)

    print(f"\n✅ Таблица существует: {supabase_schema['table_exists']}")
    print(f"✅ Поля в Supabase: {len(supabase_schema.get('columns', {}))}")
    print(f"⚠️  Ошибки: {len(supabase_schema.get('errors', []))}")

    if supabase_schema.get("errors"):
        print("\n❌ Найденные ошибки:")
        for error in supabase_schema["errors"]:
            print(f"   - {error}")

    print("\n✅ Валидация завершена")
    print("\n📋 Следующие шаги:")
    print("   1. Убедиться что схема Supabase соответствует требованиям")
    print("   2. Создать модуль supabase_writer.py для записи сообщений")
    print("   3. Протестировать запись тестового сообщения")


if __name__ == "__main__":
    main()
