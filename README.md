# Telegram MCP Server

Telegram MCP Server предоставляет полный доступ к Telegram API через Model Context Protocol (MCP) для Cursor IDE.

## Возможности

- 📱 Полный доступ к Telegram API (73+ инструментов)
- 🔐 Безопасное хранение ключей в macOS Keychain
- 👥 Один стабильный профиль на endpoint (`lisa` или `ikrasinsky`)
- 🛠️ CLI команды для тестирования и отладки
- 📊 Интеграция с Cursor IDE
- 📋 Получение метаданных чатов (админы, владелец, участники, настройки истории)
- 🔍 Анализ структуры чатов для Rick.ai клиентов

## Быстрый старт

### 1. Установка зависимостей
```bash
pip install -r requirements.txt
```

### 2. Настройка ключей через credential registry

#### Для профиля lisa

Зарегистрируйте logical ids `lisa_tg_api_key`, `lisa_tg_app_hash`,
`lisa_tg_session` через `credentials_registry`; backend выбирается
метаданными registry.

#### Для профиля ik

Используйте зарегистрированные logical ids `telegram_api_id`, `telegram_api_hash`,
`telegram_session` и, когда нужен номер, `telegram_phone`;
не обращайтесь к Keychain или Windows Credential Manager напрямую.

### 2.1. Секреты sandbox-ik: Bitwarden + GPG + systemd credentials

Для `sandbox-ik` действуют два разных слоя хранения одного payload:

- portable backup/source for recovery — `config/sandbox-ik-<profile>.secrets.env.gpg`,
  совместимый с Heromaton/Laba (`rickai/secrets`, symmetric GPG AES256,
  Bitwarden item `Services secrets passphrase`);
- VPS runtime — host-encrypted
  `/etc/credstore.encrypted/telegram-mcp-<profile>.env.cred`, который systemd
  расшифровывает только в `/run/credentials/<unit>/telegram_env` на время жизни
  процесса.

`TELEGRAM_USER` не входит в secret payload: профиль должен быть явно закреплён
в unit (`lisa` или `ikrasinsky`). Поэтому один и тот же credential нельзя
использовать как переключатель аккаунта.

Production deploy использует чистый checkout
`/home/idkras/telegram-mcp-production`. Исторический dirty checkout
`/home/idkras/telegram-mcp` не сбрасывается и не обновляется поверх локальных
hotfix-файлов. После merge сначала доставляется только код без изменения units:

```bash
deploy/deploy-sandbox-ik.sh --prepare-code-only
```

Полное переключение выполняется тем же скриптом только после установки и
успешного readback обоих encrypted credentials.

Первичная настройка Bitwarden и portable archive (пароль/ключи не вводить в чат):

```bash
./scripts/secrets-tool.sh init https://84.252.142.100
cp config/sandbox-ik.secret-template.env config/sandbox-ik-lisa.secrets.env
chmod 600 config/sandbox-ik-lisa.secrets.env
$EDITOR config/sandbox-ik-lisa.secrets.env
./scripts/secrets-tool.sh encrypt sandbox-ik-lisa
./scripts/secrets-tool.sh clear sandbox-ik-lisa
```

Повторить для `sandbox-ik-ikrasinsky`. В Git добавляется только `.gpg`; файл
`config/*.secrets.env` игнорируется. `just secrets ...` вызывает тот же wrapper,
но `just` не является обязательной зависимостью. Wrapper намеренно не принимает
`SECRETS_PASSPHRASE` из environment: interactive passphrase source — только
Bitwarden; `init`, `encrypt` и `decrypt` без terminal TTY завершаются fail-closed.

Миграция уже существующего root-only `/etc/telegram-mcp/env.d/<profile>.env`
делается без вывода значений одной командой. Она создаёт portable `.gpg`, делает
`clear → decrypt → cmp`, устанавливает host-encrypted credential и сохраняет
legacy env до последующих runtime/Supabase readbacks:

```bash
./scripts/secrets-sandbox-ik-migrate.sh lisa sandbox-ik.infra.node.rickai.net
```

Systemd unit должен использовать:

```ini
LoadCredentialEncrypted=telegram_env:/etc/credstore.encrypted/telegram-mcp-lisa.env.cred
Environment=TELEGRAM_USER=lisa
ExecStart=/home/idkras/telegram-mcp-production/.venv/bin/python deploy/run-with-encrypted-credential.py /home/idkras/telegram-mcp-production/.venv/bin/python listener.py
```

Перед переключением units encrypted blob проверяется реальным
`systemd-creds decrypt --name=telegram_env`, канонической структурой payload и
полным набором обязательных ключей. Непустой, но повреждённый blob не проходит.

Для IK в трёх местах заменить `lisa` на `ikrasinsky`. Удалять legacy `.env`
можно только после четырёх независимых зелёных проверок: GPG decrypt, internal
`systemd-creds` readback (`readback=match`), restart/health нужного profile unit,
и физическая запись нового Telegram message в соответствующую Supabase schema.
До этого legacy-файл — rollback source и не удаляется.

При неудачном обновлении installer оставляет предыдущий encrypted blob с
суффиксом `.rollback`. Откат не раскрывает plaintext:

```bash
sudo -n systemctl stop telegram-mcp-lisa.service
sudo -n mv /etc/credstore.encrypted/telegram-mcp-lisa.env.cred \
  /etc/credstore.encrypted/telegram-mcp-lisa.env.cred.failed
sudo -n mv /etc/credstore.encrypted/telegram-mcp-lisa.env.cred.rollback \
  /etc/credstore.encrypted/telegram-mcp-lisa.env.cred
sudo -n systemctl start telegram-mcp-lisa.service
```

### 3. Тестирование
```bash
# Тест профиля lisa (по умолчанию)
python3 main.py --test-credentials

# Тест профиля ik
TELEGRAM_USER=ik python3 main.py --test-credentials

# Список доступных инструментов
python3 main.py --list-tools
```

### 4. Получение метаданных чатов

#### Новые MCP инструменты для анализа чатов:

- **`get_chat_metadata(chat_id: int)`** - Получить полные метаданные чата:
  - Список админов с именами и username
  - Владелец чата
  - Настройки истории чата (visible/hidden)
  - Список участников (первые 10)
  - Количество участников

- **`get_chat(chat_id: int)`** - Расширенная информация о чате:
  - Базовая информация + метаданные
  - Админы, владелец, настройки истории
  - Участники и их количество

#### Пример использования:
```python
# Получить метаданные чата
metadata = await get_chat_metadata(chat_id)
# Результат: JSON с полными метаданными

# Получить расширенную информацию
chat_info = await get_chat(chat_id)
# Результат: Текстовый формат с метаданными
```

### 5. Настройка в Cursor

Добавьте в `.cursor/mcp.json`:
```json
{
  "mcpServers": {
    "telegram-mcp": {
      "command": "${workspaceFolder}/.venv/bin/python",
      "args": [
        "-m", "credentials_registry.service_env",
        "telegram",
        "${workspaceFolder}/.venv/bin/python",
        "${workspaceFolder}/heroes_platform/heroes_telegram_mcp/main.py"
      ],
      "env": {
        "PYTHONPATH": "${workspaceFolder}",
        "TELEGRAM_USER": "lisa"
      }
    }
  }
}
```

## Использование

### Канонический пакет

- Командный source of truth: `heroes_platform/heroes_telegram_mcp`
- Legacy compatibility path `heroes_platform/telegram_mcp` сохраняется только для старых ссылок и миграции
- Для Cursor, health-checks, deployment и teammate setup использовать только `heroes_platform/heroes_telegram_mcp/main.py`

### Origin / upstream contract

- `origin` должен указывать на командный fork
- `upstream` должен указывать на исходный репозиторий `chigwell/telegram-mcp`
- Обновление делается так:

```bash
git -C heroes_platform/heroes_telegram_mcp fetch upstream
git -C heroes_platform/heroes_telegram_mcp checkout main
git -C heroes_platform/heroes_telegram_mcp merge --ff-only upstream/main || git -C heroes_platform/heroes_telegram_mcp merge upstream/main
git -C heroes_platform/heroes_telegram_mcp push origin main
git add heroes_platform/heroes_telegram_mcp
git commit -m "chore: update heroes_telegram_mcp submodule ref"
```

Если в submodule есть наши product-specific изменения, сначала держим их в опубликованной ветке `ik-codex/*` или `feature/*`, потом переносим в `main` командного fork, и только после этого обновляем root gitlink.

### CLI команды
- `--help` - Справка
- `--test-credentials` - Тест ключей
- `--list-tools` - Список инструментов
- `--version` - Версия

### Выбор профиля

Профиль выбирается endpoint-ом, а не переключается внутри процесса:

- `telegram-mcp-lisa` / port `8767` → `TELEGRAM_USER=lisa`;
- `telegram-mcp-ikrasinsky` / port `8766` → `TELEGRAM_USER=ikrasinsky`.

В tool call используйте `profile="current"` или не передавайте profile. Старый
`default` остаётся только alias к текущему endpoint. Cross-profile запрос
завершается ошибкой с именем нужного endpoint.

## Таблицы Supabase: ik_telegram_chats и rick_telegram_chats

- **ik_telegram_chats** — полная выгрузка всех чатов IK (исходная таблица после синка).
- **rick_telegram_chats** — подмножество только чатов Rick.ai (клиенты, внутренние, бот): для быстрого поиска и выгрузки сообщений.

В обе таблицы добавлена колонка **segment** (тип чата для индексации):

| segment            | Описание |
|--------------------|----------|
| advising           | Advising клиенты Rick.ai |
| pilot              | Пилот |
| dogovorennosti     | Договорённости |
| na_soprovozhdenii   | На сопровождении |
| partners           | Партнёры |
| internal           | Внутренние Rick (Flow, подстраховка, PM Care и т.д.) |
| bot_feedback       | Rick.ai bot feedback |
| community          | Комьюнити Heroes (HOC, PH, Management) |
| other              | прочее |

**Миграции:** 1) `20250215000000_rick_telegram_chats_create.sql` — создать таблицы `rick_telegram_chats` и `ik_telegram_chats` (если ещё нет). 2) `20250216000001_telegram_chats_segment.sql` — добавить колонку `segment` и индексы. Применить: Supabase Dashboard → SQL Editor (схема `rick_messages_tasks`). Либо: `APPLY_MIGRATION_FILE=.../20250215000000_rick_telegram_chats_create.sql python -m heroes_platform.heroes_telegram_mcp.scripts.apply_telegram_migration`, затем то же для `20250216000001_telegram_chats_segment.sql`.

**Заполнение rick_telegram_chats (только чаты Rick.ai + индексация по segment):** запустить скрипт `python -m heroes_platform.heroes_telegram_mcp.scripts.sync_rick_telegram_chats_from_telegram_chats`. Скрипт читает `telegram_chats`, оставляет только чаты с «rick.ai»/«rick» в названии или username, проставляет segment по ключевым словам (advising, pilot, dogovorennosti, na_soprovozhdenii и т.д.) и перезаписывает `rick_telegram_chats`. Поиск и выгрузка сообщений: по таблице `rick_telegram_chats` с `WHERE segment = 'advising'` и т.д.

## Структура проекта

```
telegram-mcp/
├── main.py                    # Основной MCP сервер (официальная версия)
├── keychain_integration.py    # Интеграция с macOS Keychain
├── telegram_profile_manager.py # Менеджер профилей
├── chat_exporter.py          # Экспорт чатов
├── session_string_generator.py # Генератор сессий
├── requirements.txt          # Зависимости Python
├── USAGE.md                  # Подробная документация
└── README.md                 # Этот файл
```

## Файл main.py

Основной файл `main.py` содержит:

- **MCP сервер** - Реализация Model Context Protocol для интеграции с Cursor IDE
- **50+ инструментов** - Полный набор функций для работы с Telegram API
- **Безопасная аутентификация** - Поддержка string sessions и file-based sessions
- **Обработка ошибок** - Комплексная система логирования и обработки исключений
- **CLI интерфейс** - Командная строка для тестирования и отладки

### Основные компоненты:

1. **Инициализация клиента** - Настройка TelegramClient с поддержкой различных типов сессий
2. **MCP инструменты** - Декораторы `@mcp.tool()` для всех функций Telegram API
3. **Обработка ошибок** - Функция `log_and_format_error()` для унифицированной обработки
4. **CLI команды** - Аргументы командной строки для тестирования и диагностики

### Источник:
Файл основан на официальном репозитории: [github.com/chigwell/telegram-mcp](https://github.com/chigwell/telegram-mcp)

## Решение проблем

### Cursor показывает "• No tools or prompts"
1. Проверьте ключи: `python3 main.py --test-credentials`
2. Убедитесь, что MCP сервер запускается: `python3 main.py --list-tools`
3. Перезапустите Cursor
4. Проверьте конфигурацию в `.cursor/mcp.json`

### Ключи не найдены
1. Проверьте правильность названий ключей в keychain
2. Убедитесь, что account name соответствует профилю
3. Используйте `telegram_profile_manager.py` для диагностики

## Лицензия

MIT License - см. файл LICENSE
