# Telegram MCP Server

Telegram MCP Server предоставляет полный доступ к Telegram API через Model Context Protocol (MCP) для Cursor IDE.

## Возможности

- 📱 Полный доступ к Telegram API (73+ инструментов)
- 🔐 Безопасное хранение ключей в macOS Keychain
- 👥 Поддержка множественных профилей (lisa, ik, ilyakrasinsky)
- 🛠️ CLI команды для тестирования и отладки
- 📊 Интеграция с Cursor IDE
- 📋 Получение метаданных чатов (админы, владелец, участники, настройки истории)
- 🔍 Анализ структуры чатов для Rick.ai клиентов

## Быстрый старт

### 1. Установка зависимостей
```bash
pip install -r requirements.txt
```

### 2. Настройка ключей в macOS Keychain

#### Для профиля lisa:
```bash
security add-generic-password -s "lisa_tg_api_key" -a "lisa" -w "YOUR_API_ID"
security add-generic-password -s "lisa_tg_app_hash" -a "lisa" -w "YOUR_API_HASH"
security add-generic-password -s "lisa_tg_session" -a "lisa" -w "YOUR_SESSION_STRING"
```

#### Для профиля ik:
```bash
security add-generic-password -s "ik_tg_api_id" -a "ilyakrasinsky" -w "YOUR_API_ID"
security add-generic-password -s "ik_tg_api_hash" -a "ilyakrasinsky" -w "YOUR_API_HASH"
security add-generic-password -s "ik_tg_session" -a "ilyakrasinsky" -w "YOUR_SESSION_STRING"
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
      "command": "python3",
      "args": ["main.py"],
      "cwd": "${workspaceFolder}/heroes-platform/telegram-mcp",
      "env": {
        "PYTHONPATH": "${workspaceFolder}/heroes-platform/telegram-mcp",
        "TELEGRAM_USER": "lisa"
      }
    }
  }
}
```

## Использование

### CLI команды
- `--help` - Справка
- `--test-credentials` - Тест ключей
- `--list-tools` - Список инструментов
- `--version` - Версия

### Переключение профилей
Измените `TELEGRAM_USER` в конфигурации MCP:
- `"TELEGRAM_USER": "lisa"` - профиль lisa
- `"TELEGRAM_USER": "ik"` - профиль ik
- `"TELEGRAM_USER": "ilyakrasinsky"` - профиль ilyakrasinsky

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