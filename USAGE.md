# Telegram MCP Usage Guide

## Поддерживаемые пользователи

Telegram MCP поддерживает работу с двумя пользователями:
- **lisa** (по умолчанию)
- **ilyakrasinsky**

## Конфигурация

### В .cursor/mcp.json

```json
{
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
```

### Переключение пользователей

Для переключения на пользователя ilyakrasinsky измените:
```json
"TELEGRAM_USER": "ilyakrasinsky"
```

## Keychain структура

### Для пользователя lisa:
- `lisa_tg_api_key` → API ID
- `lisa_tg_app_hash` → API Hash  
- `lisa_tg_session` → Session String

### Для пользователя ilyakrasinsky:
- `telegram_api_id` → API ID
- `telegram_api_hash` → API Hash
- `telegram_session` → Session String

## Тестирование

### Проверка ключей lisa:
```bash
TELEGRAM_USER=lisa python3 -c "from main import get_telegram_credentials; print(get_telegram_credentials())"
```

### Проверка ключей ilyakrasinsky:
```bash
TELEGRAM_USER=ilyakrasinsky python3 -c "from main import get_telegram_credentials; print(get_telegram_credentials())"
```

## Запуск

### Автоматический запуск через Cursor
Telegram MCP автоматически:
1. Читает переменную `TELEGRAM_USER` (по умолчанию "lisa")
2. Получает ключи из macOS Keychain для указанного пользователя
3. Запускает Telegram клиент с полученными ключами
4. Запускает MCP сервер

### Ручной запуск из терминала

#### Проверка ключей:
```bash
python3 main.py --test-credentials
```

#### Список доступных инструментов:
```bash
python3 main.py --list-tools
```

#### Справка:
```bash
python3 main.py --help
```

#### Запуск MCP сервера:
```bash
python3 main.py
```

## Логи

При успешном запуске вы увидите:
```
✅ Credentials retrieved from Mac Keychain (individual)
✅ Using credentials for user: lisa
Starting Telegram client...
Telegram client started. Running MCP server...
```

## Решение проблем

### Cursor показывает "• No tools or prompts"
1. Проверьте ключи: `python3 main.py --test-credentials`
2. Убедитесь, что MCP сервер запускается: `python3 main.py --list-tools`
3. Перезапустите Cursor
4. Проверьте конфигурацию в `.cursor/mcp.json`
