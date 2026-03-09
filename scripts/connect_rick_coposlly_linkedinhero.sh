#!/bin/bash
# Telegram Session Creator for Rick Coposlly LinkedinHero
# Запускает скрипт подключения с правильным Python из .venv

cd "$(dirname "$0")/../../.."
.venv/bin/python3 heroes_platform/heroes_telegram_mcp/scripts/connect_rick_coposlly_linkedinhero.py
