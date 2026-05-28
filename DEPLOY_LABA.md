# Deploy Telegram MCP to laba — clean checklist (session-per-endpoint safe)

RCA 2026-05-28 «токен Лизы опять протух» → корень: одна StringSession
использовалась с двух IP (local Mac + laba) → Telegram навсегда убил auth key
(`AuthKeyDuplicatedError`). Этот runbook делает деплой **повторяемым** и
**механически защищённым** от рецидива. Основан на Standard 5.18 (Rick.ai Laba
Deployment) + session-per-endpoint guard.

## Главный инвариант (запомнить одно)

**Один Telegram-аккаунт может иметь N параллельных долгоживущих сессий — но
каждая сессия (auth key) обязана жить на ОДНОМ endpoint (одном IP).**

- ❌ Скопировать `lisa_tg_session` из Keychain в `.env.laba` → одна строка на 2 IP → смерть ключа.
- ✅ Сгенерировать ОТДЕЛЬНУЮ сессию для laba (re-auth на laba) → laba и local держат РАЗНЫЕ строки → обе живут.

## Кто что делает

| Шаг | Кто | Почему |
|---|---|---|
| Re-auth (SMS/QR код) | **owner / Lisa** | код приходит на телефон аккаунта — API для этого нет |
| `just secrets` + deploy на прод-ноду | **owner / Ваня / koodjo** | нужен Bitwarden (аккаунт Рика) + SSH к `jupyter.production.node.rickai.net:30022` |
| Код, гард, тесты, runbook | **agent** | подготовлено в этом репозитории (idkras/telegram-mcp) |

Agent НЕ имеет доступа: `git@git.rick.ai` (publickey denied), Bitwarden CLI,
`just`, прод-нода SSH. Поэтому финальный «поднять сервер + токены на laba» —
шаг команды. Всё остальное готово и протестировано.

## Pre-deploy checklist (на laba-хосте)

1. **Сгенерировать ВЫДЕЛЕННУЮ laba-сессию** (НЕ копировать из Keychain):
   ```bash
   # на laba-хосте, под аккаунтом Лизы — введёшь SMS/Telegram код
   python3 scripts/update_lisa_session.py          # или _via_qr.py
   # → новая StringSession. Положить её ТОЛЬКО в .env.laba, НЕ в Keychain.
   ```
2. **Заполнить `.env.laba`** (НЕ коммитить — в .gitignore):
   ```
   TELEGRAM_API_ID=...
   TELEGRAM_API_HASH=...
   TELEGRAM_SESSION_STRING=<laba-собственная сессия из шага 1>
   SUPABASE_URL=https://supabase.rick.ai
   SUPABASE_API_KEY=...
   TELEGRAM_USER=lisa
   LABA_MODE=true
   ```
3. **Деплой запустит guard автоматически** — `deploy-to-laba.sh` вызывает
   `validate_session_per_endpoint.py` и **откажет (exit 1)**, если laba-сессия
   совпадает с локальной Keychain-сессией. Это и есть защита от рецидива.

## Deploy

```bash
./scripts/deploy-to-laba.sh          # build + start (запустит preflight guard)
./scripts/deploy-to-laba.sh status   # проверить
./scripts/deploy-to-laba.sh logs     # хвост логов
```

Для прод-ноды через `[projects]/laba` (Standard 5.18 §Деплой):
`just secrets decrypt production` → `just release` → deploy via
`DOCKER_HOST=ssh://jupyter.production.node.rickai.net:30022` → `just secrets clear production`.

## Post-deploy / непрерывный контроль

**Канал не должен умирать молча.** Поставить `session_health_monitor.py` в cron/CI:
```bash
# каждые N минут; exit 1 если хоть один профиль мёртв (REVOKED / AUTHKEY_DUPLICATED / ...)
python3 scripts/session_health_monitor.py --profiles lisa --json \
  --alert-cmd 'telegram-send --to <owner_chat>'   # канал alert — owner decision
```
NETWORK = transient (не считается смертью). Любой DEAD-код → немедленный alert →
re-auth по шагу 1 (на том endpoint, где ключ умер).

## Почему рецидив теперь не повторится (3 слоя)

| Слой | Файл | Когда ловит |
|---|---|---|
| deploy-time static guard | `scripts/validate_session_per_endpoint.py` | при подготовке деплоя: laba-строка == local Keychain → abort |
| runtime death detector | `scripts/session_health_monitor.py` | cron/CI: реальная смерть ключа любого профиля → alert |
| diagnosis surface | `session_manager.test_session` | классифицирует причину (REVOKED / AUTHKEY_DUPLICATED / NETWORK / ...) |

Тесты слоёв: `tests/test_session_per_endpoint.py`, `tests/test_session_health_monitor.py`.
