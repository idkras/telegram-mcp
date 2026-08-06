> ⚠️ DEPRECATED 2026-07-23: laba (159.223.129.246) мёртв; актуальный деплой — deploy/deploy-sandbox-ik.sh на sandbox-ik. Файл сохранён как исторический RCA.

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
3. **Авторитетная проверка — НА ЛОКАЛЬНОЙ Mac-машине ПЕРЕД отгрузкой `.env.laba`**
   (там живут local-сессии, есть с чем сравнивать):
   ```bash
   # на Mac, где лежит local Keychain
   python3 scripts/validate_session_per_endpoint.py --env-path /path/to/.env.laba
   #   exit 2 = COLLISION (laba-строка == local) → НЕ отгружай, перегенерируй сессию
   #   exit 0 = laba-сессия отлична от всех local → можно отгружать
   ```
   ⚠️ Запуск guard **на самом laba-хосте** (Linux, без macOS Keychain) вернёт
   **INCONCLUSIVE** (сравнивать не с чем — laba и есть «другой endpoint»), НЕ
   ложный «PASS». В preflight `deploy-to-laba.sh` это advisory (exit 0); чтобы CI
   на laba жёстко падал при INCONCLUSIVE — `TG_SESSION_GUARD_STRICT=1`.

## Deploy

```bash
./scripts/deploy-to-laba.sh          # build + start (запустит preflight guard)
./scripts/deploy-to-laba.sh status   # проверить
./scripts/deploy-to-laba.sh logs     # хвост логов
```

Для прод-ноды через `[projects]/laba` (Standard 5.18 §Деплой):
`just secrets decrypt production` → `just release` → deploy via
`DOCKER_HOST=ssh://jupyter.production.node.rickai.net:30022` → `just secrets clear production`.

### ⚠️ B1 — telegram-mcp НЕ в дефолтной deploy-цепочке (RCA 2026-06-01, корень 38-дневного простоя)

**Голый `just release` / `just deploy` НЕ деплоит telegram-mcp.** В `[projects]/laba/justfile`
`base_compose_file` = `compose.yml:compose.common:compose.<env>.yml` — telegram-mcp compose-файлов
там нет, а `.drone.yml` pipeline `deploy production` гоняет именно эти дефолтные `just`-таргеты →
деплоится только `app`. Поэтому контейнер telegram-mcp никогда не поднимался автоматически
(supabase замёрз на 2026-04-24).

**Фикс — один из двух (применяет команда, нужен git.rick.ai push):**

(a) Добавить telegram-mcp в дефолтную цепочку: вписать `:docker/compose.telegram-mcp.yml` в
`base_compose_file` (станет always-on вместе с `app`).

(b) Отдельные таргеты (рекомендуется — изоляция) в `[projects]/laba/justfile`:

```makefile
_tg_compose := "compose.yml:docker/compose.common.yml:docker/compose.telegram-mcp.yml:docker/compose.telegram-mcp." + env + ".yml:docker/compose." + env + ".yml"

release-telegram-mcp:
    COMPOSE_FILE={{_tg_compose}} DOCKER_DEFAULT_PLATFORM=linux/amd64 docker compose build telegram-mcp
    COMPOSE_FILE={{_tg_compose}} docker compose push telegram-mcp

deploy-telegram-mcp:
    COMPOSE_FILE={{_tg_compose}} docker compose pull telegram-mcp
    COMPOSE_FILE={{_tg_compose}} docker compose up --detach telegram-mcp
```
+ шаг в `.drone.yml` pipeline `deploy production`: `just env=production release-telegram-mcp` и `deploy-telegram-mcp`.

После деплоя — проверить непрерывность через supabase:
`SELECT max(message_ts), count(*) FROM rick_messages_tasks.telegram_messages_raw WHERE created_at > now() - interval '10 min'` (должен расти) и `SELECT mode, status FROM ...telegram_ingest_runs ORDER BY id DESC LIMIT 1` (live `listener_boot`/`heartbeat`, не `failed`).

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

Тесты слоёв: `tests/test_session_per_endpoint.py`, `tests/test_session_health_monitor.py` (34 теста, pure-function, без Keychain/сети).

## Остаточные риски (честно, не закрыто этим слоем)

| # | Риск | Почему не закрыто сейчас | Что закроет |
|---|---|---|---|
| D2 | После того как laba владеет сессией аккаунта, **локальной машине ничто механически не мешает** грузить сессию того же аккаунта (если строки случайно совпали ИЛИ local перечитал старую) → снова 2 IP | Нужен write-side lock на local endpoint (local MCP при известном laba-owned аккаунте сам отказывается коннектиться ИЛИ роутит через laba) — это бОльшая архитектурная работа, отдельный bead | local MCP routes «send-as-X» через laba-сервис вместо прямого StringSession; либо local-load guard читает session-ownership manifest |
| — | Уже-мёртвый/дублирующий auth key в Telegram «Active Sessions» не отзывается автоматически | Лечим причину (не плодить дубли), не симптом | ручной terminate в Telegram при re-auth |

Эти два — следующий слой (route-through-laba). Текущие 3 слоя убирают САМ механизм рецидива (copy local→laba) + ловят смерть рантайм.
