# Telegram MCP Server - Profile Management

## 🎯 **Profile Switching Instructions**

### **Default Profile: IK (ikrasinsky)**
**By default, Telegram MCP uses `ikrasinsky` profile if `TELEGRAM_USER` is not set.**

No configuration needed - just use Telegram MCP commands and they will work with ikrasinsky account.

### **To use LISA profile (explicit request only):**
1. Edit `.cursor/mcp.json`
2. Add or change `"TELEGRAM_USER": "lisa"` in `env` section
3. Restart Cursor to reload MCP configuration

### **To use IK profile (explicit):**
1. Edit `.cursor/mcp.json`  
2. Add or change `"TELEGRAM_USER": "ikrasinsky"` in `env` section
3. Restart Cursor to reload MCP configuration

### **To use Rick Coposlly LinkedinHero profile (explicit request only):**
1. Edit `.cursor/mcp.json`
2. Add or change `"TELEGRAM_USER": "rick-coposlly-linkedinhero"` in `env` section
3. Restart Cursor to reload MCP configuration
4. **First time setup:** Run `heroes_platform/telegram_mcp/scripts/connect_rick_coposlly_linkedinhero.sh` to create session

## 🔑 **Keychain Key Mapping**

### **LISA PROFILE:**
- `lisa_tg_api_key` (service: lisa_tg_api_key, account: lisa)
- `lisa_tg_app_hash` (service: lisa_tg_app_hash, account: lisa)  
- `lisa_tg_session` (service: lisa_tg_session, account: lisa)
- `lisa_tg_phone` (service: lisa_tg_phone, account: lisa)

### **IK PROFILE (ikrasinsky):**
- `ik_tg_api_id` (service: ik_tg_api_id, account: ilyakrasinsky) - ALTERNATIVE FORMAT
- `ik_tg_api_hash` (service: ik_tg_api_hash, account: ilyakrasinsky) - ALTERNATIVE FORMAT
- `ik_tg_session` (service: ik_tg_session, account: ilyakrasinsky) - ALTERNATIVE FORMAT
- `ik_tg_phone` (service: ik_tg_phone, account: ilyakrasinsky) - ALTERNATIVE FORMAT
- `telegram_api_id` (service: telegram_api_id, account: ilyakrasinsky) - DEFAULT FORMAT (fallback)
- `telegram_api_hash` (service: telegram_api_hash, account: ilyakrasinsky) - DEFAULT FORMAT (fallback)
- `telegram_session` (service: telegram_session, account: ilyakrasinsky) - DEFAULT FORMAT (fallback)

### **RICK COPOSLLY LINKEDINHERO PROFILE:**
- `rick_linkedinhero_api_id` (service: rick_linkedinhero_api_id, account: ilyakrasinsky)
- `rick_linkedinhero_api_hash` (service: rick_linkedinhero_api_hash, account: ilyakrasinsky)
- `rick_coposlly_linkedinhero_session` (service: rick_coposlly_linkedinhero_session, account: ilyakrasinsky)

## 🚀 **How to Run MCP in Cursor**

1. **Default Behavior:** Telegram MCP uses `ikrasinsky` profile automatically (no configuration needed)
2. **Switch Profile (if needed):** Set `TELEGRAM_USER` in `.cursor/mcp.json` → `env` section
3. **Restart Cursor:** Reload MCP configuration after changes
4. **Check Logs:** Look for "🔑 Using TELEGRAM_USER: [profile]"
5. **Use Commands:** `/mcp__telegram-mcp__<command>`

### **Example .cursor/mcp.json configuration:**

```json
{
  "mcpServers": {
    "telegram-mcp": {
      "command": "...",
      "args": [...],
      "env": {
        "PYTHONPATH": "...",
        "TELEGRAM_USER": "ikrasinsky"  // Optional: defaults to ikrasinsky if not set
      }
    }
  }
}
```

**Note:** If `TELEGRAM_USER` is not set, it defaults to `ikrasinsky`.

## 🐛 **Debugging**

### **Check Profile Detection:**
```bash
# Check logs for profile detection
grep "🔑 Using TELEGRAM_USER" logs/mcp_errors.log
```

### **Check Keychain Keys:**
```bash
# List all telegram keys in keychain
security dump-keychain | grep -i telegram
```

### **Test Profile Manually:**
```bash
# Test with specific profile
TELEGRAM_USER=lisa python main.py --help
TELEGRAM_USER=ikrasinsky python main.py --help
TELEGRAM_USER=rick-coposlly-linkedinhero python main.py --help
```

## ⚠️ **Common Issues**

1. **"NOT_SET" in logs:** Cursor not passing env variables
2. **"No credentials found":** Keys not in keychain
3. **"Cannot send requests while disconnected":** Client not initialized

## 📝 **Quick Commands**

```bash
# Use default (ikrasinsky) - remove TELEGRAM_USER from env section
# Or don't set it at all

# Switch to LISA profile (explicit request)
# Add to .cursor/mcp.json → telegram-mcp → env: "TELEGRAM_USER": "lisa"

# Switch to IK profile (explicit)
# Add to .cursor/mcp.json → telegram-mcp → env: "TELEGRAM_USER": "ikrasinsky"

# Switch to Rick Coposlly LinkedinHero profile (explicit request)
# Add to .cursor/mcp.json → telegram-mcp → env: "TELEGRAM_USER": "rick-coposlly-linkedinhero"

# Check current profile
grep -A 5 "telegram-mcp" .cursor/mcp.json | grep "TELEGRAM_USER" || echo "Using default: ikrasinsky"
```

## 🔐 **First Time Setup for Rick Coposlly LinkedinHero**

1. **Credentials are already saved in Keychain:**
   - API ID: `39464383`
   - API Hash: `513338e67d88a16c58c6d10974089bc5`
   - Phone: `+1 (505) 389-2752`

2. **Create session:**
   ```bash
   # Option 1: Use shell wrapper (recommended)
   heroes_platform/telegram_mcp/scripts/connect_rick_coposlly_linkedinhero.sh
   
   # Option 2: Direct Python execution
   cd /Users/ilyakrasinsky/workspace/vscode.projects/heroes-rickai-workspace
   .venv/bin/python3 heroes_platform/telegram_mcp/scripts/connect_rick_coposlly_linkedinhero.py
   ```
   - Phone number will be loaded from Keychain automatically
   - Enter verification code from Telegram when prompted
   - Enter 2FA password if enabled
   - Session will be saved to Keychain automatically

3. **Use in Cursor:**
   - Set `TELEGRAM_USER=rick-coposlly-linkedinhero` in `.cursor/mcp.json`
   - Restart Cursor

## 📚 **Codebase References for Session Management**

**📁 Core Modules:**
- `heroes_platform/telegram_mcp/session_manager.py` - универсальный модуль для создания сессий
  - `create_telegram_session(profile)` - создание сессии для любого профиля
  - `test_session(profile)` - проверка валидности сессии
  - `get_profile_credential_names(profile)` - маппинг профиля на credential names

**📁 Session Scripts:**
- `heroes_platform/telegram_mcp/scripts/update_session.py` - скрипт для default профиля (ikrasinsky)
- `heroes_platform/telegram_mcp/scripts/connect_rick_coposlly_linkedinhero.py` - скрипт для rick-coposlly-linkedinhero
- `heroes_platform/telegram_mcp/scripts/connect_rick_coposlly_linkedinhero.sh` - shell wrapper

**📁 Configuration:**
- `heroes_platform/shared/credentials_manager.py` - конфигурация всех credentials
- `heroes_platform/shared/credentials_wrapper.py` - маппинг профилей на credential names

**💡 How to Create Session for New Profile:**
1. Add credential configs to `credentials_manager._setup_default_configs()`
2. Add profile mapping to `credentials_wrapper.get_service_credentials()`
3. Add credential names mapping to `session_manager.get_profile_credential_names()`
4. Create script in `heroes_platform/telegram_mcp/scripts/` using `session_manager.create_telegram_session()`
5. Update this documentation
