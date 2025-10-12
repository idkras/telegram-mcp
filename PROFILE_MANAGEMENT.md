# Telegram MCP Server - Profile Management

## 🎯 **Profile Switching Instructions**

### **To use LISA profile:**
1. Edit `.cursor/mcp.json`
2. Change `"TELEGRAM_USER": "lisa"`
3. Restart Cursor to reload MCP configuration

### **To use IK profile:**
1. Edit `.cursor/mcp.json`  
2. Change `"TELEGRAM_USER": "ikrasinsky"`
3. Restart Cursor to reload MCP configuration

## 🔑 **Keychain Key Mapping**

### **LISA PROFILE:**
- `lisa_tg_api_key` (service: lisa_tg_api_key, account: lisa)
- `lisa_tg_app_hash` (service: lisa_tg_app_hash, account: lisa)  
- `lisa_tg_session` (service: lisa_tg_session, account: lisa)
- `lisa_tg_phone` (service: lisa_tg_phone, account: lisa)

### **IK PROFILE (ikrasinsky):**
- `ik_tg_api_id` (service: ik_tg_api_id, account: ilyakrasinsky) - NEW FORMAT
- `ik_tg_api_hash` (service: ik_tg_api_hash, account: ilyakrasinsky) - NEW FORMAT
- `ik_tg_session` (service: ik_tg_session, account: ilyakrasinsky) - NEW FORMAT
- `ik_tg_phone` (service: ik_tg_phone, account: ilyakrasinsky) - NEW FORMAT
- `telegram_api_id` (service: telegram_api_id, account: ilyakrasinsky) - LEGACY FORMAT
- `telegram_api_hash` (service: telegram_api_hash, account: ilyakrasinsky) - LEGACY FORMAT
- `telegram_session` (service: telegram_session, account: ilyakrasinsky) - LEGACY FORMAT

## 🚀 **How to Run MCP in Cursor**

1. **Configure Profile:** Set `TELEGRAM_USER` in `.cursor/mcp.json`
2. **Restart Cursor:** Reload MCP configuration
3. **Check Logs:** Look for "🔑 Using TELEGRAM_USER: [profile]"
4. **Use Commands:** `/mcp__telegram-mcp__<command>`

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
```

## ⚠️ **Common Issues**

1. **"NOT_SET" in logs:** Cursor not passing env variables
2. **"No credentials found":** Keys not in keychain
3. **"Cannot send requests while disconnected":** Client not initialized

## 📝 **Quick Commands**

```bash
# Switch to LISA profile
sed -i 's/"TELEGRAM_USER": "ikrasinsky"/"TELEGRAM_USER": "lisa"/' .cursor/mcp.json

# Switch to IK profile  
sed -i 's/"TELEGRAM_USER": "lisa"/"TELEGRAM_USER": "ikrasinsky"/' .cursor/mcp.json

# Check current profile
grep "TELEGRAM_USER" .cursor/mcp.json
```
