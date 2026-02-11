#!/usr/bin/env python3
"""
Telegram Session Manager
Универсальный модуль для создания и обновления Telegram сессий для разных профилей.

JTBD: Как разработчик, я хочу создавать Telegram сессии для разных профилей,
чтобы не дублировать код и использовать единый подход.

📚 CODEBASE REFERENCES:
- heroes_platform/telegram_mcp/scripts/update_session.py - пример для default профиля (ikrasinsky)
- heroes_platform/telegram_mcp/scripts/connect_rick_coposlly_linkedinhero.py - пример для rick-coposlly-linkedinhero
- heroes_platform/shared/credentials_manager.py - управление credentials (см. комментарии в _setup_default_configs)
- heroes_platform/shared/credentials_wrapper.py - маппинг профилей на credential names (см. get_service_credentials)
- heroes_platform/telegram_mcp/PROFILE_MANAGEMENT.md - документация по профилям

TDD Documentation Standard v2.5 Compliance:
- Atomic Functions Architecture (≤20 строк на функцию)
- Security First (валидация всех входных данных)
- Modern Python Development (type hints, dataclasses)
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional

# Add the heroes_platform directory to Python path
heroes_platform_path = Path(__file__).parent.parent.parent
if str(heroes_platform_path) not in sys.path:
    sys.path.insert(0, str(heroes_platform_path))

from heroes_platform.shared.credentials_manager import credentials_manager  # type: ignore
from telethon import TelegramClient  # type: ignore
from telethon.sessions import StringSession  # type: ignore
import telethon.errors.rpcerrorlist  # type: ignore


def get_profile_credential_names(profile: str) -> dict[str, str]:
    """Get credential names for a specific Telegram profile.
    
    📚 SEE: heroes_platform/shared/credentials_wrapper.py for profile mapping logic
    
    Args:
        profile: Profile name (ikrasinsky, lisa, rick-coposlly-linkedinhero)
        
    Returns:
        dict with credential names: api_id, api_hash, session, phone
    """
    profile_lower = profile.lower()
    
    if profile_lower == "rick-coposlly-linkedinhero":
        return {
            "api_id": "rick_coposlly_linkedinhero_api_id",
            "api_hash": "rick_coposlly_linkedinhero_api_hash",
            "session": "rick_coposlly_linkedinhero_session",
            "phone": "rick_coposlly_linkedinhero_phone",
        }
    elif profile_lower == "lisa":
        return {
            "api_id": "lisa_tg_api_key",
            "api_hash": "lisa_tg_app_hash",
            "session": "lisa_tg_session",
            "phone": "lisa_tg_phone",
        }
    elif profile_lower in ["ikrasinsky", "ilyakrasinsky", "ik"]:
        # Try new format first, fallback to default
        return {
            "api_id": "ik_tg_api_id",  # Fallback to telegram_api_id in wrapper
            "api_hash": "ik_tg_api_hash",  # Fallback to telegram_api_hash in wrapper
            "session": "ik_tg_session",  # Fallback to telegram_session in wrapper
            "phone": "ik_tg_phone",
        }
    else:
        # Default profile (ikrasinsky)
        return {
            "api_id": "telegram_api_id",
            "api_hash": "telegram_api_hash",
            "session": "telegram_session",
            "phone": None,  # No phone credential for default
        }


async def create_telegram_session(
    profile: str,
    phone: Optional[str] = None,
    code: Optional[str] = None,
    password: Optional[str] = None,
) -> tuple[bool, Optional[str], Optional[str]]:
    """Create or update Telegram session for a specific profile.
    
    📚 SEE: heroes_platform/telegram_mcp/scripts/update_session.py for reference implementation
    
    Args:
        profile: Profile name (ikrasinsky, lisa, rick-coposlly-linkedinhero)
        phone: Phone number (optional, will be loaded from keychain if not provided)
        code: Verification code (optional, will be requested if not provided)
        password: 2FA password (optional, will be requested if needed)
        
    Returns:
        tuple: (success: bool, session_string: Optional[str], error_message: Optional[str])
    """
    credential_names = get_profile_credential_names(profile)
    
    # Get API credentials
    api_id_result = credentials_manager.get_credential(credential_names["api_id"])
    api_hash_result = credentials_manager.get_credential(credential_names["api_hash"])
    
    if not api_id_result.success or not api_hash_result.success:
        error_msg = f"Failed to get API credentials for profile {profile}"
        if not api_id_result.success:
            error_msg += f": API ID - {api_id_result.error}"
        if not api_hash_result.success:
            error_msg += f": API Hash - {api_hash_result.error}"
        return False, None, error_msg
    
    api_id = int(api_id_result.value) if api_id_result.value else 0
    api_hash = api_hash_result.value
    
    # Create session
    session = StringSession()
    client = TelegramClient(session, api_id, api_hash)
    
    try:
        await client.connect()
        
        if await client.is_user_authorized():
            # Already authorized, just get session
            session_string = client.session.save()
            me = await client.get_me()
            return True, session_string, None
        
        # Need authorization
        # Get phone
        if not phone:
            phone_result = credentials_manager.get_credential(credential_names.get("phone") or "")
            if phone_result.success and phone_result.value:
                phone = phone_result.value
            else:
                phone = input("Enter your phone number (with country code, e.g., +1234567890): ")
        
        # Send code request
        await client.send_code_request(phone)
        
        # Get verification code
        if not code:
            code = input("Enter the verification code: ")
        
        # Sign in with code
        try:
            await client.sign_in(phone, code)
        except telethon.errors.rpcerrorlist.SessionPasswordNeededError:
            # 2FA is enabled
            if not password:
                password = input("Enter your 2FA password: ")
            
            try:
                await client.sign_in(password=password)
            except telethon.errors.rpcerrorlist.PasswordHashInvalidError:
                return False, None, "Invalid 2FA password"
            except Exception as e:
                return False, None, f"2FA authorization failed: {e}"
        
        # Get session string
        session_string = client.session.save()
        
        # Get user info
        me = await client.get_me()
        user_info = f"{me.first_name} {me.last_name or ''} (@{me.username or 'no username'})"
        
        # Save to keychain
        success = credentials_manager.store_credential(
            credential_names["session"],
            session_string,
            "keychain"
        )
        
        if success:
            return True, session_string, None
        else:
            return False, None, "Failed to save session to keychain"
    
    except Exception as e:
        return False, None, f"Failed to create session: {e}"
    finally:
        await client.disconnect()


async def test_session(profile: str) -> tuple[bool, Optional[str]]:
    """Test if session exists and is valid for a profile.
    
    Args:
        profile: Profile name
        
    Returns:
        tuple: (is_valid: bool, user_info: Optional[str])
    """
    credential_names = get_profile_credential_names(profile)
    
    # Get credentials
    api_id_result = credentials_manager.get_credential(credential_names["api_id"])
    api_hash_result = credentials_manager.get_credential(credential_names["api_hash"])
    session_result = credentials_manager.get_credential(credential_names["session"])
    
    if not api_id_result.success or not api_hash_result.success:
        return False, None
    
    if not session_result.success or not session_result.value:
        return False, None
    
    api_id = int(api_id_result.value) if api_id_result.value else 0
    api_hash = api_hash_result.value
    session_string = session_result.value
    
    # Test session
    try:
        session = StringSession(session_string)
        client = TelegramClient(session, api_id, api_hash)
        await client.connect()
        
        if await client.is_user_authorized():
            me = await client.get_me()
            user_info = f"{me.first_name} {me.last_name or ''} (@{me.username or 'no username'})"
            await client.disconnect()
            return True, user_info
        else:
            await client.disconnect()
            return False, None
    except Exception:
        return False, None
