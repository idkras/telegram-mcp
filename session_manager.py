#!/usr/bin/env python3
"""
Telegram Session Manager
Универсальный модуль для создания и обновления Telegram сессий для разных профилей.

JTBD: Как разработчик, я хочу создавать Telegram сессии для разных профилей,
чтобы не дублировать код и использовать единый подход.

📚 CODEBASE REFERENCES:
- heroes_platform/heroes_telegram_mcp/scripts/update_session.py - пример для default профиля (ikrasinsky)
- heroes_platform/heroes_telegram_mcp/scripts/connect_rick_coposlly_linkedinhero.py - пример для rick-coposlly-linkedinhero
- heroes_platform/shared/credentials_manager.py - управление credentials (см. комментарии в _setup_default_configs)
- heroes_platform/shared/credentials_wrapper.py - маппинг профилей на credential names (см. get_service_credentials)
- heroes_platform/heroes_telegram_mcp/PROFILE_MANAGEMENT.md - документация по профилям

TDD Documentation Standard v2.5 Compliance:
- Atomic Functions Architecture (≤20 строк на функцию)
- Security First (валидация всех входных данных)
- Modern Python Development (type hints, dataclasses)
"""

import asyncio
import re
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


def _mask_phone(phone: str) -> str:
    """Mask phone number for terminal diagnostics."""
    normalized = (phone or "").strip()
    if len(normalized) <= 4:
        return normalized
    return f"{normalized[:2]}***{normalized[-2:]}"


def _sent_code_type_name(sent_code_type: object) -> str:
    """Return a readable Telegram sent-code type name."""
    return type(sent_code_type).__name__ if sent_code_type is not None else "Unknown"


# ── Profile → credential-name resolution (universal, config-driven) ──
#
# Generalization-first gate (AGENTS.md): добавление НОВОГО клиента обязано быть
# правкой config/Keychain, НЕ правкой Python-кода (Q4 = YES). Поэтому:
#   1. Легаси-профили с исторически непоследовательными именами ключей
#      (lisa: api_key/app_hash; ik: ik_tg_*; default: telegram_*) — в явной
#      _PROFILE_OVERRIDES таблице, чтобы НЕ протухли существующие Keychain-записи.
#   2. Любой НОВЫЙ профиль резолвится по конвенции {slug}_tg_{field} —
#      zero code change. Новый клиент = просто записать ключи в Keychain
#      по конвенции + указать TELEGRAM_USER=<имя>.

_PROFILE_ALIASES: dict[str, str] = {
    "ilyakrasinsky": "ikrasinsky",
    "ik": "ikrasinsky",
}

# Канонический набор полей. None = у профиля нет такого ключа.
_PROFILE_OVERRIDES: dict[str, dict[str, Optional[str]]] = {
    "lisa": {
        "api_id": "lisa_tg_api_key",
        "api_hash": "lisa_tg_app_hash",
        "session": "lisa_tg_session",
        "phone": "lisa_tg_phone",
        "2fa_password": "lisa_tg_2fa_password",
    },
    "ikrasinsky": {
        "api_id": "ik_tg_api_id",
        "api_hash": "ik_tg_api_hash",
        "session": "ik_tg_session",
        "phone": "ik_tg_phone",
    },
    "rick-coposlly-linkedinhero": {
        "api_id": "rick_coposlly_linkedinhero_api_id",
        "api_hash": "rick_coposlly_linkedinhero_api_hash",
        "session": "rick_coposlly_linkedinhero_session",
        "phone": "rick_coposlly_linkedinhero_phone",
    },
    # default / "" → исторические telegram_* ключи (ikrasinsky legacy fallback)
    "default": {
        "api_id": "telegram_api_id",
        "api_hash": "telegram_api_hash",
        "session": "telegram_session",
        "phone": None,
    },
}


def _slugify_profile(profile: str) -> str:
    """Normalize an arbitrary profile/client name to a safe snake_case slug.

    'my-client' → 'my_client'; 'Smokeway Co' → 'smokeway_co';
    'vipavenue.ru' → 'vipavenue_ru'; '  Typhoon--Coffee  ' → 'typhoon_coffee'.
    Любой не-[a-z0-9] символ → '_', схлопывание повторов, обрезка краёв.
    Не-latin (кириллица/emoji) → отбрасывается; если slug пустой — caller
    обязан обработать (см. get_profile_credential_names fail-fast).
    """
    slug = re.sub(r"[^a-z0-9]+", "_", profile.strip().lower())
    return slug.strip("_")


def get_profile_credential_names(profile: str) -> dict[str, Optional[str]]:
    """Get credential names for a Telegram profile (universal, config-driven).

    📚 SEE: heroes_platform/shared/credentials_wrapper.py for profile mapping logic

    ⚠️ Контракт безопасности (security review 2026-06-01): `profile` ОБЯЗАН
    приходить из доверенного источника (operator-config / TELEGRAM_USER env),
    НЕ из сетевого ввода без allowlist — иначе вызов с profile="lisa" вернёт
    реальные ключи Лизы (cross-account session read). Имена в _PROFILE_OVERRIDES
    зарезервированы и не могут переиспользоваться для новых клиентов без миграции.

    Args:
        profile: Profile/client name. Легаси (ikrasinsky/lisa/rick-coposlly-linkedinhero/
            default) резолвятся через _PROFILE_OVERRIDES (backward compatible);
            любой новый клиент — по конвенции {slug}_tg_{field}.

    Returns:
        dict with credential names: api_id, api_hash, session, phone, 2fa_password.
        Возвращается СВЕЖИЙ dict на каждый вызов (мутация не протекает).

    Raises:
        ValueError: если profile после нормализации даёт пустой slug
            (whitespace/symbols/non-latin only) — fail-fast вместо silent
            `_tg_session` namespace-collision (security finding 2026-06-01).
    """
    normalized = (profile or "").strip().lower()
    if not normalized:
        normalized = "default"

    key = _PROFILE_ALIASES.get(normalized, normalized)
    if key in _PROFILE_OVERRIDES:
        return dict(_PROFILE_OVERRIDES[key])

    # Универсальная конвенция — НОВЫЙ клиент без правки кода.
    slug = _slugify_profile(profile)
    if not slug:
        raise ValueError(
            f"Cannot derive a safe Keychain slug from profile {profile!r} "
            "(empty after normalization — whitespace/symbols/non-latin only). "
            "Use a latin-alphanumeric profile name or add an explicit "
            "_PROFILE_OVERRIDES entry."
        )
    return {
        "api_id": f"{slug}_tg_api_id",
        "api_hash": f"{slug}_tg_api_hash",
        "session": f"{slug}_tg_session",
        "phone": f"{slug}_tg_phone",
        "2fa_password": f"{slug}_tg_2fa_password",
    }


async def create_telegram_session(
    profile: str,
    phone: Optional[str] = None,
    code: Optional[str] = None,
    password: Optional[str] = None,
) -> tuple[bool, Optional[str], Optional[str]]:
    """Create or update Telegram session for a specific profile.

    📚 SEE: heroes_platform/heroes_telegram_mcp/scripts/update_session.py for reference implementation

    Args:
        profile: Profile name (ikrasinsky, lisa, rick-coposlly-linkedinhero)
        phone: Phone number (optional, will be loaded from keychain if not provided)
        code: Verification code (optional, will be requested if not provided)
        password: 2FA password (optional, will be requested if needed)

    Returns:
        tuple: (success: bool, session_string: Optional[str], error_message: Optional[str])
    """
    credential_names = get_profile_credential_names(profile)
    print(
        "INFO: Using Telegram profile "
        f"{profile} with credential keys "
        f"api_id={credential_names['api_id']}, "
        f"api_hash={credential_names['api_hash']}, "
        f"session={credential_names['session']}, "
        f"phone={credential_names.get('phone') or 'prompt/manual'}"
    )

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
            await client.get_me()
            return True, session_string, None

        # Need authorization
        if not phone:
            phone_result = credentials_manager.get_credential(credential_names.get("phone") or "")
            if phone_result.success and phone_result.value:
                phone = phone_result.value
                print(
                    f"INFO: Loaded phone from credential {credential_names.get('phone')}: {_mask_phone(phone)}"
                )
            else:
                phone = input("Enter your phone number (with country code, e.g., +1234567890): ")

        sent_code = await client.send_code_request(phone)
        sent_type = _sent_code_type_name(getattr(sent_code, "type", None))
        next_type = _sent_code_type_name(getattr(sent_code, "next_type", None))
        timeout = getattr(sent_code, "timeout", None)
        print(f"INFO: Telegram accepted code request for {_mask_phone(phone)}")
        print(f"INFO: Delivery type: {sent_type}")
        if next_type != "Unknown":
            print(f"INFO: Next delivery type: {next_type}")
        if timeout is not None:
            print(f"INFO: Telegram timeout before another resend: {timeout}s")
            print(
                "INFO: If no fresh message arrives, Telegram may still expect the previous unexpired code."
            )

        if not code:
            code = input("Enter the verification code: ")

        try:
            await client.sign_in(phone, code)
        except telethon.errors.rpcerrorlist.SessionPasswordNeededError:
            if not password:
                password = input("Enter your 2FA password: ")

            try:
                await client.sign_in(password=password)
            except telethon.errors.rpcerrorlist.PasswordHashInvalidError:
                return False, None, "Invalid 2FA password"
            except Exception as e:
                return False, None, f"2FA authorization failed: {e}"

        session_string = client.session.save()

        me = await client.get_me()
        _user_info = f"{me.first_name} {me.last_name or ''} (@{me.username or 'no username'})"

        success = credentials_manager.store_credential(
            credential_names["session"], session_string, "keychain"
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

    Surfaces the *real* failure reason instead of silently returning False.
    Без этого протухание сессии всегда выглядело как абстрактное "expired",
    и нельзя было отличить главную причину рецидива (AuthKeyDuplicated —
    одна сессия с двух IP) от ручного logout или сетевой ошибки.
    RCA 2026-05-28: lisa_tg_session keeps getting revoked.

    Args:
        profile: Profile name

    Returns:
        tuple: (is_valid: bool, info_or_diagnosis: Optional[str])
            success → user_info; failure → diagnosis prefixed with reason code,
            one of: NO_SESSION / REVOKED / AUTHKEY_DUPLICATED / NETWORK / UNKNOWN.
    """
    credential_names = get_profile_credential_names(profile)

    # Get credentials
    api_id_result = credentials_manager.get_credential(credential_names["api_id"])
    api_hash_result = credentials_manager.get_credential(credential_names["api_hash"])
    session_result = credentials_manager.get_credential(credential_names["session"])

    if not api_id_result.success or not api_hash_result.success:
        return False, "NO_SESSION: api_id/api_hash credentials missing in Keychain"

    if not session_result.success or not session_result.value:
        return False, "NO_SESSION: session string missing in Keychain"

    api_id = int(api_id_result.value) if api_id_result.value else 0
    api_hash = api_hash_result.value
    session_string = session_result.value

    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    try:
        await client.connect()

        if await client.is_user_authorized():
            me = await client.get_me()
            user_info = f"{me.first_name} {me.last_name or ''} (@{me.username or 'no username'})"
            return True, user_info

        # connect() ok, but server says not authorized → key revoked server-side
        # (manual logout in Active Sessions, or after-effect of AuthKeyDuplicated).
        return False, (
            "REVOKED: connect ok but Telegram returns is_user_authorized=False — "
            "auth key revoked server-side. Re-auth required (see update_*_session.py)."
        )
    except telethon.errors.rpcerrorlist.AuthKeyDuplicatedError as e:
        return False, (
            "AUTHKEY_DUPLICATED: the same session string was used from two IPs "
            "simultaneously (e.g. local + laba container) and is permanently dead. "
            "Fix: give each endpoint its OWN session string. "
            f"Telethon: {e}"
        )
    except telethon.errors.rpcerrorlist.AuthKeyUnregisteredError as e:
        return False, f"REVOKED: auth key unregistered (logged out). Telethon: {e}"
    except (ConnectionError, OSError, asyncio.TimeoutError) as e:
        return False, f"NETWORK: cannot reach Telegram (transient, retry). {type(e).__name__}: {e}"
    except Exception as e:
        return False, f"UNKNOWN: {type(e).__name__}: {e}"
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
