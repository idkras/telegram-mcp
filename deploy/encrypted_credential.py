"""Value-blind parsing shared by encrypted credential install/runtime tools."""

from __future__ import annotations

import json
import re


KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
MAX_PAYLOAD_BYTES = 1024 * 1024


class PayloadError(ValueError):
    """A metadata-only dotenv validation error."""


def parse_dotenv(raw: bytes) -> dict[str, str]:
    """Parse the intentionally small secret-payload dotenv subset.

    Accepted values are raw single-line strings, JSON-style double-quoted
    strings, or literal single-quoted strings.  Shell expansion, interpolation,
    command substitution, multiline syntax, and inline comments are never
    evaluated.  That keeps both migration and runtime independent of a shell.
    """

    if not raw or len(raw) > MAX_PAYLOAD_BYTES:
        raise PayloadError("secret payload is empty or exceeds the size limit")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PayloadError("secret payload must be UTF-8") from exc

    values: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[7:].lstrip()
        if "=" not in stripped:
            raise PayloadError("secret payload contains a non-assignment line")
        key, encoded = stripped.split("=", 1)
        key = key.strip()
        encoded = encoded.strip()
        if not KEY_RE.fullmatch(key):
            raise PayloadError("secret payload contains an invalid key name")
        if key in values:
            raise PayloadError("duplicate secret keys: " + key)

        if encoded.startswith('"'):
            try:
                value = json.loads(encoded)
            except (TypeError, json.JSONDecodeError) as exc:
                raise PayloadError(f"invalid quoted value for key {key}") from exc
            if not isinstance(value, str):
                raise PayloadError(f"non-string value for key {key}")
        elif encoded.startswith("'"):
            if len(encoded) < 2 or not encoded.endswith("'"):
                raise PayloadError(f"invalid quoted value for key {key}")
            value = encoded[1:-1]
        else:
            value = encoded
        if "\x00" in value:
            raise PayloadError(f"NUL byte in key {key}")
        values[key] = value
    return values


def serialize_dotenv(values: dict[str, str], ordered_keys: tuple[str, ...]) -> bytes:
    """Serialize deterministically without exposing values to a shell."""

    return "".join(
        f"{key}={json.dumps(values[key], ensure_ascii=True)}\n"
        for key in ordered_keys
        if values.get(key)
    ).encode("utf-8")


# ---------------------------------------------------------------------------
# Profile-scoped secret key sets.  The single source of truth for the installer,
# the runtime loader and the QR login.  Only key NAMES live here.
# ---------------------------------------------------------------------------

BASE_REQUIRED_SECRET_KEYS = (
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_SESSION_STRING",
    "SUPABASE_DB_URL",
)
SUPABASE_OPTIONAL_SECRET_KEYS = (
    # Registry id supabase_rick_api_key resolves SUPABASE_RICK_API_KEY first.
    "SUPABASE_RICK_API_KEY",
    "SUPABASE_API_KEY",
    "SUPABASE_SERVICE_ROLE_KEY",
)
# Registry ids lisa_tg_api_key / lisa_tg_app_hash / lisa_tg_session read these
# names first (the canonical registry reads ONLY these), so they are required.
LISA_SECRET_KEYS = ("LISA_TG_API_KEY", "LISA_TG_APP_HASH", "LISA_TG_SESSION")
# Accepted only as migration input and dropped from the encrypted payload; they
# are non-secret unit settings (Environment=) owned by the systemd unit.
LEGACY_NONSECRET_KEYS = frozenset(("TELEGRAM_USER", "LABA_MODE", "SUPABASE_URL"))
# Names that are aliases of one logical credential: when several are present
# their values must be identical, otherwise the runtime would silently pick one.
EQUAL_ALIAS_GROUPS = (
    ("LISA_TG_API_KEY", "TELEGRAM_API_ID"),
    ("LISA_TG_APP_HASH", "TELEGRAM_API_HASH"),
    ("LISA_TG_SESSION", "TELEGRAM_SESSION_STRING"),
    ("SUPABASE_RICK_API_KEY", "SUPABASE_API_KEY"),
)


class ProfileKeySet:
    """Allowed secret key names for one Telegram profile."""

    def __init__(self, required: tuple[str, ...], optional: tuple[str, ...],
                 session_keys: tuple[str, ...]) -> None:
        self.required = required
        self.optional = optional
        self.session_keys = session_keys
        self.ordered = required + optional
        self.allowed = frozenset(self.ordered)


PROFILE_KEY_SETS = {
    "ikrasinsky": ProfileKeySet(
        BASE_REQUIRED_SECRET_KEYS, SUPABASE_OPTIONAL_SECRET_KEYS, ("TELEGRAM_SESSION_STRING",)
    ),
    "lisa": ProfileKeySet(
        BASE_REQUIRED_SECRET_KEYS + LISA_SECRET_KEYS,
        SUPABASE_OPTIONAL_SECRET_KEYS,
        # LISA_TG_SESSION is the effective key; TELEGRAM_SESSION_STRING is kept equal.
        ("LISA_TG_SESSION", "TELEGRAM_SESSION_STRING"),
    ),
    # Fictitious profile used only by the root self-test of the QR login.
    "selftest": ProfileKeySet(
        BASE_REQUIRED_SECRET_KEYS, SUPABASE_OPTIONAL_SECRET_KEYS, ("TELEGRAM_SESSION_STRING",)
    ),
}
RUNTIME_PROFILES = frozenset(("ikrasinsky", "lisa"))


def profile_key_set(profile: str) -> ProfileKeySet:
    keyset = PROFILE_KEY_SETS.get(profile)
    if keyset is None:
        raise PayloadError(f"profile has no declared secret key set: {profile!r}")
    return keyset


def validate_profile_secrets(
    values: dict[str, str],
    profile: str,
    *,
    allow_legacy_nonsecret: bool,
    exempt_required: tuple[str, ...] = (),
) -> dict[str, str]:
    """Return the non-empty secret subset for ``profile``; refuse anything else.

    Error messages carry key names only, never values.
    """

    keyset = profile_key_set(profile)
    ignored = LEGACY_NONSECRET_KEYS if allow_legacy_nonsecret else frozenset()
    unknown = sorted(set(values) - keyset.allowed - ignored)
    if unknown:
        raise PayloadError(f"unknown secret keys for profile {profile}: " + ",".join(unknown))
    missing = [
        key for key in keyset.required
        if key not in exempt_required and not values.get(key)
    ]
    if missing:
        raise PayloadError(
            f"missing required secret keys for profile {profile}: " + ",".join(missing)
        )
    for group in EQUAL_ALIAS_GROUPS:
        present = [key for key in group if values.get(key)]
        if len(present) > 1 and len({values[key] for key in present}) > 1:
            raise PayloadError("alias keys carry different values: " + ",".join(present))
    return {key: values[key] for key in keyset.ordered if values.get(key)}
