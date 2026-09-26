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
