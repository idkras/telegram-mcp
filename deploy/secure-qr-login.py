#!/usr/bin/env python3
"""Interactive Telegram QR login that stores the session only as an encrypted credential.

Contract:
- root only; stdin and stdout must both be a TTY;
- the QR is rendered as ASCII on the terminal only and never written to a file;
- the 2FA password is read with getpass and never stored;
- the profile unit must not be running (protects against AuthKeyDuplicatedError);
- the new session exists only in memory and reaches install-encrypted-credential.py
  through its stdin; argv, files and output never carry secret values;
- the listener unit is NOT restarted here.
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import hmac
import os
import re
import secrets
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from encrypted_credential import PayloadError, parse_dotenv, serialize_dotenv  # noqa: E402

PROFILE_RE = re.compile(r"^[a-z0-9_]+$")
ALLOWED_PROFILES = frozenset({"ikrasinsky", "lisa"})
SELFTEST_PROFILE = "selftest"
CREDENTIAL_NAME = "telegram_env"
CREDSTORE = Path("/etc/credstore.encrypted")
LEGACY_ENV_DIR = Path("/etc/telegram-mcp/env.d")
INSTALLER = HERE / "install-encrypted-credential.py"
CARRIED_KEYS = (
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "SUPABASE_DB_URL",
    "SUPABASE_API_KEY",
    "SUPABASE_SERVICE_ROLE_KEY",
)
REQUIRED_CARRIED_KEYS = CARRIED_KEYS[:3]
PAYLOAD_ORDER = (
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_SESSION_STRING",
    "SUPABASE_DB_URL",
    "SUPABASE_API_KEY",
    "SUPABASE_SERVICE_ROLE_KEY",
)
SAFE_UNIT_STATES = frozenset({"inactive", "failed"})
QR_ATTEMPTS = 10
PASSWORD_ATTEMPTS = 3
CLEAR_SCREEN = "\x1b[2J\x1b[3J\x1b[H"


class Refused(RuntimeError):
    """Value-blind refusal."""


def credential_path(profile: str) -> Path:
    return CREDSTORE / f"telegram-mcp-{profile}.env.cred"


def check_preconditions(profile: str, self_test: bool, *, geteuid=os.geteuid, isatty=os.isatty) -> None:
    if geteuid() != 0:
        raise Refused("must run as root")
    if not (isatty(0) and isatty(1)):
        raise Refused("interactive TTY required on stdin and stdout")
    if not PROFILE_RE.fullmatch(profile):
        raise Refused("profile must match [a-z0-9_]+")
    if profile not in ({SELFTEST_PROFILE} if self_test else ALLOWED_PROFILES):
        raise Refused(f"profile not allowed: {profile}")


def unit_state(profile: str) -> str:
    result = subprocess.run(
        ["systemctl", "is-active", f"telegram-mcp-{profile}.service"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=False,
    )
    return result.stdout.strip() or "unknown"


def ensure_unit_stopped(profile: str) -> None:
    state = unit_state(profile)
    if state not in SAFE_UNIT_STATES:
        raise Refused(f"telegram-mcp-{profile}.service is {state}; stop it before login")


def decrypt_credential(path: Path) -> bytes:
    result = subprocess.run(
        ["systemd-creds", "decrypt", f"--name={CREDENTIAL_NAME}", str(path), "-"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False,
    )
    if result.returncode != 0:
        raise Refused(f"cannot decrypt {path}")
    return result.stdout


def load_carried_values(profile: str) -> tuple[dict[str, str], str]:
    encrypted = credential_path(profile)
    legacy = LEGACY_ENV_DIR / f"{profile}.env"
    if encrypted.is_file():
        raw, source = decrypt_credential(encrypted), f"encrypted:{encrypted}"
    elif legacy.is_file():
        raw, source = legacy.read_bytes(), f"legacy:{legacy}"
    else:
        raise Refused("neither encrypted nor legacy credential source exists")
    try:
        values = parse_dotenv(raw)
    except PayloadError as exc:
        raise Refused(f"{source}: {exc}") from exc
    carried = {key: values[key] for key in CARRIED_KEYS if values.get(key)}
    missing = [key for key in REQUIRED_CARRIED_KEYS if key not in carried]
    if missing:
        raise Refused(f"{source}: missing keys {','.join(missing)}")
    if not carried["TELEGRAM_API_ID"].isdigit():
        raise Refused(f"{source}: TELEGRAM_API_ID is not an integer")
    return carried, source


def render_qr(url: str, out) -> None:
    import qrcode

    code = qrcode.QRCode(border=2)
    code.add_data(url)
    code.make(fit=True)
    code.print_ascii(out=out, tty=True)  # raises OSError when out is not a TTY
    out.flush()


async def qr_login(api_id: int, api_hash: str, *, client_factory=None, render=render_qr,
                   ask_password=getpass.getpass, out=None) -> tuple[str, object]:
    from telethon.errors import PasswordHashInvalidError, SessionPasswordNeededError

    out = out or sys.stdout
    if client_factory is None:
        from telethon import TelegramClient
        from telethon.sessions import StringSession

        def client_factory():
            return TelegramClient(StringSession(), api_id, api_hash)

    client = client_factory()
    await client.connect()
    try:
        qr = await client.qr_login()
        user = None
        for _ in range(QR_ATTEMPTS):
            out.write(CLEAR_SCREEN + "Telegram -> Settings -> Devices -> Link Desktop Device; "
                      f"QR expires {qr.expires:%H:%M:%S} UTC\n")
            render(qr.url, out)
            try:
                user = await qr.wait()
                break
            except asyncio.TimeoutError:
                await qr.recreate()
            except SessionPasswordNeededError:
                out.write(CLEAR_SCREEN)
                out.flush()
                loop = asyncio.get_running_loop()
                for _ in range(PASSWORD_ATTEMPTS):
                    try:
                        user = await client.sign_in(password=await loop.run_in_executor(
                            None, ask_password, "Telegram 2FA password (hidden): "))
                        break
                    except PasswordHashInvalidError:
                        out.write("2FA password rejected\n")
                break
        out.write(CLEAR_SCREEN)
        out.flush()
        if user is None:
            raise Refused("QR login did not complete")
        session = client.session.save()
        if not session:
            raise Refused("Telegram returned an empty session")
        return session, user
    finally:
        await client.disconnect()


def build_payload(carried: dict[str, str], session: str) -> bytes:
    return serialize_dotenv({**carried, "TELEGRAM_SESSION_STRING": session}, PAYLOAD_ORDER)


def install_payload(profile: str, payload: bytes) -> int:
    result = subprocess.run(
        [sys.executable, "-B", "-E", "-s", str(INSTALLER), "--profile", profile],
        input=payload, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    for stream, text in ((sys.stdout, result.stdout), (sys.stderr, result.stderr)):
        if text:
            stream.write("installer: " + text.decode("utf-8", "replace"))
    return result.returncode


def run_self_test() -> None:
    cred = credential_path(SELFTEST_PROFILE)
    rollback = cred.with_name(cred.name + ".rollback")
    if cred.exists() or rollback.exists():
        raise Refused(f"{cred} already exists; inspect before self-test")
    marker = "selftest-" + secrets.token_hex(16)  # fictitious session, safe to print
    print(f"SELFTEST_MARKER={marker}")
    render_qr("tg://login?token=selftest-not-a-real-token", sys.stdout)
    print("qr_render=tty-ascii-ok qr_files_written=0")
    carried = {"TELEGRAM_API_ID": "1", "TELEGRAM_API_HASH": "selftest-hash",
               "SUPABASE_DB_URL": "postgresql://selftest.invalid/selftest"}
    try:
        if install_payload(SELFTEST_PROFILE, build_payload(carried, marker)) != 0:
            raise Refused("installer failed")
        blob_has_marker = marker.encode() in cred.read_bytes()
        session = parse_dotenv(decrypt_credential(cred)).get("TELEGRAM_SESSION_STRING", "")
        match = hmac.compare_digest(session.encode(), marker.encode())
        wrong_name = subprocess.run(
            ["systemd-creds", "decrypt", "--name=wrong_name", str(cred), "-"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
        )
        print(f"positive_control=decrypt_readback_{'match' if match else 'MISMATCH'}")
        print(f"negative_control=wrong_name_decrypt_{'refused' if wrong_name.returncode else 'ACCEPTED'}")
        print(f"ciphertext_contains_marker={'YES' if blob_has_marker else 'no'}")
        if not match or wrong_name.returncode == 0 or blob_has_marker:
            raise Refused("self-test FAILED")
    finally:
        for path in (cred, rollback):
            if path.exists():
                path.unlink()
        print(f"cleanup={cred} exists_after={cred.exists()}")
    print("self_test=PASS")


def main(argv: list[str] | None = None, *, geteuid=os.geteuid, isatty=os.isatty) -> int:
    parser = argparse.ArgumentParser(description="Telegram QR login -> encrypted systemd credential")
    parser.add_argument("profile")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    try:
        check_preconditions(args.profile, args.self_test, geteuid=geteuid, isatty=isatty)
        ensure_unit_stopped(args.profile)
        if args.self_test:
            run_self_test()
            return 0
        carried, source = load_carried_values(args.profile)
        print(f"profile={args.profile} inputs={source} carried_keys={','.join(carried)}")
        session, user = asyncio.run(qr_login(int(carried["TELEGRAM_API_ID"]), carried["TELEGRAM_API_HASH"]))
        print(f"telegram_login=ok user_id={getattr(user, 'id', None)} "
              f"username={getattr(user, 'username', None) or '-'}")
        ensure_unit_stopped(args.profile)
        rc = install_payload(args.profile, build_payload(carried, session))
        del session
        print(f"encrypted_credential={credential_path(args.profile)} installer_rc={rc} unit_restart=not-performed")
        return rc
    except Refused as exc:
        print(f"secure-qr-login refused: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("secure-qr-login aborted", file=sys.stderr)
        return 130
    except Exception as exc:  # value-blind: never echo messages that might carry data
        print(f"secure-qr-login failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
