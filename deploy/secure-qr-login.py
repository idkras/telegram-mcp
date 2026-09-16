#!/usr/bin/env python3
"""Interactive Telegram QR login that stores the session only as an encrypted credential.

Contract:
- root only; runs from a root-owned venv with ``python -I -B`` and a scrubbed
  environment (see deploy/telegram-secure-qr-login); sys.path never reaches /home;
- login and self-test: stdin and stdout must both be a TTY;
- the QR is rendered as ASCII on the terminal only and never written to a file;
- the 2FA password is read with getpass and never stored;
- the profile unit must not be running during login (AuthKeyDuplicatedError);
- the secret key set is profile-scoped (encrypted_credential.PROFILE_KEY_SETS);
  unknown keys refuse, legacy non-secret unit settings are dropped;
- the new session exists only in memory and reaches install-encrypted-credential.py
  through its stdin; it is installed BEFORE the client disconnects, the install
  is retried once, and a double failure is reported (optional log_out by flag);
- ``--migrate-only`` moves the legacy env into the encrypted credential without
  Telegram; ``--dry-inputs`` prints key NAMES only; nothing prints secret values;
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
import stat
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:  # python -I does not add the script directory
    sys.path.insert(0, str(HERE))

from encrypted_credential import (  # noqa: E402
    LEGACY_NONSECRET_KEYS,
    PayloadError,
    parse_dotenv,
    profile_key_set,
    serialize_dotenv,
    validate_profile_secrets,
)

PROFILE_RE = re.compile(r"^[a-z0-9_]+$")
ALLOWED_PROFILES = frozenset({"ikrasinsky", "lisa"})
SELFTEST_PROFILE = "selftest"
CREDENTIAL_NAME = "telegram_env"
CREDSTORE = Path("/etc/credstore.encrypted")
LEGACY_ENV_DIR = Path("/etc/telegram-mcp/env.d")
INSTALLER = HERE / "install-encrypted-credential.py"
# The installer child uses the same isolated interpreter flags as the wrapper.
CHILD_PYTHON_FLAGS = ("-I", "-B")
UNTRUSTED_PATH_PREFIXES = ("/home/", "/tmp/", "/var/tmp/", "/dev/shm/", "/run/user/")
SAFE_UNIT_STATES = frozenset({"inactive", "failed"})
QR_ATTEMPTS = 10
PASSWORD_ATTEMPTS = 3
INSTALL_ATTEMPTS = 2
CLEAR_SCREEN = "\x1b[2J\x1b[3J\x1b[H"
MANUAL_DEVICE_CLEANUP = (
    "сессия не сохранена, завершите устройство вручную: Telegram → Настройки → Устройства"
)


class Refused(RuntimeError):
    """Value-blind refusal."""


class Inputs(NamedTuple):
    values: dict[str, str]
    source: str
    dropped_nonsecret: tuple[str, ...]


def credential_path(profile: str) -> Path:
    return CREDSTORE / f"telegram-mcp-{profile}.env.cred"


def check_preconditions(profile: str, self_test: bool, *, require_tty: bool = True,
                        geteuid=os.geteuid, isatty=os.isatty) -> None:
    if geteuid() != 0:
        raise Refused("must run as root")
    if require_tty and not (isatty(0) and isatty(1)):
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


def load_carried_values(profile: str, *, include_session: bool = False,
                        legacy_only: bool = False) -> Inputs:
    """Read the profile source and return the validated secret subset (names checked).

    Login mode (include_session=False) drops the profile's session keys because
    they are replaced by the new QR session.  Migration mode keeps them and reads
    only the legacy env.
    """

    encrypted = credential_path(profile)
    legacy = LEGACY_ENV_DIR / f"{profile}.env"
    if not legacy_only and encrypted.is_file():
        raw, source = decrypt_credential(encrypted), f"encrypted:{encrypted}"
    elif legacy.is_file():
        raw, source = legacy.read_bytes(), f"legacy:{legacy}"
    elif legacy_only:
        raise Refused(f"legacy credential source does not exist: {legacy}")
    else:
        raise Refused("neither encrypted nor legacy credential source exists")
    try:
        values = parse_dotenv(raw)
        keyset = profile_key_set(profile)
        exempt = () if include_session else keyset.session_keys
        carried = validate_profile_secrets(
            values, profile, allow_legacy_nonsecret=True, exempt_required=exempt
        )
    except PayloadError as exc:
        raise Refused(f"{source}: {exc}") from exc
    if not include_session:
        carried = {key: value for key, value in carried.items() if key not in keyset.session_keys}
    if not carried["TELEGRAM_API_ID"].isdigit():
        raise Refused(f"{source}: TELEGRAM_API_ID is not an integer")
    dropped = tuple(key for key in values if key in LEGACY_NONSECRET_KEYS)
    return Inputs(carried, source, dropped)


def render_qr(url: str, out) -> None:
    import qrcode

    code = qrcode.QRCode(border=2)
    code.add_data(url)
    code.make(fit=True)
    code.print_ascii(out=out, tty=True)  # raises OSError when out is not a TTY
    out.flush()


async def qr_login(api_id: int, api_hash: str, *, on_session, logout_on_failure: bool = False,
                   client_factory=None, render=render_qr, ask_password=getpass.getpass,
                   out=None) -> tuple[int, object]:
    """Log in by QR and hand the session to ``on_session`` while still connected.

    ``on_session(session) -> int`` installs the credential (0 = saved).  On a
    non-zero result the session is NOT logged out unless ``logout_on_failure``;
    the operator is told to terminate the device manually.
    """

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
        out.write(f"telegram_login=ok user_id={getattr(user, 'id', None)} "
                  f"username={getattr(user, 'username', None) or '-'}\n")
        out.flush()
        try:
            rc = on_session(session)
        except Exception as exc:  # value-blind; the session must still be reported
            print(f"install failed: {type(exc).__name__}", file=sys.stderr)
            rc = 1
        del session
        if rc != 0:
            if logout_on_failure:
                try:
                    await client.log_out()
                    print("session_logout=done (--logout-on-install-failure); "
                          "the new device is terminated", file=sys.stderr)
                except Exception as exc:  # value-blind
                    print(f"session_logout=failed {type(exc).__name__}; {MANUAL_DEVICE_CLEANUP}",
                          file=sys.stderr)
            else:
                print(MANUAL_DEVICE_CLEANUP, file=sys.stderr)
        return rc, user
    finally:
        await client.disconnect()


def build_payload(profile: str, carried: dict[str, str], session: str | None) -> bytes:
    """Serialize the profile payload; ``session`` replaces every profile session key."""

    keyset = profile_key_set(profile)
    values = dict(carried)
    if session is not None:
        values.update({key: session for key in keyset.session_keys})
    try:
        complete = validate_profile_secrets(values, profile, allow_legacy_nonsecret=False)
    except PayloadError as exc:
        raise Refused(f"payload: {exc}") from exc
    return serialize_dotenv(complete, keyset.ordered)


def install_payload(profile: str, payload: bytes) -> int:
    result = subprocess.run(
        [sys.executable, *CHILD_PYTHON_FLAGS, str(INSTALLER), "--profile", profile],
        input=payload, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    for stream, text in ((sys.stdout, result.stdout), (sys.stderr, result.stderr)):
        if text:
            stream.write("installer: " + text.decode("utf-8", "replace"))
    return result.returncode


def install_with_retry(profile: str, payload: bytes, *, require_unit_stopped: bool,
                       install=None) -> int:
    """Install from memory; one retry after a failure.  Returns the last rc."""

    install = install or install_payload
    rc = 1
    for attempt in range(1, INSTALL_ATTEMPTS + 1):
        try:
            if require_unit_stopped:
                ensure_unit_stopped(profile)
            rc = install(profile, payload)
        except Refused as exc:
            print(f"install_attempt={attempt} refused: {exc}", file=sys.stderr)
            rc = 2
        except OSError as exc:
            print(f"install_attempt={attempt} failed: {type(exc).__name__}", file=sys.stderr)
            rc = 1
        if rc == 0:
            return 0
        print(f"install_attempt={attempt}/{INSTALL_ATTEMPTS} installer_rc={rc}", file=sys.stderr)
    return rc


def untrusted_sys_path_entries(paths: list[str]) -> list[str]:
    """Entries an unprivileged user could influence ('' = cwd, /home, tmp dirs)."""

    return [
        entry for entry in paths
        if not entry or not entry.startswith("/")
        or any((entry.rstrip("/") + "/").startswith(prefix) for prefix in UNTRUSTED_PATH_PREFIXES)
    ]


def writable_by_non_root(roots: list[Path]) -> list[str]:
    """Paths under ``roots`` (and their parent chain) not owned by root or group/other-writable."""

    findings: list[str] = []
    seen: set[Path] = set()

    def check(path: Path) -> None:
        if path in seen:
            return
        seen.add(path)
        info = path.lstat()
        if info.st_uid != 0 or (
            not stat.S_ISLNK(info.st_mode) and info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        ):
            findings.append(str(path))

    for root in roots:
        for parent in [root, *root.parents]:
            check(parent)
        if root.is_dir():
            for dirpath, dirnames, filenames in os.walk(root):
                for name in dirnames + filenames:
                    check(Path(dirpath) / name)
    return findings


def run_trust_checks() -> None:
    import qrcode
    import telethon

    print(f"python_executable={sys.executable} prefix={sys.prefix}")
    print(f"flags isolated={sys.flags.isolated} ignore_environment={sys.flags.ignore_environment} "
          f"no_user_site={sys.flags.no_user_site} dont_write_bytecode={sys.flags.dont_write_bytecode}")
    for index, entry in enumerate(sys.path):
        print(f"sys.path[{index}]={entry!r}")
    modules = {"telethon": telethon.__file__, "qrcode": qrcode.__file__, "secure_qr_login": __file__}
    for name, path in modules.items():
        print(f"module {name}={path}")
    untrusted = untrusted_sys_path_entries(sys.path + list(modules.values()))
    tree = writable_by_non_root([Path(sys.prefix), HERE])
    print(f"sys_path_untrusted_entries={len(untrusted)} trusted_tree_findings={len(tree)}")
    for finding in untrusted + tree:
        print(f"  untrusted: {finding}")
    if untrusted or tree or not sys.flags.isolated:
        raise Refused("interpreter trust check FAILED")


def run_self_test() -> None:
    run_trust_checks()
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
        rc = install_with_retry(SELFTEST_PROFILE, build_payload(SELFTEST_PROFILE, carried, marker),
                                require_unit_stopped=True)
        if rc != 0:
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


def print_dry_inputs(profile: str, migrate_only: bool) -> None:
    inputs = load_carried_values(profile, include_session=migrate_only, legacy_only=migrate_only)
    keyset = profile_key_set(profile)
    print(f"profile={profile} mode={'migrate-only' if migrate_only else 'qr-login'} "
          f"telegram=not-contacted unit_state={unit_state(profile)}")
    print(f"inputs={inputs.source}")
    print(f"carried_secret_keys={','.join(inputs.values)}")
    print("session_keys_replaced_by_login="
          + ("-" if migrate_only else ",".join(keyset.session_keys)))
    print(f"dropped_nonsecret_keys={','.join(inputs.dropped_nonsecret) or '-'}")
    print(f"target={credential_path(profile)} exists={credential_path(profile).exists()}")


def main(argv: list[str] | None = None, *, geteuid=os.geteuid, isatty=os.isatty) -> int:
    parser = argparse.ArgumentParser(description="Telegram QR login -> encrypted systemd credential")
    parser.add_argument("profile")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--dry-inputs", action="store_true",
                        help="print profile, source and key NAMES; no Telegram, no install")
    parser.add_argument("--migrate-only", action="store_true",
                        help="install the legacy env as encrypted credential; no Telegram")
    parser.add_argument("--replace", action="store_true",
                        help="with --migrate-only: allow replacing an existing credential")
    parser.add_argument("--logout-on-install-failure", action="store_true",
                        help="log the new session out if both install attempts fail")
    args = parser.parse_args(argv)
    if args.self_test and (args.dry_inputs or args.migrate_only):
        parser.error("--self-test cannot be combined with --dry-inputs/--migrate-only")
    if args.replace and not args.migrate_only:
        parser.error("--replace requires --migrate-only")
    if args.logout_on_install_failure and (args.migrate_only or args.dry_inputs or args.self_test):
        parser.error("--logout-on-install-failure applies to QR login only")
    no_telegram = args.dry_inputs or args.migrate_only
    try:
        check_preconditions(args.profile, args.self_test, require_tty=not no_telegram,
                            geteuid=geteuid, isatty=isatty)
        if args.dry_inputs:
            print_dry_inputs(args.profile, args.migrate_only)
            return 0
        if args.migrate_only:
            if credential_path(args.profile).exists() and not args.replace:
                raise Refused(f"{credential_path(args.profile)} already exists; use --replace")
            inputs = load_carried_values(args.profile, include_session=True, legacy_only=True)
            print(f"profile={args.profile} mode=migrate-only inputs={inputs.source} "
                  f"carried_keys={','.join(inputs.values)} "
                  f"dropped_nonsecret_keys={','.join(inputs.dropped_nonsecret) or '-'}")
            rc = install_with_retry(args.profile, build_payload(args.profile, inputs.values, None),
                                    require_unit_stopped=False)
            print(f"encrypted_credential={credential_path(args.profile)} installer_rc={rc} "
                  "telegram=not-contacted unit_restart=not-performed")
            return rc
        ensure_unit_stopped(args.profile)
        if args.self_test:
            run_self_test()
            return 0
        inputs = load_carried_values(args.profile)
        print(f"profile={args.profile} inputs={inputs.source} "
              f"carried_keys={','.join(inputs.values)} "
              f"session_keys={','.join(profile_key_set(args.profile).session_keys)}")

        def on_session(session: str) -> int:
            return install_with_retry(args.profile, build_payload(args.profile, inputs.values, session),
                                      require_unit_stopped=True)

        rc, _user = asyncio.run(qr_login(
            int(inputs.values["TELEGRAM_API_ID"]), inputs.values["TELEGRAM_API_HASH"],
            on_session=on_session, logout_on_failure=args.logout_on_install_failure,
        ))
        print(f"encrypted_credential={credential_path(args.profile)} installer_rc={rc} "
              "unit_restart=not-performed")
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
