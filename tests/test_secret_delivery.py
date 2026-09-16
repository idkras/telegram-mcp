"""Secret delivery contract: GPG archive + systemd encrypted runtime."""

from __future__ import annotations

import importlib.util
import os
import stat
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy"
sys.path.insert(0, str(DEPLOY))

from encrypted_credential import PayloadError, parse_dotenv  # noqa: E402


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, DEPLOY / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


installer = _load("install_encrypted_credential", "install-encrypted-credential.py")
runner = _load("run_with_encrypted_credential", "run-with-encrypted-credential.py")

NORMALIZER_PATH = ROOT / "scripts" / "secrets-normalize-payload.py"
normalizer_spec = importlib.util.spec_from_file_location("secrets_normalize_payload", NORMALIZER_PATH)
assert normalizer_spec and normalizer_spec.loader
normalizer = importlib.util.module_from_spec(normalizer_spec)
normalizer_spec.loader.exec_module(normalizer)


def _payload(session: str = "test-only-session") -> bytes:
    return (
        "TELEGRAM_API_ID=test-only-id\n"
        "TELEGRAM_API_HASH=test-only-hash\n"
        f"TELEGRAM_SESSION_STRING={session}\n"
        "SUPABASE_DB_URL=postgresql://test-only\n"
    ).encode()


def _fake_systemd_creds(tmp_path: Path) -> Path:
    binary = tmp_path / "systemd-creds"
    binary.write_text(
        """#!/usr/bin/env python3
import pathlib
import sys
action, source, destination = sys.argv[1], sys.argv[-2], sys.argv[-1]
if action == "encrypt":
    pathlib.Path(destination).write_bytes(b"host-encrypted:" + sys.stdin.buffer.read())
elif action == "decrypt":
    payload = pathlib.Path(source).read_bytes()
    if not payload.startswith(b"host-encrypted:"):
        raise SystemExit(1)
    sys.stdout.buffer.write(payload[len(b"host-encrypted:"):])
else:
    raise SystemExit(2)
"""
    )
    binary.chmod(0o755)
    return binary


def test_payload_parser_is_data_not_shell_and_preserves_equals():
    values = parse_dotenv(
        b'TELEGRAM_SESSION_STRING="abc==${NOT_EXPANDED}"\nTELEGRAM_API_ID=1\n'
    )
    assert values["TELEGRAM_SESSION_STRING"] == "abc==${NOT_EXPANDED}"
    assert values["TELEGRAM_API_ID"] == "1"


def test_payload_parser_rejects_duplicate_and_invalid_lines():
    with pytest.raises(PayloadError, match="duplicate secret keys"):
        parse_dotenv(b"TELEGRAM_API_ID=one\nTELEGRAM_API_ID=two\n")
    with pytest.raises(PayloadError, match="non-assignment"):
        parse_dotenv(b"not-an-assignment\n")


def test_installer_encrypts_verifies_atomically_and_keeps_encrypted_rollback(tmp_path):
    binary = _fake_systemd_creds(tmp_path)
    output = tmp_path / "credstore" / "telegram-mcp-ikrasinsky.env.cred"

    installed, rollback = installer.install_credential(
        "ikrasinsky", _payload(), output, systemd_creds=str(binary)
    )
    assert installed == output.resolve()
    assert rollback is None
    assert b"test-only-session" not in output.read_bytes().split(b":", 1)[0]
    assert stat.S_IMODE(output.stat().st_mode) == 0o600

    previous = output.read_bytes()
    _, rollback = installer.install_credential(
        "ikrasinsky", _payload("replacement-session"), output, systemd_creds=str(binary)
    )
    assert rollback and rollback.read_bytes() == previous
    assert stat.S_IMODE(rollback.stat().st_mode) == 0o600


def test_installer_fails_closed_on_missing_or_unknown_keys(tmp_path):
    with pytest.raises(installer.CredentialInstallError, match="missing required secret keys"):
        installer.canonicalize_payload(b"TELEGRAM_API_ID=test-only\n", "ikrasinsky")
    with pytest.raises(installer.CredentialInstallError, match="unknown secret keys"):
        installer.canonicalize_payload(_payload() + b"PYTHONPATH=/untrusted\n", "ikrasinsky")


def test_installer_verifies_real_encrypted_payload_and_rejects_corruption(tmp_path):
    binary = _fake_systemd_creds(tmp_path)
    output = tmp_path / "telegram-mcp-ikrasinsky.env.cred"
    installer.install_credential("ikrasinsky", _payload(), output, systemd_creds=str(binary))
    assert installer.verify_encrypted_credential(
        output, systemd_creds=str(binary)
    ) == installer.REQUIRED_SECRET_KEYS

    output.write_bytes(b"not-an-encrypted-credential")
    with pytest.raises(installer.CredentialInstallError, match="decrypt failed"):
        installer.verify_encrypted_credential(output, systemd_creds=str(binary))


def test_installer_strips_legacy_nonsecret_unit_settings():
    canonical = installer.canonicalize_payload(
        _payload() + b"TELEGRAM_USER=lisa\nLABA_MODE=true\nSUPABASE_URL=https://example.invalid\n",
        "ikrasinsky",
    )
    assert b"TELEGRAM_USER" not in canonical
    assert b"LABA_MODE" not in canonical
    assert b"SUPABASE_URL" not in canonical


def test_normalizer_replaces_legacy_env_with_secret_only_mode_0600(tmp_path):
    path = tmp_path / "sandbox-ik-ikrasinsky.secrets.env"
    path.write_bytes(_payload() + b"TELEGRAM_USER=lisa\nLABA_MODE=true\n")
    keys = normalizer.normalize_in_place(path)
    normalized = parse_dotenv(path.read_bytes())
    assert tuple(normalized) == keys
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_runtime_loader_reads_only_regular_credential_and_preserves_base_env(tmp_path):
    credential_dir = tmp_path / "credentials"
    credential_dir.mkdir()
    credential = credential_dir / "telegram_env"
    credential.write_bytes(installer.canonicalize_payload(_payload(), "ikrasinsky"))
    env = runner.build_exec_environment(credential_dir, {"TELEGRAM_USER": "ikrasinsky"})
    assert env["TELEGRAM_USER"] == "ikrasinsky"
    assert env["TELEGRAM_SESSION_STRING"] == "test-only-session"

    credential.unlink()
    target = tmp_path / "other"
    target.write_bytes(_payload())
    credential.symlink_to(target)
    with pytest.raises(RuntimeError, match="missing or not a regular file"):
        runner.build_exec_environment(credential_dir, {"TELEGRAM_USER": "ikrasinsky"})


def test_portable_archive_contract_matches_laba_without_env_passphrase_override():
    justfile = (ROOT / "justfile").read_text()
    assert "rickai/secrets" in justfile
    assert "scripts/secrets-tool.sh" in justfile
    assert "SECRETS_PASSPHRASE" not in "\n".join(
        line for line in justfile.splitlines() if not line.lstrip().startswith("#")
    )
    ignore = (ROOT / ".gitignore").read_text()
    assert "config/*.secrets.env" in ignore
    assert "config/*.secrets.env.expected" in ignore

    wrapper = (ROOT / "scripts" / "secrets-tool.sh").read_text()
    assert "rickai/secrets" in wrapper
    assert 'docker_io=(--interactive)' in wrapper
    assert "docker_io+=(--tty)" in wrapper
    assert "requires an interactive terminal" in wrapper
    assert "SECRETS_PASSPHRASE" not in wrapper

    template = (ROOT / "config" / "sandbox-ik.secret-template.env").read_text()
    for key in installer.REQUIRED_SECRET_KEYS:
        assert f"{key}=\n" in template
    assert "TELEGRAM_USER=" not in template
    assert "TELEGRAM_SESSION_STRING=\n" in template


def test_docs_require_four_readbacks_before_plaintext_retirement():
    readme = (ROOT / "README.md").read_text()
    for marker in (
        "GPG decrypt",
        "readback=match",
        "restart/health",
        "Supabase",
        ".rollback",
    ):
        assert marker in readme


def test_migration_script_preserves_legacy_until_external_readbacks():
    script = (ROOT / "scripts" / "secrets-sandbox-ik-migrate.sh").read_text()
    assert 'secrets-tool.sh" encrypt "$environment"' in script
    assert 'secrets-tool.sh" clear "$environment"' in script
    assert 'secrets-tool.sh" decrypt "$environment"' in script
    assert "mktemp -d /tmp/telegram-mcp-secrets." in script
    assert 'expected="$scratch_dir/expected.env"' in script
    assert "SECRETS_PASSPHRASE" not in script
    assert 'cmp -s "$expected" "$plaintext"' in script
    assert "secrets-normalize-payload.py" in script
    assert "install-encrypted-credential.py" in script
    assert "legacy=preserved" in script
    assert "sudo -n cat" in script
    assert "rm -f -- \"$plaintext\"" in script
    assert "rm -f -- \"$legacy\"" not in script


def test_systemd_units_use_only_encrypted_credentials():
    listener = (ROOT / "deploy" / "telegram-mcp.service.template").read_text()
    backfill = (ROOT / "deploy" / "telegram-mcp-backfill@.service").read_text()
    deploy = (ROOT / "deploy" / "deploy-sandbox-ik.sh").read_text()
    for unit in (listener, backfill):
        assert "LoadCredentialEncrypted=telegram_env:" in unit
        assert "run-with-encrypted-credential.py" in unit
        assert "EnvironmentFile=" not in unit
    assert "/etc/credstore.encrypted" in deploy
    assert "ENV_DIR=" not in deploy
    assert "install env skeleton" not in deploy
    assert "--verify-encrypted" in deploy
    assert "--prepare-code-only" in deploy
    assert deploy.index("--verify-encrypted") < deploy.index("unit written")
    assert deploy.index('if [ "$PREPARE_CODE_ONLY" = 1 ]') < deploy.index(
        'import credentials_registry'
    )


# ---------------------------------------------------------------------------
# Profile-scoped key sets (critic finding H)
# ---------------------------------------------------------------------------

def _lisa_payload(session: str = "test-only-lisa-session") -> bytes:
    return _payload(session) + (
        "LISA_TG_API_KEY=test-only-id\n"
        "LISA_TG_APP_HASH=test-only-hash\n"
        f"LISA_TG_SESSION={session}\n"
    ).encode()


def test_ikrasinsky_profile_keeps_rick_api_key_and_drops_nonsecret_settings():
    canonical = installer.canonicalize_payload(
        _payload() + b"SUPABASE_RICK_API_KEY=test-only-rick\nTELEGRAM_USER=ikrasinsky\n"
        b"LABA_MODE=1\nSUPABASE_URL=https://example.invalid\n",
        "ikrasinsky",
    )
    assert tuple(parse_dotenv(canonical)) == installer.REQUIRED_SECRET_KEYS + ("SUPABASE_RICK_API_KEY",)


@pytest.mark.parametrize("extra", [b"LISA_TG_SESSION=x\n", b"LD_PRELOAD=/tmp/x.so\n", b"SUPABASE_RICKAI_DB_URL=x\n"])
def test_ikrasinsky_profile_refuses_keys_outside_its_set(extra):
    with pytest.raises(installer.CredentialInstallError, match="unknown secret keys for profile ikrasinsky"):
        installer.canonicalize_payload(_payload() + extra, "ikrasinsky")


def test_lisa_profile_accepts_and_requires_lisa_tg_keys():
    canonical = installer.canonicalize_payload(
        _lisa_payload() + b"SUPABASE_RICK_API_KEY=test-only-rick\nTELEGRAM_USER=lisa\n", "lisa"
    )
    keys = tuple(parse_dotenv(canonical))
    assert {"LISA_TG_API_KEY", "LISA_TG_APP_HASH", "LISA_TG_SESSION", "SUPABASE_RICK_API_KEY"} <= set(keys)
    with pytest.raises(installer.CredentialInstallError,
                       match="missing required secret keys for profile lisa: LISA_TG_API_KEY"):
        installer.canonicalize_payload(_payload(), "lisa")


def test_alias_keys_must_carry_identical_values():
    diverged = _payload("new-session") + (
        b"LISA_TG_API_KEY=test-only-id\nLISA_TG_APP_HASH=test-only-hash\nLISA_TG_SESSION=stale-session\n"
    )
    with pytest.raises(installer.CredentialInstallError, match="LISA_TG_SESSION,TELEGRAM_SESSION_STRING"):
        installer.canonicalize_payload(diverged, "lisa")


def test_unknown_profile_and_foreign_file_name_refuse(tmp_path):
    with pytest.raises(installer.CredentialInstallError, match="no declared secret key set"):
        installer.canonicalize_payload(_payload(), "mallory")
    binary = _fake_systemd_creds(tmp_path)
    with pytest.raises(installer.CredentialInstallError, match="belongs to another profile"):
        installer.install_credential(
            "ikrasinsky", _payload(), tmp_path / "telegram-mcp-lisa.env.cred", systemd_creds=str(binary)
        )


def test_verify_infers_profile_from_credential_file_name(tmp_path):
    binary = _fake_systemd_creds(tmp_path)
    output = tmp_path / "telegram-mcp-lisa.env.cred"
    installer.install_credential("lisa", _lisa_payload(), output, systemd_creds=str(binary))
    keys = installer.verify_encrypted_credential(output, systemd_creds=str(binary))
    assert "LISA_TG_SESSION" in keys
    with pytest.raises(installer.CredentialInstallError, match="cannot infer profile"):
        installer.verify_encrypted_credential(tmp_path / "other.cred", systemd_creds=str(binary))


def test_runtime_loader_is_profile_scoped(tmp_path):
    credential_dir = tmp_path / "credentials"
    credential_dir.mkdir()
    credential = credential_dir / "telegram_env"
    credential.write_bytes(installer.canonicalize_payload(_lisa_payload(), "lisa"))
    env = runner.build_exec_environment(credential_dir, {"TELEGRAM_USER": "lisa"})
    assert env["LISA_TG_SESSION"] == env["TELEGRAM_SESSION_STRING"] == "test-only-lisa-session"

    with pytest.raises(RuntimeError, match="TELEGRAM_USER must name a runtime profile"):
        runner.build_exec_environment(credential_dir, {})
    with pytest.raises(RuntimeError, match="unknown secret keys for profile ikrasinsky"):
        runner.build_exec_environment(credential_dir, {"TELEGRAM_USER": "ikrasinsky"})
    credential.write_bytes(_payload() + b"LD_PRELOAD=/tmp/evil.so\n")
    with pytest.raises(RuntimeError, match="unknown secret keys for profile ikrasinsky: LD_PRELOAD"):
        runner.build_exec_environment(credential_dir, {"TELEGRAM_USER": "ikrasinsky"})
