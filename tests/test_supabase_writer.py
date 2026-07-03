"""Tests for supabase_writer.py

JTBD: Verify that SupabaseWriter correctly transforms Telethon messages
into Supabase-compatible rows, handles JSON serialization edge cases,
and manages batch operations.

Data source: Unit tests with mock Telethon message objects (no live Supabase).
"""
import json
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, AsyncMock, patch

import sys
import os

# Ensure heroes_platform is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))


class TestMakeJsonSafe:
    """Test the _make_json_safe helper function."""

    def test_bytes_converted_to_hex(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import _make_json_safe
        result = _make_json_safe(b"\x00\xff")
        assert result == "00ff"

    def test_datetime_converted_to_iso(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import _make_json_safe
        dt = datetime(2026, 2, 9, 12, 0, 0, tzinfo=timezone.utc)
        result = _make_json_safe(dt)
        assert "2026-02-09" in result

    def test_dict_recursion(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import _make_json_safe
        data = {"key": b"\xab", "nested": {"inner": datetime(2026, 1, 1, tzinfo=timezone.utc)}}
        result = _make_json_safe(data)
        assert result["key"] == "ab"
        assert "2026-01-01" in result["nested"]["inner"]

    def test_list_recursion(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import _make_json_safe
        data = [b"\x01", datetime(2026, 1, 1, tzinfo=timezone.utc), "text"]
        result = _make_json_safe(data)
        assert result[0] == "01"
        assert result[2] == "text"

    def test_primitives_pass_through(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import _make_json_safe
        assert _make_json_safe(42) == 42
        assert _make_json_safe("hello") == "hello"
        assert _make_json_safe(True) is True
        assert _make_json_safe(None) is None

    def test_result_is_json_serializable(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import _make_json_safe
        data = {
            "bytes_field": b"\xde\xad",
            "date_field": datetime(2026, 2, 9, tzinfo=timezone.utc),
            "list_field": [b"\x01", {"nested_bytes": b"\x02"}],
            "normal": "text",
            "number": 123,
        }
        result = _make_json_safe(data)
        # Must not raise
        serialized = json.dumps(result)
        assert isinstance(serialized, str)


class TestTelethonMessageToRow:
    """Test conversion of Telethon-like message objects to Supabase rows."""

    def _make_mock_message(self, msg_id=1, text="Hello", date=None, sender_id=12345):
        """Create a mock Telethon message."""
        msg = MagicMock()
        msg.id = msg_id
        msg.text = text
        msg.date = date or datetime(2026, 2, 9, 12, 0, 0, tzinfo=timezone.utc)
        msg.sender_id = sender_id

        sender = MagicMock()
        sender.id = sender_id
        sender.first_name = "John"
        sender.last_name = "Doe"
        sender.username = "johndoe"
        sender.title = None
        msg.sender = sender

        msg.to_dict.return_value = {
            "id": msg_id,
            "message": text,
            "date": str(msg.date),
            "from_id": {"user_id": sender_id},
        }
        return msg

    def test_basic_conversion(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter
        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.telegram_user_id = "ikrasinsky"

        msg = self._make_mock_message()
        row = writer._telethon_message_to_row(msg, chat_id=100, chat_type="group")

        assert row["chat_id"] == "100"
        assert row["message_id"] == 1
        assert row["text"] == "Hello"
        assert row["sender_name"] == "John Doe"
        assert row["sender_username"] == "johndoe"
        assert row["chat_type"] == "group"
        assert row["source"] == "telegram"
        assert row["telegram_user_id"] == "ikrasinsky"
        assert isinstance(row["raw"], dict)

    def test_message_without_sender(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter
        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.telegram_user_id = "test"

        msg = MagicMock()
        msg.id = 5
        msg.text = "System message"
        msg.date = datetime(2026, 2, 9, tzinfo=timezone.utc)
        msg.sender = None
        msg.sender_id = 999
        msg.to_dict.return_value = {"id": 5, "message": "System message"}

        row = writer._telethon_message_to_row(msg, chat_id=200)
        assert row["sender_user_id"] == "999"
        assert row["sender_name"] == ""

    def test_message_with_none_text(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter
        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.telegram_user_id = "test"

        msg = self._make_mock_message(text=None)
        row = writer._telethon_message_to_row(msg, chat_id=300)
        assert row["text"] == ""

    def test_raw_field_is_json_serializable(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter
        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.telegram_user_id = "test"

        msg = self._make_mock_message()
        # Simulate to_dict returning bytes (common in Telethon)
        msg.to_dict.return_value = {
            "id": 1,
            "photo": b"\x89PNG",
            "date": datetime(2026, 1, 1, tzinfo=timezone.utc),
        }

        row = writer._telethon_message_to_row(msg, chat_id=400)
        # Must not raise
        json.dumps(row["raw"])

    def test_chat_id_is_always_string(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter
        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.telegram_user_id = "test"

        msg = self._make_mock_message()

        # Integer chat_id
        row = writer._telethon_message_to_row(msg, chat_id=12345)
        assert row["chat_id"] == "12345"
        assert isinstance(row["chat_id"], str)

        # String chat_id
        row2 = writer._telethon_message_to_row(msg, chat_id="-100123456")
        assert row2["chat_id"] == "-100123456"


class TestEventHandlers:
    """Test event handler helper functions."""

    def test_get_chat_type_channel(self):
        from heroes_platform.heroes_telegram_mcp.event_handlers import _get_chat_type
        chat = MagicMock()
        chat.broadcast = True
        assert _get_chat_type(chat) == "channel"

    def test_get_chat_type_supergroup(self):
        from heroes_platform.heroes_telegram_mcp.event_handlers import _get_chat_type
        chat = MagicMock()
        chat.broadcast = False
        assert _get_chat_type(chat) == "supergroup"

    def test_get_chat_type_private(self):
        from heroes_platform.heroes_telegram_mcp.event_handlers import _get_chat_type
        chat = MagicMock(spec=[])  # empty spec so no attrs by default
        chat.first_name = "John"
        assert _get_chat_type(chat) == "private"

    def test_get_chat_type_none(self):
        from heroes_platform.heroes_telegram_mcp.event_handlers import _get_chat_type
        assert _get_chat_type(None) == "unknown"

    def test_get_chat_type_group(self):
        from heroes_platform.heroes_telegram_mcp.event_handlers import _get_chat_type
        chat = MagicMock(spec=[])
        chat.megagroup = False
        chat.participants_count = 10
        assert _get_chat_type(chat) == "group"

    def test_get_chat_type_megagroup(self):
        from heroes_platform.heroes_telegram_mcp.event_handlers import _get_chat_type
        chat = MagicMock(spec=[])
        chat.megagroup = True
        assert _get_chat_type(chat) == "supergroup"

    def test_get_chat_type_unknown_fallback(self):
        from heroes_platform.heroes_telegram_mcp.event_handlers import _get_chat_type
        chat = MagicMock(spec=[])
        assert _get_chat_type(chat) == "unknown"


class TestSupabaseWriterInit:
    """Test SupabaseWriter initialization and credential handling."""

    def test_init_defaults(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter
        writer = SupabaseWriter()
        assert writer.telegram_user_id == "ikrasinsky"
        assert writer.batch_size == 50
        assert writer._client is None
        assert writer._batch == []

    def test_init_custom_user(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter
        writer = SupabaseWriter(telegram_user_id="testuser")
        assert writer.telegram_user_id == "testuser"

    def test_lazy_client_not_created_on_init(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter
        writer = SupabaseWriter()
        # Client should NOT be created until first access
        assert writer._client is None


class TestBackfillGuardianTitle:
    @pytest.mark.asyncio
    async def test_backfill_resolves_chat_title_for_guardian_skip_rules(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter

        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.batch_size = 2
        writer.get_chat_cursor = AsyncMock(return_value=None)
        writer.update_chat_cursor = AsyncMock()

        batch_calls = []

        async def write_messages_batch(batch, chat_id, chat_type="unknown", chat_title=None):
            batch_calls.append((list(batch), chat_id, chat_type, chat_title))
            return len(batch)

        writer.write_messages_batch = write_messages_batch

        class Client:
            async def get_entity(self, chat_id):
                self.resolved_chat_id = chat_id
                entity = MagicMock()
                entity.title = "verify bot"
                entity.first_name = None
                entity.username = "verify_bot"
                return entity

            async def iter_messages(self, **_kwargs):
                for msg_id in (3, 2, 1):
                    msg = MagicMock()
                    msg.id = msg_id
                    yield msg

        client = Client()

        written = await writer.backfill_chat(client, "-100123", chat_type="group", limit=3)

        assert written == 3
        assert client.resolved_chat_id == -100123
        assert [call[3] for call in batch_calls] == ["verify bot", "verify bot"]

    @pytest.mark.asyncio
    async def test_catch_up_resolves_chat_title_for_guardian_skip_rules(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter

        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.batch_size = 2
        writer.get_chat_cursor = AsyncMock(return_value={"last_seen_message_id": 10})
        writer.update_chat_cursor = AsyncMock()

        batch_calls = []

        async def write_messages_batch(batch, chat_id, chat_type="unknown", chat_title=None):
            batch_calls.append((list(batch), chat_id, chat_type, chat_title))
            return len(batch)

        writer.write_messages_batch = write_messages_batch

        class Client:
            async def get_entity(self, chat_id):
                self.resolved_chat_id = chat_id
                entity = MagicMock()
                entity.title = None
                entity.first_name = None
                entity.username = "verify_bot"
                return entity

            async def iter_messages(self, **_kwargs):
                for msg_id in (11, 12, 13):
                    msg = MagicMock()
                    msg.id = msg_id
                    yield msg

        client = Client()

        written = await writer.catch_up_recent(client, "-100123", chat_type="group", limit=3)

        assert written == 3
        assert client.resolved_chat_id == -100123
        assert [call[3] for call in batch_calls] == ["verify_bot", "verify_bot"]


class TestPartialBatchCursorSafety:
    class Msg:
        def __init__(self, msg_id):
            self.id = msg_id

    class Client:
        async def get_entity(self, chat_id):
            return None

        async def iter_messages(self, **kwargs):
            start = int(kwargs.get("min_id") or 0) + 1
            for msg_id in range(start, start + 10):
                yield TestPartialBatchCursorSafety.Msg(msg_id)

    @pytest.mark.asyncio
    async def test_catch_up_does_not_advance_cursor_after_partial_batch_write(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter

        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.batch_size = 10
        writer.get_chat_cursor = AsyncMock(return_value={"last_seen_message_id": 10})
        writer.update_chat_cursor = AsyncMock(return_value=True)

        async def partial_write(batch, chat_id, chat_type="unknown", chat_title=None):
            return 5

        writer.write_messages_batch = partial_write

        written = await writer.catch_up_recent(self.Client(), "123", limit=10)

        assert written == 5
        writer.update_chat_cursor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_catch_up_advances_cursor_after_full_batch_write(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter

        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.batch_size = 10
        writer.get_chat_cursor = AsyncMock(return_value={"last_seen_message_id": 10})
        writer.update_chat_cursor = AsyncMock(return_value=True)

        async def full_write(batch, chat_id, chat_type="unknown", chat_title=None):
            return len(batch)

        writer.write_messages_batch = full_write

        written = await writer.catch_up_recent(self.Client(), "123", limit=10)

        assert written == 10
        writer.update_chat_cursor.assert_awaited_once_with("123", last_seen_message_id=20)

    @pytest.mark.asyncio
    async def test_backfill_does_not_advance_cursor_after_partial_batch_write(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter

        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.batch_size = 10
        writer.get_chat_cursor = AsyncMock(return_value=None)
        writer.update_chat_cursor = AsyncMock(return_value=True)

        async def partial_write(batch, chat_id, chat_type="unknown", chat_title=None):
            return 5

        writer.write_messages_batch = partial_write

        written = await writer.backfill_chat(self.Client(), "123", limit=10)

        assert written == 5
        writer.update_chat_cursor.assert_not_awaited()


class TestIngestRunFailureVisibility:
    class RaisingQuery:
        def insert(self, *_args, **_kwargs):
            return self

        def update(self, *_args, **_kwargs):
            return self

        def eq(self, *_args, **_kwargs):
            return self

        def execute(self):
            raise RuntimeError("supabase down")

    @pytest.mark.asyncio
    async def test_start_ingest_run_surfaces_table_write_failure(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter

        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.telegram_user_id = "ikrasinsky"
        writer._postgres_url = None
        writer._table = MagicMock(return_value=self.RaisingQuery())

        with pytest.raises(RuntimeError, match="supabase down"):
            await writer.start_ingest_run("listener_heartbeat")

    @pytest.mark.asyncio
    async def test_record_runtime_event_surfaces_marker_write_failure(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter

        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.telegram_user_id = "ikrasinsky"
        writer._postgres_url = None
        writer._table = MagicMock(return_value=self.RaisingQuery())

        with pytest.raises(RuntimeError, match="supabase down"):
            await writer.record_runtime_event("listener_heartbeat")


class TestRuntimeHealthEvaluation:
    def test_runtime_health_requires_listener_heartbeat(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import _evaluate_runtime_health

        ok, message = _evaluate_runtime_health(
            listener_event_at=None,
            latest_message_at=None,
            max_staleness_seconds=180,
            transport_message="transport ok",
        )

        assert ok is False
        assert "no listener heartbeat" in message

    def test_runtime_health_rejects_stale_heartbeat(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import _evaluate_runtime_health

        stale = datetime.now(tz=timezone.utc) - timedelta(seconds=600)
        ok, message = _evaluate_runtime_health(
            listener_event_at=stale,
            latest_message_at=None,
            max_staleness_seconds=180,
            transport_message="transport ok",
        )

        assert ok is False
        assert "stale" in message

    def test_runtime_health_accepts_fresh_heartbeat(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import _evaluate_runtime_health

        fresh = datetime.now(tz=timezone.utc) - timedelta(seconds=15)
        latest_message = datetime.now(tz=timezone.utc) - timedelta(seconds=5)
        ok, message = _evaluate_runtime_health(
            listener_event_at=fresh,
            latest_message_at=latest_message,
            max_staleness_seconds=180,
            transport_message="transport ok",
        )

        assert ok is True
        assert "listener heartbeat age=" in message
        assert "latest message age=" in message


class TestConversationsSQLView:
    """Validate the SQL migration file structure."""

    def _migration_path(self):
        return os.path.join(
            os.path.dirname(__file__), "..", "..", "..",
            "self-hosted-solutions", "supabase", "supabase", "migrations",
            "20260209000001_telegram_conversations_view.sql"
        )

    def test_migration_file_exists(self):
        path = self._migration_path()
        assert os.path.exists(path), f"Migration file not found at {path}"

    def test_migration_contains_required_objects(self):
        path = self._migration_path()
        with open(path) as f:
            sql = f.read()

        # Must create the helper function
        assert "extract_reply_to_id" in sql
        # Must create conversations view
        assert "telegram_conversations" in sql
        # Must create unanswered view
        assert "telegram_unanswered_conversations" in sql
        # Must use recursive CTE
        assert "reply_roots" in sql
        # Must have time proximity logic (30 min = 1800 seconds)
        assert "1800" in sql
        # Must have reply chain depth limit
        assert "depth < 50" in sql

    def test_migration_depends_on_telegram_messages_raw(self):
        path = self._migration_path()
        with open(path) as f:
            sql = f.read()

        assert "telegram_messages_raw" in sql
        assert "telegram_chats" in sql


class TestDeploymentFiles:
    """Validate deployment file structure."""

    def _project_root(self):
        return os.path.join(os.path.dirname(__file__), "..", "..", "..")

    def test_dockerfile_laba_exists(self):
        path = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "Dockerfile.laba")
        assert os.path.exists(path)

    def test_docker_compose_laba_exists(self):
        path = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "docker-compose.laba.yml")
        assert os.path.exists(path)

    def test_requirements_laba_exists(self):
        path = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "requirements-laba.txt")
        assert os.path.exists(path)

    def test_deploy_script_exists(self):
        path = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "scripts", "deploy-to-laba.sh")
        assert os.path.exists(path)

    def test_deploy_script_is_executable(self):
        path = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "scripts", "deploy-to-laba.sh")
        assert os.access(path, os.X_OK)

    def test_dockerfile_sets_laba_mode(self):
        path = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "Dockerfile.laba")
        with open(path) as f:
            content = f.read()
        assert "LABA_MODE=true" in content

    def test_dockerfile_sets_pythonpath(self):
        path = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "Dockerfile.laba")
        with open(path) as f:
            content = f.read()
        assert "PYTHONPATH" in content

    def test_docker_compose_has_volume(self):
        path = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "docker-compose.laba.yml")
        with open(path) as f:
            content = f.read()
        assert "volumes:" in content
        assert "telegram-session-data" in content

    def test_docker_compose_has_healthcheck(self):
        path = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "docker-compose.laba.yml")
        with open(path) as f:
            content = f.read()
        assert "healthcheck:" in content

    def test_no_secrets_in_dockerfile(self):
        """SECURITY: Ensure no secrets/credentials are hardcoded in Dockerfile."""
        path = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "Dockerfile.laba")
        with open(path) as f:
            content = f.read()
        # Check no actual values for secrets
        assert 'TELEGRAM_API_ID=""' in content or "TELEGRAM_API_ID=" in content
        assert 'SUPABASE_API_KEY=""' in content or "SUPABASE_API_KEY=" in content
        # No real API keys
        for line in content.split("\n"):
            if "API_KEY=" in line or "API_HASH=" in line or "SESSION_STRING=" in line:
                val = line.split("=", 1)[-1].strip().strip('"')
                assert val == "" or val.startswith("$"), f"Possible secret in Dockerfile: {line}"

    def test_dockerfile_copies_init_files(self):
        """ARCHITECTURE: Ensure __init__.py files are copied for module imports."""
        path = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "Dockerfile.laba")
        with open(path) as f:
            content = f.read()
        assert "heroes_platform/__init__.py" in content
        assert "heroes_platform/heroes_telegram_mcp/__init__.py" in content

    def test_healthcheck_matches_cmd(self):
        """ARCHITECTURE: Healthcheck must match the actual process name from CMD."""
        dockerfile = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "Dockerfile.laba")
        compose = os.path.join(self._project_root(), "heroes_platform", "heroes_telegram_mcp", "docker-compose.laba.yml")
        with open(dockerfile) as f:
            df_content = f.read()
        with open(compose) as f:
            dc_content = f.read()
        # CMD runs heroes_platform.heroes_telegram_mcp.main
        assert "heroes_platform.heroes_telegram_mcp.main" in df_content
        # Healthcheck should search for the same process
        assert "heroes_platform.heroes_telegram_mcp.main" in df_content
        assert "heroes_platform.heroes_telegram_mcp.main" in dc_content


class TestN8nWorkflow:
    """Validate n8n workflow JSON structure."""

    def test_workflow_file_exists(self):
        path = os.path.join(
            os.path.dirname(__file__), "..", "..", "..",
            "[n8n] workflows", "n8n-daily-client-digest.json"
        )
        assert os.path.exists(path)

    def test_workflow_is_valid_json(self):
        path = os.path.join(
            os.path.dirname(__file__), "..", "..", "..",
            "[n8n] workflows", "n8n-daily-client-digest.json"
        )
        with open(path) as f:
            data = json.load(f)
        assert isinstance(data, dict)
        assert "nodes" in data
        assert "connections" in data

    def test_workflow_has_required_nodes(self):
        path = os.path.join(
            os.path.dirname(__file__), "..", "..", "..",
            "[n8n] workflows", "n8n-daily-client-digest.json"
        )
        with open(path) as f:
            data = json.load(f)
        node_names = [n["name"] for n in data["nodes"]]
        assert "Daily 09:00 CET (Mon-Fri)" in node_names
        assert "Get Unanswered Conversations" in node_names
        assert "Format Digest" in node_names
        assert "Send Digest to Telegram" in node_names

    def test_workflow_has_cron_schedule(self):
        path = os.path.join(
            os.path.dirname(__file__), "..", "..", "..",
            "[n8n] workflows", "n8n-daily-client-digest.json"
        )
        with open(path) as f:
            data = json.load(f)
        cron_node = [n for n in data["nodes"] if n["name"] == "Daily 09:00 CET (Mon-Fri)"][0]
        # Should have cron expression
        params = cron_node.get("parameters", {})
        assert "rule" in params


class TestMainLamaIntegration:
    """Test that main.py correctly integrates LABA_MODE."""

    def test_main_py_has_laba_mode_check(self):
        path = os.path.join(
            os.path.dirname(__file__), "..",
            "main.py"
        )
        with open(path) as f:
            content = f.read()
        assert 'LABA_MODE' in content
        assert 'register_event_handlers' in content


class TestSchemaPerProfile:
    """Schema-per-profile resolution (RCA 2026-06-05, owner directive).

    Каждый Telegram-аккаунт пишет в СВОЮ Supabase-схему — данные не смешиваются.
    """

    def _resolver(self):
        from heroes_platform.heroes_telegram_mcp.supabase_writer import _schema_for_profile
        return _schema_for_profile

    def test_ikrasinsky_keeps_legacy_schema(self, monkeypatch):
        monkeypatch.delenv("SUPABASE_TELEGRAM_SCHEMA", raising=False)
        assert self._resolver()("ikrasinsky") == "rick_messages_tasks"

    def test_ik_aliases_to_legacy_schema(self, monkeypatch):
        monkeypatch.delenv("SUPABASE_TELEGRAM_SCHEMA", raising=False)
        assert self._resolver()("ik") == "rick_messages_tasks"
        assert self._resolver()("ilyakrasinsky") == "rick_messages_tasks"

    def test_lisa_gets_own_schema(self, monkeypatch):
        monkeypatch.delenv("SUPABASE_TELEGRAM_SCHEMA", raising=False)
        assert self._resolver()("lisa") == "tg_lisa"

    def test_new_client_uses_tg_slug_convention(self, monkeypatch):
        monkeypatch.delenv("SUPABASE_TELEGRAM_SCHEMA", raising=False)
        assert self._resolver()("smokeway-co") == "tg_smokeway_co"
        assert self._resolver()("Typhoon Coffee") == "tg_typhoon_coffee"
        assert self._resolver()("vipavenue.ru") == "tg_vipavenue_ru"

    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv("SUPABASE_TELEGRAM_SCHEMA", "public")
        assert self._resolver()("lisa") == "public"
        assert self._resolver()("ikrasinsky") == "public"

    def test_empty_profile_raises_fail_fast(self, monkeypatch):
        monkeypatch.delenv("SUPABASE_TELEGRAM_SCHEMA", raising=False)
        with pytest.raises(ValueError):
            self._resolver()("   ")

    def test_writer_instance_schema_wired(self, monkeypatch):
        monkeypatch.delenv("SUPABASE_TELEGRAM_SCHEMA", raising=False)
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter
        assert SupabaseWriter(telegram_user_id="lisa").schema == "tg_lisa"
        assert SupabaseWriter(telegram_user_id="ikrasinsky").schema == "rick_messages_tasks"
