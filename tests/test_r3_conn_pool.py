"""R3 D5 experiment (pr-hero-1u1): prove _pg_conn now uses a BOUNDED pool that reuses
connections instead of opening a fresh psycopg2.connect per call (the too-many-
connections crash that stalled backfill). Uses a fake pool that tracks concurrency."""
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import supabase_writer as sw  # noqa: E402


class _FakeCursor:
    def execute(self, *_args, **_kwargs):
        return None

    def close(self):
        return None


class _FakeConn:
    closed = 0

    def cursor(self):
        return _FakeCursor()

    def commit(self):
        return None

    def rollback(self):
        return None


class _FakePool:
    instances = []

    def __init__(self, minc, maxc, dsn, **kwargs):
        self.minc, self.maxc, self.dsn = minc, maxc, dsn
        self.kwargs = kwargs
        self.created = 0
        self.borrowed = 0
        self.max_concurrent = 0
        _FakePool.instances.append(self)

    def getconn(self):
        self.borrowed += 1
        self.created += 1  # fake: a real pool would reuse, but count borrows
        self.max_concurrent = max(self.max_concurrent, self.borrowed)
        return _FakeConn()

    def putconn(self, conn, close=False):
        self.borrowed -= 1


def _writer():
    w = sw.SupabaseWriter.__new__(sw.SupabaseWriter)
    w._postgres_url = "postgresql://fake"
    w._pg_pool = None
    return w


def test_pool_created_once_with_bounds(monkeypatch):
    _FakePool.instances.clear()
    monkeypatch.setattr("psycopg2.pool.ThreadedConnectionPool", _FakePool, raising=False)
    monkeypatch.setenv("TELEGRAM_PG_POOL_MAX", "8")
    w = _writer()
    # 50 sequential borrows
    for _ in range(50):
        with w._pg_conn():
            pass
    assert len(_FakePool.instances) == 1              # ONE pool, not 50 connects
    pool = _FakePool.instances[0]
    assert pool.maxc == 8                              # bound honoured
    assert pool.max_concurrent == 1                    # sequential → never >1 borrowed
    assert pool.borrowed == 0                          # every conn returned (no leak)


def test_conn_returned_to_pool_even_on_error(monkeypatch):
    _FakePool.instances.clear()
    monkeypatch.setattr("psycopg2.pool.ThreadedConnectionPool", _FakePool, raising=False)
    w = _writer()
    try:
        with w._pg_conn():
            raise ValueError("boom inside txn")
    except ValueError:
        pass
    pool = _FakePool.instances[0]
    assert pool.borrowed == 0   # returned to pool despite the exception (no leak)


def test_concurrent_borrowers_wait_instead_of_exhausting_pool(monkeypatch):
    _FakePool.instances.clear()
    monkeypatch.setattr("psycopg2.pool.ThreadedConnectionPool", _FakePool, raising=False)
    monkeypatch.setenv("TELEGRAM_PG_POOL_MAX", "1")
    monkeypatch.setenv("TELEGRAM_PG_ACQUIRE_TIMEOUT_SECONDS", "2")
    w = _writer()
    entered = threading.Event()
    release = threading.Event()
    outcomes = []

    def first():
        with w._pg_conn():
            entered.set()
            release.wait(1)

    def second():
        entered.wait(1)
        try:
            with w._pg_conn():
                outcomes.append("borrowed")
        except Exception as exc:  # pragma: no cover - assertion reports exact defect
            outcomes.append(type(exc).__name__)

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    t2.start()
    entered.wait(1)
    time.sleep(0.05)
    release.set()
    t1.join(1)
    t2.join(1)

    assert outcomes == ["borrowed"]
    assert _FakePool.instances[0].max_concurrent == 1
    assert w.get_db_load_snapshot()["waits_total"] >= 1


def test_broken_connection_is_discarded(monkeypatch):
    class BrokenConn:
        closed = 1

        def rollback(self):
            raise RuntimeError("socket gone")

    class BrokenPool(_FakePool):
        def __init__(self, minc, maxc, dsn, **kwargs):
            super().__init__(minc, maxc, dsn, **kwargs)
            self.closed_returns = 0
            self.borrow_count = 0

        def getconn(self):
            self.borrowed += 1
            self.borrow_count += 1
            return BrokenConn() if self.borrow_count == 1 else _FakeConn()

        def putconn(self, conn, close=False):
            self.borrowed -= 1
            self.closed_returns += int(close)

    monkeypatch.setattr("psycopg2.pool.ThreadedConnectionPool", BrokenPool, raising=False)
    w = _writer()
    with w._pg_conn():
        pass

    assert BrokenPool.instances[-1].closed_returns == 1
    assert w.get_db_load_snapshot()["discarded_connections_total"] == 1


if __name__ == "__main__":
    import subprocess
    subprocess.run([sys.executable, "-m", "pytest", __file__, "-v"])
