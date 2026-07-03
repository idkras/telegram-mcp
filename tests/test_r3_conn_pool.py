"""R3 D5 experiment (pr-hero-1u1): prove _pg_conn now uses a BOUNDED pool that reuses
connections instead of opening a fresh psycopg2.connect per call (the too-many-
connections crash that stalled backfill). Uses a fake pool that tracks concurrency."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import supabase_writer as sw  # noqa: E402


class _FakePool:
    instances = []

    def __init__(self, minc, maxc, dsn):
        self.minc, self.maxc, self.dsn = minc, maxc, dsn
        self.created = 0
        self.borrowed = 0
        self.max_concurrent = 0
        _FakePool.instances.append(self)

    def getconn(self):
        self.borrowed += 1
        self.created += 1  # fake: a real pool would reuse, but count borrows
        self.max_concurrent = max(self.max_concurrent, self.borrowed)
        return object()  # a fake conn (rollback swallowed by except)

    def putconn(self, conn):
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


if __name__ == "__main__":
    import subprocess
    subprocess.run([sys.executable, "-m", "pytest", __file__, "-v"])
