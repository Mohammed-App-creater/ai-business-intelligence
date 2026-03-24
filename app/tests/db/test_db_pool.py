"""
test_db_pool.py
===============
Tests for DBPool (MySQL) and PGVectorPool (PostgreSQL/pgvector).

Unit tests only — no real DB connection required.
Both aiomysql.create_pool and asyncpg.create_pool are mocked.
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# MySQL mock helpers
# ---------------------------------------------------------------------------

def make_mock_mysql_pool(size: int = 2, freesize: int = 2):
    pool          = MagicMock()
    pool.size     = size
    pool.freesize = freesize
    pool.close    = MagicMock()
    pool.wait_closed = AsyncMock()

    conn = MagicMock()
    pool.acquire = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__  = AsyncMock(return_value=False)

    return pool, conn


def _prod_env(monkeypatch, **overrides):
    defaults = {
        "PROD_DB_USER":     "prod_user",
        "PROD_DB_PASSWORD": "prod_pass",
        "PROD_DB_NAME":     "prod_db",
    }
    defaults.update(overrides)
    for k, v in defaults.items():
        monkeypatch.setenv(k, v)


def _wh_env(monkeypatch, **overrides):
    defaults = {
        "WH_DB_USER":     "wh_user",
        "WH_DB_PASSWORD": "wh_pass",
        "WH_DB_NAME":     "wh_db",
    }
    defaults.update(overrides)
    for k, v in defaults.items():
        monkeypatch.setenv(k, v)


# ---------------------------------------------------------------------------
# pgvector mock helpers
# ---------------------------------------------------------------------------

def make_mock_pg_pool(size: int = 1, idle: int = 1):
    pool = MagicMock()
    pool.get_size      = MagicMock(return_value=size)
    pool.get_idle_size = MagicMock(return_value=idle)
    pool.close         = AsyncMock()

    conn = MagicMock()
    pool.acquire = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__  = AsyncMock(return_value=False)

    return pool, conn


def _pg_env(monkeypatch, **overrides):
    defaults = {
        "PG_USER":     "pg_user",
        "PG_PASSWORD": "pg_pass",
        "PG_NAME":     "pg_db",
    }
    defaults.update(overrides)
    for k, v in defaults.items():
        monkeypatch.setenv(k, v)


# ===========================================================================
# DBTarget enum
# ===========================================================================

class TestDBTarget:

    def test_production_value(self):
        from app.services.db.db_pool import DBTarget
        assert DBTarget.PRODUCTION.value == "production"

    def test_warehouse_value(self):
        from app.services.db.db_pool import DBTarget
        assert DBTarget.WAREHOUSE.value == "warehouse"

    def test_two_targets_exist(self):
        from app.services.db.db_pool import DBTarget
        assert len(list(DBTarget)) == 2


# ===========================================================================
# DBPool — production
# ===========================================================================

class TestDBPoolProduction:

    async def test_missing_user_raises(self, monkeypatch):
        monkeypatch.delenv("PROD_DB_USER",     raising=False)
        monkeypatch.delenv("PROD_DB_PASSWORD", raising=False)
        monkeypatch.delenv("PROD_DB_NAME",     raising=False)
        from app.services.db.db_pool import DBPool, DBTarget
        with pytest.raises(KeyError):
            await DBPool.from_env(DBTarget.PRODUCTION)

    async def test_missing_password_raises(self, monkeypatch):
        monkeypatch.setenv("PROD_DB_USER", "u")
        monkeypatch.delenv("PROD_DB_PASSWORD", raising=False)
        monkeypatch.delenv("PROD_DB_NAME",     raising=False)
        from app.services.db.db_pool import DBPool, DBTarget
        with pytest.raises(KeyError):
            await DBPool.from_env(DBTarget.PRODUCTION)

    async def test_missing_name_raises(self, monkeypatch):
        monkeypatch.setenv("PROD_DB_USER",     "u")
        monkeypatch.setenv("PROD_DB_PASSWORD", "p")
        monkeypatch.delenv("PROD_DB_NAME",     raising=False)
        from app.services.db.db_pool import DBPool, DBTarget
        with pytest.raises(KeyError):
            await DBPool.from_env(DBTarget.PRODUCTION)

    async def test_reads_production_prefix(self, monkeypatch):
        _prod_env(monkeypatch,
                  PROD_DB_HOST="prodhost", PROD_DB_PORT="3308",
                  PROD_DB_POOL_MIN="3",    PROD_DB_POOL_MAX="12")
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.from_env(DBTarget.PRODUCTION)
            kw = m.call_args.kwargs
            assert kw["host"]     == "prodhost"
            assert kw["port"]     == 3308
            assert kw["user"]     == "prod_user"
            assert kw["password"] == "prod_pass"
            assert kw["db"]       == "prod_db"
            assert kw["minsize"]  == 3
            assert kw["maxsize"]  == 12

    async def test_default_host_localhost(self, monkeypatch):
        monkeypatch.delenv("PROD_DB_HOST", raising=False)
        _prod_env(monkeypatch)
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.from_env(DBTarget.PRODUCTION)
            assert m.call_args.kwargs["host"] == "localhost"

    async def test_default_pool_min_2(self, monkeypatch):
        monkeypatch.delenv("PROD_DB_POOL_MIN", raising=False)
        _prod_env(monkeypatch)
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.from_env(DBTarget.PRODUCTION)
            assert m.call_args.kwargs["minsize"] == 2

    async def test_default_pool_max_10(self, monkeypatch):
        monkeypatch.delenv("PROD_DB_POOL_MAX", raising=False)
        _prod_env(monkeypatch)
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.from_env(DBTarget.PRODUCTION)
            assert m.call_args.kwargs["maxsize"] == 10

    async def test_target_property_is_production(self, monkeypatch):
        _prod_env(monkeypatch)
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import DBPool, DBTarget
            pool = await DBPool.from_env(DBTarget.PRODUCTION)
            assert pool.target == DBTarget.PRODUCTION


# ===========================================================================
# DBPool — warehouse
# ===========================================================================

class TestDBPoolWarehouse:

    async def test_missing_user_raises(self, monkeypatch):
        monkeypatch.delenv("WH_DB_USER",     raising=False)
        monkeypatch.delenv("WH_DB_PASSWORD", raising=False)
        monkeypatch.delenv("WH_DB_NAME",     raising=False)
        from app.services.db.db_pool import DBPool, DBTarget
        with pytest.raises(KeyError):
            await DBPool.from_env(DBTarget.WAREHOUSE)

    async def test_reads_warehouse_prefix(self, monkeypatch):
        _wh_env(monkeypatch,
                WH_DB_HOST="whhost", WH_DB_PORT="3309",
                WH_DB_POOL_MIN="1",  WH_DB_POOL_MAX="4")
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.from_env(DBTarget.WAREHOUSE)
            kw = m.call_args.kwargs
            assert kw["host"]     == "whhost"
            assert kw["port"]     == 3309
            assert kw["user"]     == "wh_user"
            assert kw["password"] == "wh_pass"
            assert kw["db"]       == "wh_db"
            assert kw["minsize"]  == 1
            assert kw["maxsize"]  == 4

    async def test_default_pool_min_1(self, monkeypatch):
        monkeypatch.delenv("WH_DB_POOL_MIN", raising=False)
        _wh_env(monkeypatch)
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.from_env(DBTarget.WAREHOUSE)
            assert m.call_args.kwargs["minsize"] == 1

    async def test_default_pool_max_5(self, monkeypatch):
        monkeypatch.delenv("WH_DB_POOL_MAX", raising=False)
        _wh_env(monkeypatch)
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.from_env(DBTarget.WAREHOUSE)
            assert m.call_args.kwargs["maxsize"] == 5

    async def test_target_property_is_warehouse(self, monkeypatch):
        _wh_env(monkeypatch)
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import DBPool, DBTarget
            pool = await DBPool.from_env(DBTarget.WAREHOUSE)
            assert pool.target == DBTarget.WAREHOUSE

    async def test_prod_vars_do_not_leak_into_warehouse(self, monkeypatch):
        _prod_env(monkeypatch, PROD_DB_HOST="prodhost")
        _wh_env(monkeypatch,   WH_DB_HOST="whhost")
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.from_env(DBTarget.WAREHOUSE)
            assert m.call_args.kwargs["host"] == "whhost"

    async def test_wh_vars_do_not_leak_into_production(self, monkeypatch):
        _prod_env(monkeypatch, PROD_DB_HOST="prodhost")
        _wh_env(monkeypatch,   WH_DB_HOST="whhost")
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.from_env(DBTarget.PRODUCTION)
            assert m.call_args.kwargs["host"] == "prodhost"


# ===========================================================================
# DBPool — create() shared behaviour
# ===========================================================================

class TestDBPoolCreate:

    async def test_autocommit_is_true(self):
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.create(DBTarget.PRODUCTION, user="u", password="p", db="d")
            assert m.call_args.kwargs["autocommit"] is True

    async def test_dict_cursor_is_set(self):
        import aiomysql
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.create(DBTarget.PRODUCTION, user="u", password="p", db="d")
            assert m.call_args.kwargs["cursorclass"] == aiomysql.DictCursor

    async def test_charset_is_utf8mb4(self):
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.create(DBTarget.PRODUCTION, user="u", password="p", db="d")
            assert m.call_args.kwargs["charset"] == "utf8mb4"

    async def test_returns_dbpool_instance(self):
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import DBPool, DBTarget
            result = await DBPool.create(DBTarget.PRODUCTION, user="u", password="p", db="d")
            assert isinstance(result, DBPool)


# ===========================================================================
# DBPool — acquire() and close()
# ===========================================================================

class TestDBPoolAcquireClose:

    async def test_acquire_yields_connection(self):
        mock_pool, mock_conn = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import DBPool, DBTarget
            pool = await DBPool.create(DBTarget.PRODUCTION, user="u", password="p", db="d")
            async with pool.acquire() as conn:
                assert conn is mock_conn

    async def test_connection_released_on_exit(self):
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import DBPool, DBTarget
            pool = await DBPool.create(DBTarget.PRODUCTION, user="u", password="p", db="d")
            async with pool.acquire():
                pass
            mock_pool.acquire.return_value.__aexit__.assert_called_once()

    async def test_close_calls_pool_close_and_wait(self):
        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import DBPool, DBTarget
            pool = await DBPool.create(DBTarget.PRODUCTION, user="u", password="p", db="d")
            await pool.close()
            mock_pool.close.assert_called_once()
            mock_pool.wait_closed.assert_called_once()

    async def test_size_property(self):
        mock_pool, _ = make_mock_mysql_pool(size=7)
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import DBPool, DBTarget
            pool = await DBPool.create(DBTarget.PRODUCTION, user="u", password="p", db="d")
            assert pool.size == 7

    async def test_freesize_property(self):
        mock_pool, _ = make_mock_mysql_pool(freesize=4)
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import DBPool, DBTarget
            pool = await DBPool.create(DBTarget.PRODUCTION, user="u", password="p", db="d")
            assert pool.freesize == 4


# ===========================================================================
# PGVectorPool — from_env
# ===========================================================================

class TestPGVectorPoolFromEnv:

    async def test_missing_user_raises(self, monkeypatch):
        monkeypatch.delenv("PG_USER",     raising=False)
        monkeypatch.delenv("PG_PASSWORD", raising=False)
        monkeypatch.delenv("PG_NAME",     raising=False)
        from app.services.db.db_pool import PGVectorPool
        with pytest.raises(KeyError):
            await PGVectorPool.from_env()

    async def test_missing_password_raises(self, monkeypatch):
        monkeypatch.setenv("PG_USER", "u")
        monkeypatch.delenv("PG_PASSWORD", raising=False)
        monkeypatch.delenv("PG_NAME",     raising=False)
        from app.services.db.db_pool import PGVectorPool
        with pytest.raises(KeyError):
            await PGVectorPool.from_env()

    async def test_missing_name_raises(self, monkeypatch):
        monkeypatch.setenv("PG_USER",     "u")
        monkeypatch.setenv("PG_PASSWORD", "p")
        monkeypatch.delenv("PG_NAME",     raising=False)
        from app.services.db.db_pool import PGVectorPool
        with pytest.raises(KeyError):
            await PGVectorPool.from_env()

    async def test_reads_pg_env_vars(self, monkeypatch):
        _pg_env(monkeypatch,
                PG_HOST="pghost", PG_PORT="5433",
                PG_POOL_MIN="2",  PG_POOL_MAX="8")
        mock_pool, _ = make_mock_pg_pool()
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import PGVectorPool
            await PGVectorPool.from_env()
            kw = m.call_args.kwargs
            assert kw["host"]     == "pghost"
            assert kw["port"]     == 5433
            assert kw["user"]     == "pg_user"
            assert kw["password"] == "pg_pass"
            assert kw["database"] == "pg_db"
            assert kw["min_size"] == 2
            assert kw["max_size"] == 8

    async def test_default_host_localhost(self, monkeypatch):
        monkeypatch.delenv("PG_HOST", raising=False)
        _pg_env(monkeypatch)
        mock_pool, _ = make_mock_pg_pool()
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import PGVectorPool
            await PGVectorPool.from_env()
            assert m.call_args.kwargs["host"] == "localhost"

    async def test_default_port_5432(self, monkeypatch):
        monkeypatch.delenv("PG_PORT", raising=False)
        _pg_env(monkeypatch)
        mock_pool, _ = make_mock_pg_pool()
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import PGVectorPool
            await PGVectorPool.from_env()
            assert m.call_args.kwargs["port"] == 5432

    async def test_default_pool_min_1(self, monkeypatch):
        monkeypatch.delenv("PG_POOL_MIN", raising=False)
        _pg_env(monkeypatch)
        mock_pool, _ = make_mock_pg_pool()
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import PGVectorPool
            await PGVectorPool.from_env()
            assert m.call_args.kwargs["min_size"] == 1

    async def test_default_pool_max_5(self, monkeypatch):
        monkeypatch.delenv("PG_POOL_MAX", raising=False)
        _pg_env(monkeypatch)
        mock_pool, _ = make_mock_pg_pool()
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import PGVectorPool
            await PGVectorPool.from_env()
            assert m.call_args.kwargs["max_size"] == 5


# ===========================================================================
# PGVectorPool — create(), acquire(), close(), properties
# ===========================================================================

class TestPGVectorPoolCreate:

    async def test_returns_pgvectorpool_instance(self):
        mock_pool, _ = make_mock_pg_pool()
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import PGVectorPool
            result = await PGVectorPool.create(user="u", password="p", database="d")
            assert isinstance(result, PGVectorPool)

    async def test_passes_correct_params(self):
        mock_pool, _ = make_mock_pg_pool()
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import PGVectorPool
            await PGVectorPool.create(
                host="h", port=5433,
                user="u", password="p", database="d",
                min_size=2, max_size=8,
            )
            kw = m.call_args.kwargs
            assert kw["host"]     == "h"
            assert kw["port"]     == 5433
            assert kw["user"]     == "u"
            assert kw["database"] == "d"
            assert kw["min_size"] == 2
            assert kw["max_size"] == 8


class TestPGVectorPoolAcquireClose:

    async def test_acquire_yields_connection(self):
        mock_pool, mock_conn = make_mock_pg_pool()
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import PGVectorPool
            pool = await PGVectorPool.create(user="u", password="p", database="d")
            async with pool.acquire() as conn:
                assert conn is mock_conn

    async def test_connection_released_on_exit(self):
        mock_pool, _ = make_mock_pg_pool()
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import PGVectorPool
            pool = await PGVectorPool.create(user="u", password="p", database="d")
            async with pool.acquire():
                pass
            mock_pool.acquire.return_value.__aexit__.assert_called_once()

    async def test_close_awaits_pool_close(self):
        mock_pool, _ = make_mock_pg_pool()
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import PGVectorPool
            pool = await PGVectorPool.create(user="u", password="p", database="d")
            await pool.close()
            mock_pool.close.assert_called_once()

    async def test_size_property(self):
        mock_pool, _ = make_mock_pg_pool(size=3)
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import PGVectorPool
            pool = await PGVectorPool.create(user="u", password="p", database="d")
            assert pool.size == 3

    async def test_idle_property(self):
        mock_pool, _ = make_mock_pg_pool(idle=2)
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)):
            from app.services.db.db_pool import PGVectorPool
            pool = await PGVectorPool.create(user="u", password="p", database="d")
            assert pool.idle == 2


# ===========================================================================
# Isolation — MySQL and PG env vars don't cross-contaminate
# ===========================================================================

class TestEnvVarIsolation:

    async def test_pg_vars_do_not_affect_mysql_pool(self, monkeypatch):
        _prod_env(monkeypatch, PROD_DB_HOST="prodhost")
        _pg_env(monkeypatch,   PG_HOST="pghost")

        mock_pool, _ = make_mock_mysql_pool()
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import DBPool, DBTarget
            await DBPool.from_env(DBTarget.PRODUCTION)
            assert m.call_args.kwargs["host"] == "prodhost"

    async def test_mysql_vars_do_not_affect_pg_pool(self, monkeypatch):
        _prod_env(monkeypatch, PROD_DB_HOST="prodhost")
        _pg_env(monkeypatch,   PG_HOST="pghost")

        mock_pool, _ = make_mock_pg_pool()
        with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)) as m:
            from app.services.db.db_pool import PGVectorPool
            await PGVectorPool.from_env()
            assert m.call_args.kwargs["host"] == "pghost"