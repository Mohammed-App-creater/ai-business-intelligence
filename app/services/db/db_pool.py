"""
db_pool.py
==========
Connection pools for production MySQL and PostgreSQL (warehouse + vector).

  DBPool        — MySQL pool (aiomysql)
    DBTarget.PRODUCTION  : raw SaaS data    — query modules READ here

  PGPool        — PostgreSQL pools (asyncpg)
    PGTarget.WAREHOUSE   : computed analytics — ETL WRITES, doc generator + agents READ
    PGTarget.VECTOR      : vector embeddings  — ETL WRITES, retriever READS

Two separate classes because the drivers (aiomysql vs asyncpg) have
completely different pool APIs — forcing them into one class would
make both messy.

Usage
-----
    # At startup:
    prod_pool = await DBPool.from_env(DBTarget.PRODUCTION)
    wh_pool   = await PGPool.from_env(PGTarget.WAREHOUSE)
    vec_pool  = await PGPool.from_env(PGTarget.VECTOR)

    # Query module (read from prod):
    async with prod_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

    # Warehouse (asyncpg):
    async with wh_pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
        await conn.execute(insert_sql, *params)

    # Vector (asyncpg):
    async with vec_pool.acquire() as conn:
        await conn.execute(sql, *params)
        rows = await conn.fetch(sql, *params)

Environment variables
---------------------
Production DB (MySQL, read-only):
  PROD_DB_HOST              default localhost
  PROD_DB_PORT              default 3306
  PROD_DB_USER              required
  PROD_DB_PASSWORD          required
  PROD_DB_NAME              required
  PROD_DB_POOL_MIN          default 2
  PROD_DB_POOL_MAX          default 10
  PROD_DB_CONNECT_TIMEOUT   default 10

Warehouse PostgreSQL:
  WH_PG_HOST              default localhost
  WH_PG_PORT              default 5432
  WH_PG_USER              required
  WH_PG_PASSWORD          required
  WH_PG_NAME              required
  WH_PG_POOL_MIN          default 1
  WH_PG_POOL_MAX          default 5
  WH_PG_CONNECT_TIMEOUT   default 10

Vector PostgreSQL (pgvector):
  VEC_PG_HOST             default localhost
  VEC_PG_PORT             default 5432
  VEC_PG_USER             required
  VEC_PG_PASSWORD         required
  VEC_PG_NAME             required
  VEC_PG_POOL_MIN         default 1
  VEC_PG_POOL_MAX         default 5
  VEC_PG_CONNECT_TIMEOUT  default 10
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from enum import Enum
from typing import AsyncGenerator

import aiomysql
import asyncpg

logger = logging.getLogger(__name__)


# ===========================================================================
# MySQL — Production only
# ===========================================================================

class DBTarget(str, Enum):
    PRODUCTION = "production"   # raw SaaS data   — read only


_ENV_PREFIX: dict[DBTarget, str] = {
    DBTarget.PRODUCTION: "PROD_DB_",
}

_DEFAULT_POOL_MIN: dict[DBTarget, int] = {
    DBTarget.PRODUCTION: 2,
}
_DEFAULT_POOL_MAX: dict[DBTarget, int] = {
    DBTarget.PRODUCTION: 10,
}


class DBPool:
    """
    MySQL connection pool for production DB.

    Always instantiate via from_env(target) or create(target, ...).
    """

    def __init__(self, pool: aiomysql.Pool, target: DBTarget) -> None:
        self._pool   = pool
        self._target = target

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    async def from_env(cls, target: DBTarget) -> "DBPool":
        """Build from environment variables for the given target."""
        prefix = _ENV_PREFIX[target]

        return await cls.create(
            target          = target,
            host            = os.getenv(f"{prefix}HOST",     "localhost"),
            port            = int(os.getenv(f"{prefix}PORT", "3306")),
            user            = os.environ[f"{prefix}USER"],
            password        = os.environ[f"{prefix}PASSWORD"],
            db              = os.environ[f"{prefix}NAME"],
            minsize         = int(os.getenv(
                f"{prefix}POOL_MIN", str(_DEFAULT_POOL_MIN[target]))),
            maxsize         = int(os.getenv(
                f"{prefix}POOL_MAX", str(_DEFAULT_POOL_MAX[target]))),
            connect_timeout = int(os.getenv(f"{prefix}CONNECT_TIMEOUT", "10")),
        )

    @classmethod
    async def create(
        cls,
        target:          DBTarget,
        host:            str = "localhost",
        port:            int = 3306,
        user:            str = "",
        password:        str = "",
        db:              str = "",
        minsize:         int = 2,
        maxsize:         int = 10,
        connect_timeout: int = 10,
    ) -> "DBPool":
        """Build with explicit parameters — useful for tests."""
        pool = await aiomysql.create_pool(
            host            = host,
            port            = port,
            user            = user,
            password        = password,
            db              = db,
            minsize         = minsize,
            maxsize         = maxsize,
            connect_timeout = connect_timeout,
            autocommit      = True,
            charset         = "utf8mb4",
            cursorclass     = aiomysql.DictCursor,
        )

        logger.info(
            "db_pool.created target=%s host=%s port=%d db=%s min=%d max=%d",
            target.value, host, port, db, minsize, maxsize,
        )

        return cls(pool, target)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[aiomysql.Connection, None]:
        """Borrow a connection from the pool."""
        async with self._pool.acquire() as conn:
            yield conn

    async def close(self) -> None:
        """Close all connections gracefully. Call on shutdown."""
        self._pool.close()
        await self._pool.wait_closed()
        logger.info("db_pool.closed target=%s", self._target.value)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def target(self) -> DBTarget:
        return self._target

    @property
    def size(self) -> int:
        return self._pool.size

    @property
    def freesize(self) -> int:
        return self._pool.freesize


# ===========================================================================
# PostgreSQL — Warehouse + Vector (asyncpg)
# ===========================================================================

class PGTarget(str, Enum):
    WAREHOUSE = "warehouse"   # analytics warehouse
    VECTOR    = "vector"      # pgvector embeddings


_PG_ENV_PREFIX: dict[PGTarget, str] = {
    PGTarget.WAREHOUSE: "WH_PG_",
    PGTarget.VECTOR:  "VEC_PG_",
}

_DEFAULT_PG_POOL_MIN: dict[PGTarget, int] = {
    PGTarget.WAREHOUSE: 1,
    PGTarget.VECTOR:    1,
}
_DEFAULT_PG_POOL_MAX: dict[PGTarget, int] = {
    PGTarget.WAREHOUSE: 5,
    PGTarget.VECTOR:    5,
}


class PGPool:
    """
    PostgreSQL connection pool for warehouse or vector database.

    Uses asyncpg — different driver from MySQL pools.

    asyncpg connections work differently from aiomysql:
      - No cursor() — execute queries directly on the connection
      - conn.execute(sql, *args)   for INSERT/UPDATE
      - conn.fetch(sql, *args)     for SELECT returning rows as Records
      - conn.fetchrow(sql, *args)  for SELECT returning a single Record
      - conn.fetchval(sql, *args)  for SELECT returning a single value
    """

    def __init__(self, pool: asyncpg.Pool, target: PGTarget) -> None:
        self._pool   = pool
        self._target = target

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    async def from_env(cls, target: PGTarget) -> "PGPool":
        """Build from WH_PG_* or VEC_PG_* environment variables."""
        prefix = _PG_ENV_PREFIX[target]

        return await cls.create(
            target          = target,
            host            = os.getenv(f"{prefix}HOST",     "localhost"),
            port            = int(os.getenv(f"{prefix}PORT", "5432")),
            user            = os.environ[f"{prefix}USER"],
            password        = os.environ[f"{prefix}PASSWORD"],
            database        = os.environ[f"{prefix}NAME"],
            min_size        = int(os.getenv(
                f"{prefix}POOL_MIN", str(_DEFAULT_PG_POOL_MIN[target]))),
            max_size        = int(os.getenv(
                f"{prefix}POOL_MAX", str(_DEFAULT_PG_POOL_MAX[target]))),
            command_timeout = int(os.getenv(f"{prefix}CONNECT_TIMEOUT", "10")),
        )

    @classmethod
    async def create(
        cls,
        target:          PGTarget,
        host:            str = "localhost",
        port:            int = 5432,
        user:            str = "",
        password:        str = "",
        database:        str = "",
        min_size:        int = 1,
        max_size:        int = 5,
        command_timeout: int = 10,
    ) -> "PGPool":
        """Build with explicit parameters — useful for tests."""
        pool = await asyncpg.create_pool(
            host            = host,
            port            = port,
            user            = user,
            password        = password,
            database        = database,
            min_size        = min_size,
            max_size        = max_size,
            command_timeout = command_timeout,
        )

        logger.info(
            "pg_pool.created target=%s host=%s port=%d db=%s min=%d max=%d",
            target.value, host, port, database, min_size, max_size,
        )

        return cls(pool, target)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """
        Borrow a connection from the pool.

        Usage:
            async with pg_pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
                await conn.execute(insert_sql, *params)
        """
        async with self._pool.acquire() as conn:
            yield conn

    async def close(self) -> None:
        """Close all connections gracefully. Call on shutdown."""
        await self._pool.close()
        logger.info("pg_pool.closed target=%s", self._target.value)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def target(self) -> PGTarget:
        return self._target

    @property
    def size(self) -> int:
        return self._pool.get_size()

    @property
    def idle(self) -> int:
        """Number of idle connections available right now."""
        return self._pool.get_idle_size()


# Backward compatibility for imports: PGVectorPool = PGPool
PGVectorPool = PGPool
