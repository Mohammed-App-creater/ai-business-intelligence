"""
db_pool.py
==========
Connection pools for all three databases in the system.

  DBPool        — MySQL pools (aiomysql)
    DBTarget.PRODUCTION  : raw SaaS data    — query modules READ here
    DBTarget.WAREHOUSE   : computed data    — ETL pipeline WRITES here

  PGVectorPool  — PostgreSQL pool (asyncpg)
    pgvector DB          : vector embeddings — ETL WRITES, retriever READS

Two separate classes because the drivers (aiomysql vs asyncpg) have
completely different pool APIs — forcing them into one class would
make both messy.

Usage
-----
    # At startup:
    prod_pool = await DBPool.from_env(DBTarget.PRODUCTION)
    wh_pool   = await DBPool.from_env(DBTarget.WAREHOUSE)
    pg_pool   = await PGVectorPool.from_env()

    # Query module (read from prod):
    async with prod_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

    # ETL write to warehouse:
    async with wh_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(insert_sql, row)

    # pgvector read/write:
    async with pg_pool.acquire() as conn:
        await conn.execute(sql, *params)
        rows = await conn.fetch(sql, *params)

Environment variables
---------------------
Production DB (read-only):
  PROD_DB_HOST              default localhost
  PROD_DB_PORT              default 3306
  PROD_DB_USER              required
  PROD_DB_PASSWORD          required
  PROD_DB_NAME              required
  PROD_DB_POOL_MIN          default 2
  PROD_DB_POOL_MAX          default 10
  PROD_DB_CONNECT_TIMEOUT   default 10

Warehouse DB (ETL writes):
  WH_DB_HOST                default localhost
  WH_DB_PORT                default 3306
  WH_DB_USER                required
  WH_DB_PASSWORD            required
  WH_DB_NAME                required
  WH_DB_POOL_MIN            default 1
  WH_DB_POOL_MAX            default 5
  WH_DB_CONNECT_TIMEOUT     default 10

pgvector (asyncpg):
  PG_HOST                   default localhost
  PG_PORT                   default 5432
  PG_USER                   required
  PG_PASSWORD               required
  PG_NAME                   required
  PG_POOL_MIN               default 1
  PG_POOL_MAX               default 5
  PG_CONNECT_TIMEOUT        default 10
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
# MySQL — Production + Warehouse
# ===========================================================================

class DBTarget(str, Enum):
    PRODUCTION = "production"   # raw SaaS data   — read only
    WAREHOUSE  = "warehouse"    # computed data   — ETL writes here


_ENV_PREFIX: dict[DBTarget, str] = {
    DBTarget.PRODUCTION: "PROD_DB_",
    DBTarget.WAREHOUSE:  "WH_DB_",
}

_DEFAULT_POOL_MIN: dict[DBTarget, int] = {
    DBTarget.PRODUCTION: 2,
    DBTarget.WAREHOUSE:  1,
}
_DEFAULT_POOL_MAX: dict[DBTarget, int] = {
    DBTarget.PRODUCTION: 10,
    DBTarget.WAREHOUSE:  5,
}


class DBPool:
    """
    MySQL connection pool for production or warehouse DB.

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
# PostgreSQL — pgvector
# ===========================================================================

class PGVectorPool:
    """
    PostgreSQL connection pool for the pgvector database.

    Uses asyncpg — different driver from MySQL pools.
    ETL pipeline writes embeddings here; retriever reads them.

    asyncpg connections work differently from aiomysql:
      - No cursor() — execute queries directly on the connection
      - conn.execute(sql, *args)   for INSERT/UPDATE
      - conn.fetch(sql, *args)     for SELECT returning rows as Records
      - conn.fetchrow(sql, *args)  for SELECT returning a single Record
      - conn.fetchval(sql, *args)  for SELECT returning a single value
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    async def from_env(cls) -> "PGVectorPool":
        """Build from PG_* environment variables."""
        return await cls.create(
            host            = os.getenv("PG_HOST",     "localhost"),
            port            = int(os.getenv("PG_PORT", "5432")),
            user            = os.environ["PG_USER"],
            password        = os.environ["PG_PASSWORD"],
            database        = os.environ["PG_NAME"],
            min_size        = int(os.getenv("PG_POOL_MIN",         "1")),
            max_size        = int(os.getenv("PG_POOL_MAX",         "5")),
            command_timeout = int(os.getenv("PG_CONNECT_TIMEOUT",  "10")),
        )

    @classmethod
    async def create(
        cls,
        host:            str = "localhost",
        port:            int = 5432,
        user:            str = "",
        password:        str = "",
        database:        str = "",
        min_size:        int = 1,
        max_size:        int = 5,
        command_timeout: int = 10,
    ) -> "PGVectorPool":
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
            "pg_pool.created host=%s port=%d db=%s min=%d max=%d",
            host, port, database, min_size, max_size,
        )

        return cls(pool)

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
        logger.info("pg_pool.closed")

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        return self._pool.get_size()

    @property
    def idle(self) -> int:
        """Number of idle connections available right now."""
        return self._pool.get_idle_size()