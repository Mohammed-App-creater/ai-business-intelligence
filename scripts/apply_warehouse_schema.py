"""
Apply infra/warehouse_schema.sql to the analytics warehouse (PostgreSQL).

Uses WH_PG_* environment variables.
Does not require the psql CLI — only psycopg2-binary + sqlparse.

Usage:
    python scripts/apply_warehouse_schema.py
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
import psycopg2
import sqlparse

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DOTENV_PATH = _REPO_ROOT / ".env"
load_dotenv(_DOTENV_PATH, override=True)

_DEBUG_LOG = _REPO_ROOT / "debug-029353.log"
_SQL_PATH = _REPO_ROOT / "infra" / "warehouse_schema.sql"
_WH_PREFIX = "WH_PG_"


def _agent_dbg(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    try:
        payload = {
            "sessionId": "029353",
            "runId": os.environ.get("DEBUG_RUN_ID", "pre-fix"),
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with _DEBUG_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
    except OSError:
        pass


_agent_dbg(
    "H1-H2",
    "apply_warehouse_schema.py:after_load_dotenv",
    "dotenv path and WH_PG_* presence",
    {
        "repo_root": str(_REPO_ROOT),
        "dotenv_path": str(_DOTENV_PATH),
        "dotenv_exists": _DOTENV_PATH.is_file(),
        "dotenv_size_bytes": _DOTENV_PATH.stat().st_size if _DOTENV_PATH.is_file() else None,
        "has_WH_PG_USER": "WH_PG_USER" in os.environ,
        "has_WH_PG_PASSWORD": "WH_PG_PASSWORD" in os.environ,
        "has_WH_PG_NAME": "WH_PG_NAME" in os.environ,
        "has_WH_PG_HOST": "WH_PG_HOST" in os.environ,
        "has_WH_PG_PORT": "WH_PG_PORT" in os.environ,
        "wh_pg_keys_in_environ": sorted(k for k in os.environ if k.startswith("WH_PG_")),
    },
)


def main() -> int:
    if not _SQL_PATH.is_file():
        print(f"Missing schema file: {_SQL_PATH}", file=sys.stderr)
        return 1

    try:
        host = os.getenv(f"{_WH_PREFIX}HOST", "localhost")
        port = int(os.getenv(f"{_WH_PREFIX}PORT", "5432"))
        user = os.environ[f"{_WH_PREFIX}USER"]
        password = os.environ[f"{_WH_PREFIX}PASSWORD"]
        dbname = os.environ[f"{_WH_PREFIX}NAME"]

        print(
            {
                "host": host,
                "port": port,
                "user": user,
                "dbname": dbname,
                "pwd_exists": bool(password),
            }
        )

        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            sslmode="require",
        )

    except KeyError as e:
        _agent_dbg(
            "H3",
            "apply_warehouse_schema.py:KeyError",
            "missing env key for psycopg2.connect",
            {"missing_key_repr": repr(e.args[0]) if e.args else None},
        )
        print(
            "Missing required env var. Set WH_PG_USER, WH_PG_PASSWORD, WH_PG_NAME "
            f"(and optionally WH_PG_HOST, WH_PG_PORT). ({e})",
            file=sys.stderr,
        )
        return 1

    except psycopg2.Error as e:
        _agent_dbg(
            "H4",
            "apply_warehouse_schema.py:connect_error",
            "psycopg2 connection failed",
            {"error": str(e)},
        )
        print(f"PostgreSQL connection failed: {e}", file=sys.stderr)
        return 1

    conn.autocommit = True
    sql_text = _SQL_PATH.read_text(encoding="utf-8")

    try:
        with conn.cursor() as cur:
            for stmt in sqlparse.parse(sql_text):
                s = str(stmt).strip()
                if not s:
                    continue
                cur.execute(s)
    except psycopg2.Error as e:
        print(f"Schema apply failed: {e}", file=sys.stderr)
        conn.close()
        return 1

    conn.close()
    print(f"Applied warehouse schema: {_SQL_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())