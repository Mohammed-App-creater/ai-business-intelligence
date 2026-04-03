"""
Apply infra/warehouse_schema.sql to the analytics warehouse (PostgreSQL).

Uses WH_PG_* environment variables (same as app.services.db.db_pool.PGTarget.WAREHOUSE).
Does not require the psql CLI — only psycopg2 + sqlparse (see requirements.txt).

Usage (from repo root, with .env or exported WH_PG_*):
    python scripts/apply_warehouse_schema.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
import sqlparse
from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_REPO_ROOT / ".env", override=True)

_SQL_PATH = _REPO_ROOT / "infra" / "warehouse_schema.sql"
_WH_PREFIX = "WH_PG_"


def main() -> int:
    if not _SQL_PATH.is_file():
        print(f"Missing schema file: {_SQL_PATH}", file=sys.stderr)
        return 1

    try:
        conn = psycopg2.connect(
            host=os.getenv(f"{_WH_PREFIX}HOST", "localhost"),
            port=int(os.getenv(f"{_WH_PREFIX}PORT", "5432")),
            user=os.environ[f"{_WH_PREFIX}USER"],
            password=os.environ[f"{_WH_PREFIX}PASSWORD"],
            dbname=os.environ[f"{_WH_PREFIX}NAME"],
        )
    except KeyError as e:
        print(
            "Missing required env var. Set WH_PG_USER, WH_PG_PASSWORD, WH_PG_NAME "
            f"(and optionally WH_PG_HOST, WH_PG_PORT). ({e})",
            file=sys.stderr,
        )
        return 1

    conn.autocommit = True
    sql_text = _SQL_PATH.read_text(encoding="utf-8")

    with conn.cursor() as cur:
        for stmt in sqlparse.parse(sql_text):
            s = str(stmt).strip()
            if not s:
                continue
            cur.execute(s)

    conn.close()
    print(f"Applied warehouse schema: {_SQL_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
