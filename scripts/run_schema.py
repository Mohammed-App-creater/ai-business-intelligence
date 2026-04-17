# scripts/run_schema.py
"""
Run a .sql file against the warehouse or vector database using the
existing PGPool infrastructure. No psql client needed.

Usage:
    PYTHONPATH=. python scripts/run_schema.py infra/warehouse_schema_staff_append.sql warehouse
    PYTHONPATH=. python scripts/run_schema.py infra/init_db.sql vector
"""
import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv
from app.services.db.db_pool import PGPool, PGTarget

load_dotenv()


def split_sql(sql_text: str) -> list[str]:
    """
    Split a SQL file into individual statements. Handles semicolons inside
    single-line comments and empty statements.
    """
    # Strip line comments (-- ...) before splitting
    lines = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        lines.append(line)
    cleaned = "\n".join(lines)
    return [s.strip() for s in cleaned.split(";") if s.strip()]


async def run(sql_path: str, target_name: str) -> None:
    target = {
        "warehouse": PGTarget.WAREHOUSE,
        "vector":    PGTarget.VECTOR,
    }.get(target_name)
    if target is None:
        print(f"Unknown target: {target_name!r}. Use 'warehouse' or 'vector'.")
        sys.exit(1)

    sql_file = Path(sql_path)
    if not sql_file.exists():
        print(f"File not found: {sql_path}")
        sys.exit(1)

    sql_text   = sql_file.read_text(encoding="utf-8")
    statements = split_sql(sql_text)
    print(f"Target: {target_name}  |  File: {sql_path}  |  Statements: {len(statements)}\n")

    pool = await PGPool.from_env(target)
    try:
        async with pool.acquire() as conn:
            for i, stmt in enumerate(statements, 1):
                first = stmt.splitlines()[0][:80]
                try:
                    await conn.execute(stmt)
                    print(f"  [{i:>2}/{len(statements)}] OK   {first}")
                except Exception as exc:
                    print(f"  [{i:>2}/{len(statements)}] FAIL {first}")
                    print(f"       Error: {exc}")
                    raise
    finally:
        await pool.close()

    print(f"\nDone — {len(statements)} statements executed against {target_name}.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python scripts/run_schema.py <sql_file> <warehouse|vector>")
        sys.exit(1)
    asyncio.run(run(sys.argv[1], sys.argv[2]))