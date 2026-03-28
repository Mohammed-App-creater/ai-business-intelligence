"""
Static checks for infra/warehouse_schema.sql — no database connection required.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE_SQL_PATH = REPO_ROOT / "infra" / "warehouse_schema.sql"

ANALYTICS_TABLES = (
    "wh_monthly_revenue",
    "wh_daily_revenue",
    "wh_staff_performance",
    "wh_service_performance",
    "wh_client_metrics",
    "wh_appointment_metrics",
    "wh_expense_summary",
    "wh_review_summary",
    "wh_payment_breakdown",
    "wh_campaign_performance",
    "wh_attendance_summary",
    "wh_subscription_revenue",
)

ALL_WAREHOUSE_TABLES = ANALYTICS_TABLES + ("wh_etl_log",)


@pytest.fixture(scope="module")
def warehouse_sql() -> str:
    assert WAREHOUSE_SQL_PATH.is_file(), f"Missing schema file: {WAREHOUSE_SQL_PATH}"
    return WAREHOUSE_SQL_PATH.read_text(encoding="utf-8")


def _table_block(sql: str, table_name: str) -> str:
    marker = f"CREATE TABLE IF NOT EXISTS {table_name}"
    start = sql.find(marker)
    assert start != -1, f"Missing table: {table_name}"
    next_create = sql.find("CREATE TABLE IF NOT EXISTS ", start + 1)
    end = len(sql) if next_create == -1 else next_create
    return sql[start:end]


def test_all_thirteen_create_table_statements(warehouse_sql: str) -> None:
    found = re.findall(
        r"CREATE TABLE IF NOT EXISTS (wh_[a-z0-9_]+)\s*\(",
        warehouse_sql,
        flags=re.IGNORECASE,
    )
    wh_tables = [name for name in found if name.startswith("wh_")]
    assert len(wh_tables) == 13, f"Expected 13 wh_ tables, got {len(wh_tables)}: {wh_tables}"


def test_each_expected_table_name_present(warehouse_sql: str) -> None:
    for name in ALL_WAREHOUSE_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS {name}" in warehouse_sql


def test_no_mysql_artifacts(warehouse_sql: str) -> None:
    assert "AUTO_INCREMENT" not in warehouse_sql
    assert "ENGINE=InnoDB" not in warehouse_sql
    assert "`" not in warehouse_sql
    assert "tinyint" not in warehouse_sql.lower()
    assert "UNSIGNED" not in warehouse_sql


def test_postgresql_types_present(warehouse_sql: str) -> None:
    assert "BIGSERIAL" in warehouse_sql
    assert "TIMESTAMPTZ" in warehouse_sql
    assert "DECIMAL" in warehouse_sql


def test_analytics_tables_have_business_id(warehouse_sql: str) -> None:
    for table in ANALYTICS_TABLES:
        block = _table_block(warehouse_sql, table)
        assert re.search(r"\bbusiness_id\b", block), f"{table}: missing business_id"


def test_analytics_tables_have_updated_at(warehouse_sql: str) -> None:
    for table in ANALYTICS_TABLES:
        block = _table_block(warehouse_sql, table)
        assert re.search(r"\bupdated_at\b", block), f"{table}: missing updated_at"


def test_at_least_twelve_unique_constraints(warehouse_sql: str) -> None:
    assert warehouse_sql.count("UNIQUE") >= 12


def test_wh_etl_log_columns(warehouse_sql: str) -> None:
    block = _table_block(warehouse_sql, "wh_etl_log")
    for col in (
        "wh_etl_log",
        "run_id",
        "target_table",
        "status",
        "rows_inserted",
        "started_at",
    ):
        if col == "wh_etl_log":
            assert "CREATE TABLE IF NOT EXISTS wh_etl_log" in warehouse_sql
        else:
            assert re.search(rf"\b{re.escape(col)}\b", block), f"wh_etl_log: missing {col}"


def test_at_least_ten_create_index(warehouse_sql: str) -> None:
    assert warehouse_sql.count("CREATE INDEX") >= 10


def test_no_float_types_for_money(warehouse_sql: str) -> None:
    lower = warehouse_sql.lower()
    assert "float" not in lower
    assert "double precision" not in lower


def test_gen_random_uuid_used(warehouse_sql: str) -> None:
    assert "gen_random_uuid()" in warehouse_sql


def test_create_table_wh_count_is_exactly_thirteen(warehouse_sql: str) -> None:
    matches = re.findall(
        r"CREATE TABLE IF NOT EXISTS (wh_\w+)",
        warehouse_sql,
    )
    assert len(matches) == 13
