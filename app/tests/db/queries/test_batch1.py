"""
Tests for db/queries batch 1: revenue.py, expenses.py, services.py

Strategy
--------
- All DB interaction is mocked via AsyncMock so no real connection is needed.
- We test: correct SQL parameterisation, correct dataclass mapping, computed
  fields (cancellation_rate, avg_ticket, avg_price), edge cases (zero rows,
  zero appointments, zero cancellations), and convenience helpers.
"""
from __future__ import annotations

import sys
import types
from contextlib import asynccontextmanager
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

# ---------------------------------------------------------------------------
# Minimal stubs so imports work without the full project installed
# ---------------------------------------------------------------------------

def _make_db_stub() -> None:
    """Create a minimal app.services.db.db_pool stub."""
    app_mod = types.ModuleType("app")
    services_mod = types.ModuleType("app.services")
    db_mod = types.ModuleType("app.services.db")
    pool_mod = types.ModuleType("app.services.db.db_pool")

    class DBTarget:
        PRODUCTION = "production"
        WAREHOUSE = "warehouse"

    class DBPool:
        pass

    pool_mod.DBTarget = DBTarget
    pool_mod.DBPool = DBPool

    sys.modules.setdefault("app", app_mod)
    sys.modules.setdefault("app.services", services_mod)
    sys.modules.setdefault("app.services.db", db_mod)
    sys.modules.setdefault("app.services.db.db_pool", pool_mod)


_make_db_stub()

# Now safe to import the modules under test
from db_queries.revenue import get_revenue, RevenueRow, RevenueResult  # noqa: E402
from db_queries.expenses import get_expenses, ExpenseRow, ExpenseSummaryRow, ExpenseResult  # noqa: E402
from db_queries.services import get_services, ServiceRow, ServiceResult  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_pool(rows: list[dict]):
    """Return a DBPool mock whose cursor().fetchall() yields *rows*."""
    cur = AsyncMock()
    cur.execute = AsyncMock()
    cur.fetchall = AsyncMock(return_value=rows)
    cur.__aenter__ = AsyncMock(return_value=cur)
    cur.__aexit__ = AsyncMock(return_value=False)

    conn = AsyncMock()
    conn.cursor = MagicMock(return_value=cur)

    pool = MagicMock()

    @asynccontextmanager
    async def acquire(_target):
        yield conn

    pool.acquire = acquire
    return pool, cur


# ---------------------------------------------------------------------------
# revenue.py
# ---------------------------------------------------------------------------

class TestGetRevenue:
    @pytest.mark.asyncio
    async def test_basic_mapping(self):
        raw = [
            {
                "month": date(2026, 2, 1),
                "revenue": "13100.00",
                "appointments": 230,
                "cancellations": 14,
            },
            {
                "month": date(2026, 3, 1),
                "revenue": "9200.00",
                "appointments": 150,
                "cancellations": 27,
            },
        ]
        pool, cur = _mock_pool(raw)
        result = await get_revenue(pool, "salon_123", months=6)

        assert isinstance(result, RevenueResult)
        assert result.business_id == "salon_123"
        assert len(result.rows) == 2

        feb = result.rows[0]
        assert feb.month == date(2026, 2, 1)
        assert feb.revenue == Decimal("13100.00")
        assert feb.appointments == 230
        assert feb.cancellations == 14
        assert feb.cancellation_rate == pytest.approx(round(14 / 230, 4), abs=1e-9)
        # avg_ticket = revenue / completed = 13100 / 216
        assert feb.avg_ticket == (Decimal("13100") / 216).quantize(Decimal("0.01"))

    @pytest.mark.asyncio
    async def test_sql_params(self):
        pool, cur = _mock_pool([])
        await get_revenue(pool, "biz_42", months=3)
        cur.execute.assert_awaited_once()
        args = cur.execute.call_args[0]
        assert args[1] == ("biz_42", 3)

    @pytest.mark.asyncio
    async def test_empty_result(self):
        pool, _ = _mock_pool([])
        result = await get_revenue(pool, "biz_x")
        assert result.rows == []
        assert result.latest is None
        assert result.previous is None

    @pytest.mark.asyncio
    async def test_latest_and_previous(self):
        raw = [
            {"month": date(2026, 1, 1), "revenue": "10000", "appointments": 100, "cancellations": 5},
            {"month": date(2026, 2, 1), "revenue": "11000", "appointments": 110, "cancellations": 6},
            {"month": date(2026, 3, 1), "revenue": "9000",  "appointments": 90,  "cancellations": 20},
        ]
        pool, _ = _mock_pool(raw)
        result = await get_revenue(pool, "biz_y")
        assert result.latest.month == date(2026, 3, 1)
        assert result.previous.month == date(2026, 2, 1)

    @pytest.mark.asyncio
    async def test_zero_appointments_no_division_error(self):
        raw = [{"month": date(2026, 3, 1), "revenue": "0", "appointments": 0, "cancellations": 0}]
        pool, _ = _mock_pool(raw)
        result = await get_revenue(pool, "biz_z")
        row = result.rows[0]
        assert row.cancellation_rate == 0.0
        assert row.avg_ticket == Decimal("0")

    @pytest.mark.asyncio
    async def test_zero_cancellations(self):
        raw = [{"month": date(2026, 3, 1), "revenue": "5000", "appointments": 50, "cancellations": 0}]
        pool, _ = _mock_pool(raw)
        result = await get_revenue(pool, "biz_z")
        assert result.rows[0].cancellation_rate == 0.0

    @pytest.mark.asyncio
    async def test_month_string_parsed(self):
        """month column may come back as a string from some MySQL drivers."""
        raw = [{"month": "2026-03-01", "revenue": "9200", "appointments": 150, "cancellations": 27}]
        pool, _ = _mock_pool(raw)
        result = await get_revenue(pool, "biz_str")
        assert result.rows[0].month == date(2026, 3, 1)

    @pytest.mark.asyncio
    async def test_default_months_param(self):
        pool, cur = _mock_pool([])
        await get_revenue(pool, "biz_def")
        args = cur.execute.call_args[0]
        assert args[1][1] == 6   # default months=6


# ---------------------------------------------------------------------------
# expenses.py
# ---------------------------------------------------------------------------

class TestGetExpenses:
    @pytest.mark.asyncio
    async def test_basic_mapping(self):
        raw = [
            {"month": date(2026, 3, 1), "category": "payroll",   "total": "4000"},
            {"month": date(2026, 3, 1), "category": "rent",      "total": "1500"},
            {"month": date(2026, 3, 1), "category": "supplies",  "total": "300"},
            {"month": date(2026, 2, 1), "category": "payroll",   "total": "3800"},
            {"month": date(2026, 2, 1), "category": "rent",      "total": "1500"},
        ]
        pool, _ = _mock_pool(raw)
        result = await get_expenses(pool, "salon_123")

        assert isinstance(result, ExpenseResult)
        assert len(result.rows) == 5

        # Summaries: 2 months
        assert len(result.summaries) == 2

    @pytest.mark.asyncio
    async def test_summary_totals(self):
        raw = [
            {"month": date(2026, 3, 1), "category": "payroll",  "total": "4000"},
            {"month": date(2026, 3, 1), "category": "rent",     "total": "1500"},
        ]
        pool, _ = _mock_pool(raw)
        result = await get_expenses(pool, "biz_t")
        s = result.latest_summary
        assert s.total_expenses == Decimal("5500")
        assert s.breakdown["payroll"] == Decimal("4000")
        assert s.breakdown["rent"] == Decimal("1500")

    @pytest.mark.asyncio
    async def test_sql_params(self):
        pool, cur = _mock_pool([])
        await get_expenses(pool, "biz_42", months=3)
        args = cur.execute.call_args[0]
        assert args[1] == ("biz_42", 3)

    @pytest.mark.asyncio
    async def test_empty_result(self):
        pool, _ = _mock_pool([])
        result = await get_expenses(pool, "biz_x")
        assert result.rows == []
        assert result.summaries == []
        assert result.latest_summary is None
        assert result.previous_summary is None

    @pytest.mark.asyncio
    async def test_previous_summary(self):
        raw = [
            {"month": date(2026, 2, 1), "category": "payroll", "total": "3800"},
            {"month": date(2026, 3, 1), "category": "payroll", "total": "4000"},
        ]
        pool, _ = _mock_pool(raw)
        result = await get_expenses(pool, "biz_prev")
        assert result.latest_summary.month == date(2026, 3, 1)
        assert result.previous_summary.month == date(2026, 2, 1)

    @pytest.mark.asyncio
    async def test_summaries_ordered_by_month(self):
        raw = [
            {"month": date(2026, 3, 1), "category": "rent",    "total": "1500"},
            {"month": date(2026, 1, 1), "category": "rent",    "total": "1500"},
            {"month": date(2026, 2, 1), "category": "rent",    "total": "1500"},
        ]
        pool, _ = _mock_pool(raw)
        result = await get_expenses(pool, "biz_ord")
        months = [s.month for s in result.summaries]
        assert months == sorted(months)

    @pytest.mark.asyncio
    async def test_month_string_parsed(self):
        raw = [{"month": "2026-03-01", "category": "rent", "total": "1500"}]
        pool, _ = _mock_pool(raw)
        result = await get_expenses(pool, "biz_str")
        assert result.rows[0].month == date(2026, 3, 1)


# ---------------------------------------------------------------------------
# services.py
# ---------------------------------------------------------------------------

class TestGetServices:
    @pytest.mark.asyncio
    async def test_basic_mapping(self):
        raw = [
            {
                "month": date(2026, 3, 1),
                "service_name": "Facial Treatment",
                "bookings": 42,
                "revenue": "2100.00",
                "cancellations": 4,
            },
            {
                "month": date(2026, 3, 1),
                "service_name": "Haircut",
                "bookings": 60,
                "revenue": "1800.00",
                "cancellations": 2,
            },
        ]
        pool, _ = _mock_pool(raw)
        result = await get_services(pool, "salon_123")

        assert isinstance(result, ServiceResult)
        assert len(result.rows) == 2

        facial = result.rows[0]
        assert facial.service_name == "Facial Treatment"
        assert facial.bookings == 42
        assert facial.revenue == Decimal("2100.00")
        assert facial.cancellations == 4
        assert facial.cancellation_rate == pytest.approx(round(4 / 42, 4), abs=1e-9)
        # avg_price = revenue / completed = 2100 / 38
        expected_avg = (Decimal("2100") / 38).quantize(Decimal("0.01"))
        assert facial.avg_price == expected_avg

    @pytest.mark.asyncio
    async def test_sql_params(self):
        pool, cur = _mock_pool([])
        await get_services(pool, "biz_42", months=3)
        args = cur.execute.call_args[0]
        assert args[1] == ("biz_42", 3)

    @pytest.mark.asyncio
    async def test_empty_result(self):
        pool, _ = _mock_pool([])
        result = await get_services(pool, "biz_x")
        assert result.rows == []
        assert result.months == []

    @pytest.mark.asyncio
    async def test_top_by_revenue(self):
        m = date(2026, 3, 1)
        raw = [
            {"month": m, "service_name": "A", "bookings": 10, "revenue": "500", "cancellations": 0},
            {"month": m, "service_name": "B", "bookings": 20, "revenue": "800", "cancellations": 0},
            {"month": m, "service_name": "C", "bookings": 5,  "revenue": "300", "cancellations": 0},
        ]
        pool, _ = _mock_pool(raw)
        result = await get_services(pool, "biz_top")
        top2 = result.top_by_revenue(m, n=2)
        assert [r.service_name for r in top2] == ["B", "A"]

    @pytest.mark.asyncio
    async def test_top_by_bookings(self):
        m = date(2026, 3, 1)
        raw = [
            {"month": m, "service_name": "A", "bookings": 10, "revenue": "500", "cancellations": 0},
            {"month": m, "service_name": "B", "bookings": 20, "revenue": "800", "cancellations": 0},
            {"month": m, "service_name": "C", "bookings": 5,  "revenue": "300", "cancellations": 0},
        ]
        pool, _ = _mock_pool(raw)
        result = await get_services(pool, "biz_top")
        top2 = result.top_by_bookings(m, n=2)
        assert [r.service_name for r in top2] == ["B", "A"]

    @pytest.mark.asyncio
    async def test_months_property(self):
        raw = [
            {"month": date(2026, 2, 1), "service_name": "A", "bookings": 10, "revenue": "100", "cancellations": 0},
            {"month": date(2026, 3, 1), "service_name": "A", "bookings": 12, "revenue": "120", "cancellations": 0},
            {"month": date(2026, 3, 1), "service_name": "B", "bookings": 8,  "revenue": "80",  "cancellations": 0},
        ]
        pool, _ = _mock_pool(raw)
        result = await get_services(pool, "biz_months")
        assert result.months == [date(2026, 2, 1), date(2026, 3, 1)]

    @pytest.mark.asyncio
    async def test_zero_bookings_no_division_error(self):
        raw = [{"month": date(2026, 3, 1), "service_name": "X", "bookings": 0, "revenue": "0", "cancellations": 0}]
        pool, _ = _mock_pool(raw)
        result = await get_services(pool, "biz_z")
        assert result.rows[0].cancellation_rate == 0.0
        assert result.rows[0].avg_price == Decimal("0")

    @pytest.mark.asyncio
    async def test_month_string_parsed(self):
        raw = [{"month": "2026-03-01", "service_name": "Facial", "bookings": 10, "revenue": "500", "cancellations": 1}]
        pool, _ = _mock_pool(raw)
        result = await get_services(pool, "biz_str")
        assert result.rows[0].month == date(2026, 3, 1)
