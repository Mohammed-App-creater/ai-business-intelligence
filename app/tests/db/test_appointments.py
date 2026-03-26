"""
Tests for queries/appointments.py
"""
from __future__ import annotations

import pytest
from .helpers import make_mock_pool, ORG_ID, FROM_DATE, TO_DATE

from app.services.db.queries.appointments import (
    get_appointment_summary,
    get_noshow_rate,
    get_cancellation_trend,
    get_peak_hours,
    get_bookings_by_staff,
    get_upcoming_appointments,
)


# ---------------------------------------------------------------------------
# get_appointment_summary
# ---------------------------------------------------------------------------

class TestGetAppointmentSummary:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "total": 180, "completed": 150,
                 "cancelled": 18, "no_show": 12,
                 "cancellation_rate_pct": 10.0, "completion_rate_pct": 83.3}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_appointment_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id_first(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_appointment_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_passes_date_range(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_appointment_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[1] == FROM_DATE
        assert params[2] == TO_DATE

    async def test_sql_queries_calendarevent(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_appointment_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_calendarevent" in sql

    async def test_sql_cancelled_is_active_zero(self):
        """Cancellation = Active = 0."""
        pool, cursor = make_mock_pool(rows=[])
        await get_appointment_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Active = 0" in sql

    async def test_sql_completed_requires_active_and_complete(self):
        """Completed = Active=1 AND Complete=1."""
        pool, cursor = make_mock_pool(rows=[])
        await get_appointment_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Complete = 1" in sql
        assert "Active = 1" in sql

    async def test_sql_no_show_checks_confirmed_and_startdate(self):
        """No-show = Confirmed=1, Complete=0, Active=1, StartDate < NOW()."""
        pool, cursor = make_mock_pool(rows=[])
        await get_appointment_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Confirmed" in sql
        assert "NOW()" in sql

    async def test_sql_groups_by_month(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_appointment_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql

    async def test_sql_includes_cancellation_rate(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_appointment_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "NULLIF" in sql or "100.0" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_appointment_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []


# ---------------------------------------------------------------------------
# get_noshow_rate
# ---------------------------------------------------------------------------

class TestGetNoshowRate:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "confirmed_bookings": 120,
                 "no_shows": 14, "noshow_rate_pct": 11.7}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_noshow_rate(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_noshow_rate(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_filters_confirmed_bookings(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_noshow_rate(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Confirmed" in sql

    async def test_sql_noshow_uses_startdate_lt_now(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_noshow_rate(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "NOW()" in sql

    async def test_sql_filters_active_events(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_noshow_rate(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql

    async def test_sql_groups_by_month(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_noshow_rate(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql


# ---------------------------------------------------------------------------
# get_cancellation_trend
# ---------------------------------------------------------------------------

class TestGetCancellationTrend:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "total_bookings": 180,
                 "cancelled": 18, "cancellation_rate_pct": 10.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_cancellation_trend(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_cancellation_trend(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_cancelled_is_active_zero(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_cancellation_trend(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Active = 0" in sql

    async def test_sql_groups_by_month(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_cancellation_trend(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql

    async def test_sql_includes_rate_calculation(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_cancellation_trend(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "100.0" in sql or "NULLIF" in sql


# ---------------------------------------------------------------------------
# get_peak_hours
# ---------------------------------------------------------------------------

class TestGetPeakHours:

    async def test_returns_fetchall_result(self):
        rows = [{"day_of_week": "Friday", "hour_of_day": 14, "booking_count": 47}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_peak_hours(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_peak_hours(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_extracts_day_and_hour(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_peak_hours(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "DAYNAME" in sql or "DAYOFWEEK" in sql
        assert "HOUR(" in sql

    async def test_sql_only_counts_active_events(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_peak_hours(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql

    async def test_sql_orders_by_booking_count_desc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_peak_hours(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY" in sql and "DESC" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_peak_hours(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []


# ---------------------------------------------------------------------------
# get_bookings_by_staff
# ---------------------------------------------------------------------------

class TestGetBookingsByStaff:

    async def test_returns_fetchall_result(self):
        rows = [{"emp_id": 12, "employee_name": "Maria Garcia",
                 "total_bookings": 95, "completed": 82,
                 "cancelled": 8, "no_show": 5}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_bookings_by_staff(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_bookings_by_staff(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_groups_by_employee(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_bookings_by_staff(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "EmployeeId" in sql
        assert "GROUP BY" in sql

    async def test_sql_includes_all_statuses(self):
        """Completed, cancelled, no-show all present."""
        pool, cursor = make_mock_pool(rows=[])
        await get_bookings_by_staff(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Complete" in sql
        assert "Active" in sql
        assert "Confirmed" in sql

    async def test_sql_orders_by_total_bookings_desc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_bookings_by_staff(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY" in sql and "DESC" in sql


# ---------------------------------------------------------------------------
# get_upcoming_appointments
# ---------------------------------------------------------------------------

class TestGetUpcomingAppointments:

    async def test_returns_fetchall_result(self):
        rows = [{"id": 6100, "start_date": "2026-03-27 10:00:00",
                 "customer_name": "Jane Smith", "service_name": "Balayage",
                 "employee_name": "Maria Garcia",
                 "branch_name": "Main Location", "confirmed": 1}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_upcoming_appointments(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_upcoming_appointments(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_default_limit_is_50(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_upcoming_appointments(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[-1] == 50

    async def test_custom_limit_passed(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_upcoming_appointments(pool, ORG_ID, FROM_DATE, TO_DATE, limit=10)
        params = cursor.execute.call_args[0][1]
        assert params[-1] == 10

    async def test_sql_filters_active_and_not_complete(self):
        """Only show live, unfinished appointments."""
        pool, cursor = make_mock_pool(rows=[])
        await get_upcoming_appointments(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql and "= 1" in sql
        assert "Complete" in sql and "= 0" in sql

    async def test_sql_orders_by_start_date_asc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_upcoming_appointments(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY" in sql
        assert "ASC" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_upcoming_appointments(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []
