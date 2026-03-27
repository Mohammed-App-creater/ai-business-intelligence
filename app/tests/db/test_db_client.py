"""
Tests for db_client.py

Tests verify:
  - DBClient exposes all 11 domain sub-clients
  - Each sub-client correctly delegates to the underlying query module
  - The pool is injected into every sub-client
  - from_pool() factory works correctly
  - All 60 public methods exist and are callable
"""
from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__))))

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from queries.helpers import make_mock_pool, ORG_ID, FROM_DATE, TO_DATE

from app.services.db.db_client import (
    DBClient,
    RevenueClient,
    ExpensesClient,
    ServicesClient,
    StaffClient,
    ClientsClient,
    AppointmentsClient,
    MarketingClient,
    MembershipsClient,
    GiftcardsClient,
    PromosClient,
    FormsClient,
)

TENANT_ID  = 42
EMP_ID     = 12
CUST_ID    = 441
SERVICE_ID = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_db(rows=None):
    """Return a DBClient backed by a mock pool."""
    pool, cursor = make_mock_pool(rows=rows or [])
    db = DBClient(pool)
    return db, pool, cursor


# ---------------------------------------------------------------------------
# DBClient structure
# ---------------------------------------------------------------------------

class TestDBClientStructure:

    def test_has_all_eleven_sub_clients(self):
        pool, _ = make_mock_pool()
        db = DBClient(pool)
        assert hasattr(db, "revenue")
        assert hasattr(db, "expenses")
        assert hasattr(db, "services")
        assert hasattr(db, "staff")
        assert hasattr(db, "clients")
        assert hasattr(db, "appointments")
        assert hasattr(db, "marketing")
        assert hasattr(db, "memberships")
        assert hasattr(db, "giftcards")
        assert hasattr(db, "promos")
        assert hasattr(db, "forms")

    def test_sub_clients_are_correct_types(self):
        pool, _ = make_mock_pool()
        db = DBClient(pool)
        assert isinstance(db.revenue,      RevenueClient)
        assert isinstance(db.expenses,     ExpensesClient)
        assert isinstance(db.services,     ServicesClient)
        assert isinstance(db.staff,        StaffClient)
        assert isinstance(db.clients,      ClientsClient)
        assert isinstance(db.appointments, AppointmentsClient)
        assert isinstance(db.marketing,    MarketingClient)
        assert isinstance(db.memberships,  MembershipsClient)
        assert isinstance(db.giftcards,    GiftcardsClient)
        assert isinstance(db.promos,       PromosClient)
        assert isinstance(db.forms,        FormsClient)

    def test_from_pool_factory_returns_dbclient(self):
        pool, _ = make_mock_pool()
        db = DBClient.from_pool(pool)
        assert isinstance(db, DBClient)

    def test_from_pool_same_as_constructor(self):
        pool, _ = make_mock_pool()
        db1 = DBClient(pool)
        db2 = DBClient.from_pool(pool)
        assert type(db1) == type(db2)
        assert db1._pool is db2._pool

    def test_pool_is_stored(self):
        pool, _ = make_mock_pool()
        db = DBClient(pool)
        assert db._pool is pool

    def test_all_sub_clients_share_same_pool(self):
        pool, _ = make_mock_pool()
        db = DBClient(pool)
        assert db.revenue._pool      is pool
        assert db.expenses._pool     is pool
        assert db.services._pool     is pool
        assert db.staff._pool        is pool
        assert db.clients._pool      is pool
        assert db.appointments._pool is pool
        assert db.marketing._pool    is pool
        assert db.memberships._pool  is pool
        assert db.giftcards._pool    is pool
        assert db.promos._pool       is pool
        assert db.forms._pool        is pool


# ---------------------------------------------------------------------------
# Revenue sub-client
# ---------------------------------------------------------------------------

class TestRevenueClient:

    async def test_get_monthly_revenue(self):
        rows = [{"month": "2026-03", "total_revenue": 9200.0}]
        db, _, _ = make_db(rows)
        result = await db.revenue.get_monthly_revenue(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_monthly_revenue_totals(self):
        rows = [{"month": "2026-03", "total_revenue": 9200.0}]
        db, _, _ = make_db(rows)
        result = await db.revenue.get_monthly_revenue_totals(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_revenue_by_payment_type(self):
        rows = [{"payment_type": "Card", "total": 7800.0}]
        db, _, _ = make_db(rows)
        result = await db.revenue.get_revenue_by_payment_type(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_revenue_by_staff(self):
        rows = [{"emp_id": 12, "total_revenue": 5200.0}]
        db, _, _ = make_db(rows)
        result = await db.revenue.get_revenue_by_staff(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_daily_revenue(self):
        rows = [{"date": "2026-03-01", "total_revenue": 980.0}]
        db, _, _ = make_db(rows)
        result = await db.revenue.get_daily_revenue(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_promo_usage(self):
        rows = [{"promo_code": "SUMMER20", "times_used": 14}]
        db, _, _ = make_db(rows)
        result = await db.revenue.get_promo_usage(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows


# ---------------------------------------------------------------------------
# Expenses sub-client
# ---------------------------------------------------------------------------

class TestExpensesClient:

    async def test_get_monthly_expenses(self):
        rows = [{"month": "2026-03", "total": 3200.0}]
        db, _, _ = make_db(rows)
        result = await db.expenses.get_monthly_expenses(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_monthly_expense_totals(self):
        rows = [{"month": "2026-03", "total": 3200.0}]
        db, _, _ = make_db(rows)
        result = await db.expenses.get_monthly_expense_totals(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_net_profit(self):
        rows = [{"month": "2026-03", "net_profit": 6000.0}]
        db, _, _ = make_db(rows)
        result = await db.expenses.get_net_profit(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_expenses_by_category(self):
        rows = [{"category": "Supplies", "total": 2400.0}]
        db, _, _ = make_db(rows)
        result = await db.expenses.get_expenses_by_category(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_expenses_by_location(self):
        rows = [{"location_id": 1, "total": 1800.0}]
        db, _, _ = make_db(rows)
        result = await db.expenses.get_expenses_by_location(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows


# ---------------------------------------------------------------------------
# Services sub-client
# ---------------------------------------------------------------------------

class TestServicesClient:

    async def test_get_service_popularity(self):
        rows = [{"service_name": "Balayage", "booking_count": 142}]
        db, _, _ = make_db(rows)
        result = await db.services.get_service_popularity(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_service_popularity_custom_limit(self):
        db, pool, _ = make_db([])
        with patch("app.services.db.queries.services.get_service_popularity",
                   new=AsyncMock(return_value=[])) as mock_fn:
            await db.services.get_service_popularity(ORG_ID, FROM_DATE, TO_DATE, limit=5)
            mock_fn.assert_called_once_with(pool, ORG_ID, FROM_DATE, TO_DATE, 5)

    async def test_get_service_popularity_trend(self):
        rows = [{"month": "2026-03", "booking_count": 42}]
        db, _, _ = make_db(rows)
        result = await db.services.get_service_popularity_trend(
            ORG_ID, SERVICE_ID, FROM_DATE, TO_DATE
        )
        assert result == rows

    async def test_get_service_catalog(self):
        rows = [{"service_id": 5, "name": "Balayage"}]
        db, _, _ = make_db(rows)
        result = await db.services.get_service_catalog(ORG_ID)
        assert result == rows

    async def test_get_service_inventory(self):
        rows = [{"service_id": 5, "quantity": 12}]
        db, _, _ = make_db(rows)
        result = await db.services.get_service_inventory(ORG_ID)
        assert result == rows


# ---------------------------------------------------------------------------
# Staff sub-client
# ---------------------------------------------------------------------------

class TestStaffClient:

    async def test_get_staff_performance(self):
        rows = [{"emp_id": 12, "total_revenue": 5200.0}]
        db, _, _ = make_db(rows)
        result = await db.staff.get_staff_performance(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_staff_ratings(self):
        rows = [{"emp_id": 12, "avg_rating": 4.8}]
        db, _, _ = make_db(rows)
        result = await db.staff.get_staff_ratings(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_visit_reviews(self):
        rows = [{"review_count": 87, "avg_rating": 4.6}]
        db, _, _ = make_db(rows)
        result = await db.staff.get_visit_reviews(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_staff_hours(self):
        rows = [{"emp_id": 12, "total_hours": 176.5}]
        db, _, _ = make_db(rows)
        result = await db.staff.get_staff_hours(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_staff_commission_structure_no_emp_id(self):
        rows = [{"emp_id": 12, "commission": 30.0}]
        db, _, _ = make_db(rows)
        result = await db.staff.get_staff_commission_structure(ORG_ID)
        assert result == rows

    async def test_get_staff_commission_structure_with_emp_id(self):
        db, pool, _ = make_db([])
        with patch("app.services.db.queries.staff.get_staff_commission_structure",
                   new=AsyncMock(return_value=[])) as mock_fn:
            await db.staff.get_staff_commission_structure(ORG_ID, emp_id=EMP_ID)
            mock_fn.assert_called_once_with(pool, ORG_ID, EMP_ID)

    async def test_get_staff_roster(self):
        rows = [{"emp_id": 12, "first_name": "Maria"}]
        db, _, _ = make_db(rows)
        result = await db.staff.get_staff_roster(ORG_ID)
        assert result == rows

    async def test_get_staff_roster_inactive(self):
        db, pool, _ = make_db([])
        with patch("app.services.db.queries.staff.get_staff_roster",
                   new=AsyncMock(return_value=[])) as mock_fn:
            await db.staff.get_staff_roster(ORG_ID, active_only=False)
            mock_fn.assert_called_once_with(pool, ORG_ID, False)


# ---------------------------------------------------------------------------
# Clients sub-client
# ---------------------------------------------------------------------------

class TestClientsClient:

    async def test_get_active_client_count(self):
        rows = [{"active_count": 842, "total_count": 962}]
        db, _, _ = make_db(rows)
        result = await db.clients.get_active_client_count(ORG_ID)
        assert result == rows[0]

    async def test_get_client_retention(self):
        rows = [{"month": "2026-03", "new_clients": 34}]
        db, _, _ = make_db(rows)
        result = await db.clients.get_client_retention(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_lapsed_clients_defaults(self):
        db, pool, _ = make_db([])
        with patch("app.services.db.queries.clients.get_lapsed_clients",
                   new=AsyncMock(return_value=[])) as mock_fn:
            await db.clients.get_lapsed_clients(ORG_ID)
            mock_fn.assert_called_once_with(pool, ORG_ID, 60, 50)

    async def test_get_lapsed_clients_custom(self):
        db, pool, _ = make_db([])
        with patch("app.services.db.queries.clients.get_lapsed_clients",
                   new=AsyncMock(return_value=[])) as mock_fn:
            await db.clients.get_lapsed_clients(ORG_ID, days_since_visit=90, limit=25)
            mock_fn.assert_called_once_with(pool, ORG_ID, 90, 25)

    async def test_get_top_clients_by_spend(self):
        rows = [{"cust_id": 441, "total_spend": 1440.0}]
        db, _, _ = make_db(rows)
        result = await db.clients.get_top_clients_by_spend(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_walkin_stats(self):
        rows = [{"month": "2026-03", "total_walkins": 142}]
        db, _, _ = make_db(rows)
        result = await db.clients.get_walkin_stats(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows


# ---------------------------------------------------------------------------
# Appointments sub-client
# ---------------------------------------------------------------------------

class TestAppointmentsClient:

    async def test_get_appointment_summary(self):
        rows = [{"month": "2026-03", "total": 180, "cancelled": 18}]
        db, _, _ = make_db(rows)
        result = await db.appointments.get_appointment_summary(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_noshow_rate(self):
        rows = [{"month": "2026-03", "noshow_rate_pct": 11.7}]
        db, _, _ = make_db(rows)
        result = await db.appointments.get_noshow_rate(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_cancellation_trend(self):
        rows = [{"month": "2026-03", "cancellation_rate_pct": 10.0}]
        db, _, _ = make_db(rows)
        result = await db.appointments.get_cancellation_trend(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_peak_hours(self):
        rows = [{"day_of_week": "Friday", "hour_of_day": 14, "booking_count": 47}]
        db, _, _ = make_db(rows)
        result = await db.appointments.get_peak_hours(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_upcoming_appointments_default_limit(self):
        db, pool, _ = make_db([])
        with patch("app.services.db.queries.appointments.get_upcoming_appointments",
                   new=AsyncMock(return_value=[])) as mock_fn:
            await db.appointments.get_upcoming_appointments(ORG_ID, FROM_DATE, TO_DATE)
            mock_fn.assert_called_once_with(pool, ORG_ID, FROM_DATE, TO_DATE, 50)


# ---------------------------------------------------------------------------
# Marketing sub-client
# ---------------------------------------------------------------------------

class TestMarketingClient:

    async def test_get_campaigns(self):
        rows = [{"id": 12, "name": "Summer Promo"}]
        db, _, _ = make_db(rows)
        result = await db.marketing.get_campaigns(TENANT_ID)
        assert result == rows

    async def test_get_campaign_performance(self):
        rows = [{"campaign_id": 12, "open_rate_pct": 30.0}]
        db, _, _ = make_db(rows)
        result = await db.marketing.get_campaign_performance(TENANT_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_campaign_summary(self):
        rows = [{"campaign_id": 12, "total_opened": 432}]
        db, _, _ = make_db(rows)
        result = await db.marketing.get_campaign_summary(TENANT_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_active_campaigns(self):
        rows = [{"id": 12, "status": "ready"}]
        db, _, _ = make_db(rows)
        result = await db.marketing.get_active_campaigns(TENANT_ID)
        assert result == rows

    async def test_get_monthly_campaign_volume(self):
        rows = [{"month": "2026-03", "total_sent": 2000}]
        db, _, _ = make_db(rows)
        result = await db.marketing.get_monthly_campaign_volume(TENANT_ID, FROM_DATE, TO_DATE)
        assert result == rows


# ---------------------------------------------------------------------------
# Memberships sub-client
# ---------------------------------------------------------------------------

class TestMembershipsClient:

    async def test_get_active_subscription_count(self):
        rows = [{"active_count": 42, "monthly_recurring_revenue": 2100.0}]
        db, _, _ = make_db(rows)
        result = await db.memberships.get_active_subscription_count(ORG_ID)
        assert result == rows[0]

    async def test_get_subscription_growth(self):
        rows = [{"month": "2026-03", "new_subscriptions": 8}]
        db, _, _ = make_db(rows)
        result = await db.memberships.get_subscription_growth(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_upcoming_renewals(self):
        rows = [{"subscription_id": 22, "execution_date": "2026-04-01"}]
        db, _, _ = make_db(rows)
        result = await db.memberships.get_upcoming_renewals(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows


# ---------------------------------------------------------------------------
# Giftcards sub-client
# ---------------------------------------------------------------------------

class TestGiftcardsClient:

    async def test_get_giftcard_liability(self):
        rows = [{"total_outstanding_balance": 2250.0}]
        db, _, _ = make_db(rows)
        result = await db.giftcards.get_giftcard_liability(ORG_ID)
        assert result == rows[0]

    async def test_get_giftcard_redemptions(self):
        rows = [{"month": "2026-03", "total_redeemed": 720.0}]
        db, _, _ = make_db(rows)
        result = await db.giftcards.get_giftcard_redemptions(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_low_balance_giftcards_default_threshold(self):
        db, pool, _ = make_db([])
        with patch("app.services.db.queries.giftcards.get_low_balance_giftcards",
                   new=AsyncMock(return_value=[])) as mock_fn:
            await db.giftcards.get_low_balance_giftcards(ORG_ID)
            mock_fn.assert_called_once_with(pool, ORG_ID, 10.0)

    async def test_get_low_balance_giftcards_custom_threshold(self):
        db, pool, _ = make_db([])
        with patch("app.services.db.queries.giftcards.get_low_balance_giftcards",
                   new=AsyncMock(return_value=[])) as mock_fn:
            await db.giftcards.get_low_balance_giftcards(ORG_ID, threshold=25.0)
            mock_fn.assert_called_once_with(pool, ORG_ID, 25.0)


# ---------------------------------------------------------------------------
# Promos sub-client
# ---------------------------------------------------------------------------

class TestPromosClient:

    async def test_get_promo_catalog(self):
        rows = [{"promo_code": "SUMMER20", "amount": 20.0}]
        db, _, _ = make_db(rows)
        result = await db.promos.get_promo_catalog()
        assert result == rows

    async def test_get_active_promos(self):
        rows = [{"promo_code": "SUMMER20", "expiration_date": "2026-08-31"}]
        db, _, _ = make_db(rows)
        result = await db.promos.get_active_promos()
        assert result == rows

    async def test_get_promo_usage_by_org(self):
        rows = [{"promo_code": "SUMMER20", "times_used": 14}]
        db, _, _ = make_db(rows)
        result = await db.promos.get_promo_usage_by_org(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_monthly_promo_impact(self):
        rows = [{"month": "2026-03", "promo_visits": 18}]
        db, _, _ = make_db(rows)
        result = await db.promos.get_monthly_promo_impact(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows


# ---------------------------------------------------------------------------
# Forms sub-client
# ---------------------------------------------------------------------------

class TestFormsClient:

    async def test_get_form_catalog(self):
        rows = [{"form_id": 4, "name": "New Client Intake"}]
        db, _, _ = make_db(rows)
        result = await db.forms.get_form_catalog(ORG_ID)
        assert result == rows

    async def test_get_form_completion_summary(self):
        rows = [{"form_id": 4, "completion_rate_pct": 80.9}]
        db, _, _ = make_db(rows)
        result = await db.forms.get_form_completion_summary(ORG_ID)
        assert result == rows

    async def test_get_pending_forms_default_limit(self):
        db, pool, _ = make_db([])
        with patch("app.services.db.queries.forms.get_pending_forms",
                   new=AsyncMock(return_value=[])) as mock_fn:
            await db.forms.get_pending_forms(ORG_ID)
            mock_fn.assert_called_once_with(pool, ORG_ID, 50)

    async def test_get_form_completions_by_month(self):
        rows = [{"month": "2026-03", "completed": 18}]
        db, _, _ = make_db(rows)
        result = await db.forms.get_form_completions_by_month(ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_get_client_form_status(self):
        rows = [{"form_name": "New Client Intake", "status": "complete"}]
        db, _, _ = make_db(rows)
        result = await db.forms.get_client_form_status(ORG_ID, CUST_ID)
        assert result == rows


# ---------------------------------------------------------------------------
# Method coverage — verify all 60 public methods exist
# ---------------------------------------------------------------------------

class TestMethodCoverage:

    def _db(self):
        pool, _ = make_mock_pool()
        return DBClient(pool)

    def test_revenue_has_6_methods(self):
        db = self._db()
        methods = [m for m in dir(db.revenue) if not m.startswith("_")]
        assert len(methods) == 6

    def test_expenses_has_5_methods(self):
        db = self._db()
        methods = [m for m in dir(db.expenses) if not m.startswith("_")]
        assert len(methods) == 5

    def test_services_has_6_methods(self):
        db = self._db()
        methods = [m for m in dir(db.services) if not m.startswith("_")]
        assert len(methods) == 6

    def test_staff_has_7_methods(self):
        db = self._db()
        methods = [m for m in dir(db.staff) if not m.startswith("_")]
        assert len(methods) == 7

    def test_clients_has_6_methods(self):
        db = self._db()
        methods = [m for m in dir(db.clients) if not m.startswith("_")]
        assert len(methods) == 6

    def test_appointments_has_6_methods(self):
        db = self._db()
        methods = [m for m in dir(db.appointments) if not m.startswith("_")]
        assert len(methods) == 6

    def test_marketing_has_5_methods(self):
        db = self._db()
        methods = [m for m in dir(db.marketing) if not m.startswith("_")]
        assert len(methods) == 5

    def test_memberships_has_5_methods(self):
        db = self._db()
        methods = [m for m in dir(db.memberships) if not m.startswith("_")]
        assert len(methods) == 5

    def test_giftcards_has_5_methods(self):
        db = self._db()
        methods = [m for m in dir(db.giftcards) if not m.startswith("_")]
        assert len(methods) == 5

    def test_promos_has_4_methods(self):
        db = self._db()
        methods = [m for m in dir(db.promos) if not m.startswith("_")]
        assert len(methods) == 4

    def test_forms_has_5_methods(self):
        db = self._db()
        methods = [m for m in dir(db.forms) if not m.startswith("_")]
        assert len(methods) == 5
