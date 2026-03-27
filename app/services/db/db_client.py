"""
db_client.py
============
Single entry point for ALL database queries.

Every service that needs data from the production DB imports only this —
never the individual query modules directly. This keeps the import surface
clean and lets us swap query implementations without touching call sites.

Usage
-----
    from app.services.db.db_client import DBClient
    from app.services.db.db_pool import DBPool, DBTarget

    pool = await DBPool.from_env(DBTarget.PRODUCTION)
    db   = DBClient(pool)

    # Revenue
    revenue = await db.revenue.get_monthly_revenue_totals(org_id, from_date, to_date)

    # Appointments
    summary = await db.appointments.get_appointment_summary(org_id, from_date, to_date)

    # Staff
    roster = await db.staff.get_staff_roster(org_id)

Architecture
------------
DBClient holds one sub-client per domain. Each sub-client is a thin
wrapper that injects the pool into its query module functions.
Call sites never touch the pool directly — they only call db.<domain>.<func>().

Domain sub-clients:
    db.revenue       → queries/revenue.py       (6 functions)
    db.expenses      → queries/expenses.py      (5 functions)
    db.services      → queries/services.py      (6 functions)
    db.staff         → queries/staff.py         (7 functions)
    db.clients       → queries/clients.py       (6 functions)
    db.appointments  → queries/appointments.py  (6 functions)
    db.marketing     → queries/marketing.py     (5 functions)
    db.memberships   → queries/memberships.py   (5 functions)
    db.giftcards     → queries/giftcards.py     (5 functions)
    db.promos        → queries/promos.py        (4 functions)
    db.forms         → queries/forms.py         (5 functions)
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from app.services.db.queries import (
    appointments,
    clients,
    expenses,
    forms,
    giftcards,
    marketing,
    memberships,
    promos,
    revenue,
    services,
    staff,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain sub-clients — one per query module
# ---------------------------------------------------------------------------

class RevenueClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_monthly_revenue(self, org_id: int, from_date, to_date) -> list[dict]:
        return await revenue.get_monthly_revenue(self._pool, org_id, from_date, to_date)

    async def get_monthly_revenue_totals(self, org_id: int, from_date, to_date) -> list[dict]:
        return await revenue.get_monthly_revenue_totals(self._pool, org_id, from_date, to_date)

    async def get_revenue_by_payment_type(self, org_id: int, from_date, to_date) -> list[dict]:
        return await revenue.get_revenue_by_payment_type(self._pool, org_id, from_date, to_date)

    async def get_revenue_by_staff(self, org_id: int, from_date, to_date) -> list[dict]:
        return await revenue.get_revenue_by_staff(self._pool, org_id, from_date, to_date)

    async def get_daily_revenue(self, org_id: int, from_date, to_date) -> list[dict]:
        return await revenue.get_daily_revenue(self._pool, org_id, from_date, to_date)

    async def get_promo_usage(self, org_id: int, from_date, to_date) -> list[dict]:
        return await revenue.get_promo_usage(self._pool, org_id, from_date, to_date)


class ExpensesClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_monthly_expenses(self, org_id: int, from_date, to_date) -> list[dict]:
        return await expenses.get_monthly_expenses(self._pool, org_id, from_date, to_date)

    async def get_monthly_expense_totals(self, org_id: int, from_date, to_date) -> list[dict]:
        return await expenses.get_monthly_expense_totals(self._pool, org_id, from_date, to_date)

    async def get_net_profit(self, org_id: int, from_date, to_date) -> list[dict]:
        return await expenses.get_net_profit(self._pool, org_id, from_date, to_date)

    async def get_expenses_by_category(self, org_id: int, from_date, to_date) -> list[dict]:
        return await expenses.get_expenses_by_category(self._pool, org_id, from_date, to_date)

    async def get_expenses_by_location(self, org_id: int, from_date, to_date) -> list[dict]:
        return await expenses.get_expenses_by_location(self._pool, org_id, from_date, to_date)


class ServicesClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_service_popularity(self, org_id: int, from_date, to_date, limit: int = 20) -> list[dict]:
        return await services.get_service_popularity(self._pool, org_id, from_date, to_date, limit)

    async def get_service_popularity_trend(self, org_id: int, service_id: int, from_date, to_date) -> list[dict]:
        return await services.get_service_popularity_trend(self._pool, org_id, service_id, from_date, to_date)

    async def get_services_by_staff(self, org_id: int, emp_id: int, from_date, to_date) -> list[dict]:
        return await services.get_services_by_staff(self._pool, org_id, emp_id, from_date, to_date)

    async def get_walkin_service_demand(self, org_id: int, from_date, to_date, limit: int = 10) -> list[dict]:
        return await services.get_walkin_service_demand(self._pool, org_id, from_date, to_date, limit)

    async def get_service_catalog(self, org_id: int, active_only: bool = True) -> list[dict]:
        return await services.get_service_catalog(self._pool, org_id, active_only)

    async def get_service_inventory(self, org_id: int) -> list[dict]:
        return await services.get_service_inventory(self._pool, org_id)


class StaffClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_staff_performance(self, org_id: int, from_date, to_date) -> list[dict]:
        return await staff.get_staff_performance(self._pool, org_id, from_date, to_date)

    async def get_staff_ratings(self, org_id: int, from_date, to_date) -> list[dict]:
        return await staff.get_staff_ratings(self._pool, org_id, from_date, to_date)

    async def get_visit_reviews(self, org_id: int, from_date, to_date) -> list[dict]:
        return await staff.get_visit_reviews(self._pool, org_id, from_date, to_date)

    async def get_staff_hours(self, org_id: int, from_date, to_date) -> list[dict]:
        return await staff.get_staff_hours(self._pool, org_id, from_date, to_date)

    async def get_staff_commission_structure(self, org_id: int, emp_id: int | None = None) -> list[dict]:
        return await staff.get_staff_commission_structure(self._pool, org_id, emp_id)

    async def get_google_reviews_summary(self, org_id: int, from_date, to_date) -> list[dict]:
        return await staff.get_google_reviews_summary(self._pool, org_id, from_date, to_date)

    async def get_staff_roster(self, org_id: int, active_only: bool = True) -> list[dict]:
        return await staff.get_staff_roster(self._pool, org_id, active_only)


class ClientsClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_active_client_count(self, org_id: int) -> dict:
        return await clients.get_active_client_count(self._pool, org_id)

    async def get_client_retention(self, org_id: int, from_date, to_date) -> list[dict]:
        return await clients.get_client_retention(self._pool, org_id, from_date, to_date)

    async def get_lapsed_clients(self, org_id: int, days_since_visit: int = 60, limit: int = 50) -> list[dict]:
        return await clients.get_lapsed_clients(self._pool, org_id, days_since_visit, limit)

    async def get_top_clients_by_spend(self, org_id: int, from_date, to_date, limit: int = 20) -> list[dict]:
        return await clients.get_top_clients_by_spend(self._pool, org_id, from_date, to_date, limit)

    async def get_walkin_stats(self, org_id: int, from_date, to_date) -> list[dict]:
        return await clients.get_walkin_stats(self._pool, org_id, from_date, to_date)

    async def get_visit_frequency_distribution(self, org_id: int, from_date, to_date) -> list[dict]:
        return await clients.get_visit_frequency_distribution(self._pool, org_id, from_date, to_date)


class AppointmentsClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_appointment_summary(self, org_id: int, from_date, to_date) -> list[dict]:
        return await appointments.get_appointment_summary(self._pool, org_id, from_date, to_date)

    async def get_noshow_rate(self, org_id: int, from_date, to_date) -> list[dict]:
        return await appointments.get_noshow_rate(self._pool, org_id, from_date, to_date)

    async def get_cancellation_trend(self, org_id: int, from_date, to_date) -> list[dict]:
        return await appointments.get_cancellation_trend(self._pool, org_id, from_date, to_date)

    async def get_peak_hours(self, org_id: int, from_date, to_date) -> list[dict]:
        return await appointments.get_peak_hours(self._pool, org_id, from_date, to_date)

    async def get_bookings_by_staff(self, org_id: int, from_date, to_date) -> list[dict]:
        return await appointments.get_bookings_by_staff(self._pool, org_id, from_date, to_date)

    async def get_upcoming_appointments(self, org_id: int, from_date, to_date, limit: int = 50) -> list[dict]:
        return await appointments.get_upcoming_appointments(self._pool, org_id, from_date, to_date, limit)


class MarketingClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_campaigns(self, tenant_id: int, active_only: bool = False) -> list[dict]:
        return await marketing.get_campaigns(self._pool, tenant_id, active_only)

    async def get_campaign_performance(self, tenant_id: int, from_date, to_date) -> list[dict]:
        return await marketing.get_campaign_performance(self._pool, tenant_id, from_date, to_date)

    async def get_campaign_summary(self, tenant_id: int, from_date, to_date) -> list[dict]:
        return await marketing.get_campaign_summary(self._pool, tenant_id, from_date, to_date)

    async def get_active_campaigns(self, tenant_id: int) -> list[dict]:
        return await marketing.get_active_campaigns(self._pool, tenant_id)

    async def get_monthly_campaign_volume(self, tenant_id: int, from_date, to_date) -> list[dict]:
        return await marketing.get_monthly_campaign_volume(self._pool, tenant_id, from_date, to_date)


class MembershipsClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_active_subscription_count(self, org_id: int) -> dict:
        return await memberships.get_active_subscription_count(self._pool, org_id)

    async def get_subscription_growth(self, org_id: int, from_date, to_date) -> list[dict]:
        return await memberships.get_subscription_growth(self._pool, org_id, from_date, to_date)

    async def get_subscriptions_by_service(self, org_id: int) -> list[dict]:
        return await memberships.get_subscriptions_by_service(self._pool, org_id)

    async def get_upcoming_renewals(self, org_id: int, from_date, to_date) -> list[dict]:
        return await memberships.get_upcoming_renewals(self._pool, org_id, from_date, to_date)

    async def get_subscription_cancellations(self, org_id: int, from_date, to_date) -> list[dict]:
        return await memberships.get_subscription_cancellations(self._pool, org_id, from_date, to_date)


class GiftcardsClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_giftcard_liability(self, org_id: int) -> dict:
        return await giftcards.get_giftcard_liability(self._pool, org_id)

    async def get_giftcards_issued_by_month(self, org_id: int, from_date, to_date) -> list[dict]:
        return await giftcards.get_giftcards_issued_by_month(self._pool, org_id, from_date, to_date)

    async def get_giftcard_redemptions(self, org_id: int, from_date, to_date) -> list[dict]:
        return await giftcards.get_giftcard_redemptions(self._pool, org_id, from_date, to_date)

    async def get_low_balance_giftcards(self, org_id: int, threshold: float = 10.0) -> list[dict]:
        return await giftcards.get_low_balance_giftcards(self._pool, org_id, threshold)

    async def get_giftcard_issued_vs_redeemed(self, org_id: int, from_date, to_date) -> list[dict]:
        return await giftcards.get_giftcard_issued_vs_redeemed(self._pool, org_id, from_date, to_date)


class PromosClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_promo_catalog(self, active_only: bool = True) -> list[dict]:
        return await promos.get_promo_catalog(self._pool, active_only)

    async def get_active_promos(self) -> list[dict]:
        return await promos.get_active_promos(self._pool)

    async def get_promo_usage_by_org(self, org_id: int, from_date, to_date) -> list[dict]:
        return await promos.get_promo_usage_by_org(self._pool, org_id, from_date, to_date)

    async def get_monthly_promo_impact(self, org_id: int, from_date, to_date) -> list[dict]:
        return await promos.get_monthly_promo_impact(self._pool, org_id, from_date, to_date)


class FormsClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_form_catalog(self, org_id: int, active_only: bool = True) -> list[dict]:
        return await forms.get_form_catalog(self._pool, org_id, active_only)

    async def get_form_completion_summary(self, org_id: int) -> list[dict]:
        return await forms.get_form_completion_summary(self._pool, org_id)

    async def get_pending_forms(self, org_id: int, limit: int = 50) -> list[dict]:
        return await forms.get_pending_forms(self._pool, org_id, limit)

    async def get_form_completions_by_month(self, org_id: int, from_date, to_date) -> list[dict]:
        return await forms.get_form_completions_by_month(self._pool, org_id, from_date, to_date)

    async def get_client_form_status(self, org_id: int, cust_id: int) -> list[dict]:
        return await forms.get_client_form_status(self._pool, org_id, cust_id)


# ---------------------------------------------------------------------------
# DBClient — the single entry point
# ---------------------------------------------------------------------------

class DBClient:
    """
    Single entry point for all production DB queries.

    Instantiate once per request (or share across the lifetime of a pool):

        pool = await DBPool.from_env(DBTarget.PRODUCTION)
        db   = DBClient(pool)

    Then call any domain sub-client:

        await db.revenue.get_monthly_revenue_totals(org_id, from_date, to_date)
        await db.appointments.get_appointment_summary(org_id, from_date, to_date)
        await db.staff.get_staff_roster(org_id)
        await db.clients.get_lapsed_clients(org_id, days_since_visit=90)
        await db.marketing.get_campaign_summary(tenant_id, from_date, to_date)
        await db.promos.get_active_promos()
        await db.forms.get_pending_forms(org_id)

    Parameters
    ----------
    pool:
        A DBPool instance pointing at the production database.
        Use DBPool.from_env(DBTarget.PRODUCTION) in application code.
    """

    def __init__(self, pool) -> None:
        self._pool       = pool
        self.revenue      = RevenueClient(pool)
        self.expenses     = ExpensesClient(pool)
        self.services     = ServicesClient(pool)
        self.staff        = StaffClient(pool)
        self.clients      = ClientsClient(pool)
        self.appointments = AppointmentsClient(pool)
        self.marketing    = MarketingClient(pool)
        self.memberships  = MembershipsClient(pool)
        self.giftcards    = GiftcardsClient(pool)
        self.promos       = PromosClient(pool)
        self.forms        = FormsClient(pool)

    @classmethod
    def from_pool(cls, pool) -> "DBClient":
        """Convenience factory — same as DBClient(pool) but reads more clearly."""
        return cls(pool)
