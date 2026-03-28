"""
warehouse_client.py
===================
Single entry point for ALL warehouse read queries.

Usage:
    from app.services.db.warehouse_client import WarehouseClient
    from app.services.db.db_pool import PGPool, PGTarget

    pool = await PGPool.from_env(PGTarget.WAREHOUSE)
    wh   = WarehouseClient(pool)

    trend   = await wh.revenue.get_monthly_trend(org_id, months=6)
    staff   = await wh.staff.get_top_performers(org_id, period_start)
    clients = await wh.clients.get_churned_clients(org_id)

Domain sub-clients:
    wh.revenue        → warehouse/wh_revenue.py        (6 functions)
    wh.staff          → warehouse/wh_staff.py          (6 functions)
    wh.services       → warehouse/wh_services.py       (5 functions)
    wh.clients        → warehouse/wh_clients.py        (6 functions)
    wh.appointments   → warehouse/wh_appointments.py   (5 functions)
    wh.expenses       → warehouse/wh_expenses.py       (5 functions)
    wh.reviews        → warehouse/wh_reviews.py        (4 functions)
    wh.payments       → warehouse/wh_payments.py       (4 functions)
    wh.campaigns      → warehouse/wh_campaigns.py      (5 functions)
    wh.attendance     → warehouse/wh_attendance.py     (4 functions)
    wh.subscriptions  → warehouse/wh_subscriptions.py  (4 functions)
    wh.etl            → warehouse/wh_etl.py            (4 functions)
"""
from __future__ import annotations

from app.services.db.warehouse import wh_appointments
from app.services.db.warehouse import wh_attendance
from app.services.db.warehouse import wh_campaigns
from app.services.db.warehouse import wh_clients
from app.services.db.warehouse import wh_etl
from app.services.db.warehouse import wh_expenses
from app.services.db.warehouse import wh_payments
from app.services.db.warehouse import wh_revenue
from app.services.db.warehouse import wh_reviews
from app.services.db.warehouse import wh_services
from app.services.db.warehouse import wh_staff
from app.services.db.warehouse import wh_subscriptions


class WarehouseRevenueClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_monthly_trend(self, org_id: int, months: int = 6) -> list[dict]:
        return await wh_revenue.get_monthly_trend(self._pool, org_id, months)

    async def get_monthly_by_location(
        self, org_id: int, period_start, location_id: int
    ) -> dict | None:
        return await wh_revenue.get_monthly_by_location(
            self._pool, org_id, period_start, location_id
        )

    async def get_revenue_comparison(self, org_id: int, period_a, period_b) -> list[dict]:
        return await wh_revenue.get_revenue_comparison(
            self._pool, org_id, period_a, period_b
        )

    async def get_daily_trend(self, org_id: int, from_date, to_date) -> list[dict]:
        return await wh_revenue.get_daily_trend(self._pool, org_id, from_date, to_date)

    async def get_best_revenue_days(self, org_id: int, limit: int = 10) -> list[dict]:
        return await wh_revenue.get_best_revenue_days(self._pool, org_id, limit)

    async def get_location_revenue_summary(self, org_id: int, period_start) -> list[dict]:
        return await wh_revenue.get_location_revenue_summary(
            self._pool, org_id, period_start
        )


class WarehouseStaffClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_staff_monthly_performance(self, org_id: int, period_start) -> list[dict]:
        return await wh_staff.get_staff_monthly_performance(
            self._pool, org_id, period_start
        )

    async def get_staff_trend(
        self, org_id: int, employee_id: int, months: int = 6
    ) -> list[dict]:
        return await wh_staff.get_staff_trend(self._pool, org_id, employee_id, months)

    async def get_top_performers(
        self, org_id: int, period_start, limit: int = 5
    ) -> list[dict]:
        return await wh_staff.get_top_performers(
            self._pool, org_id, period_start, limit
        )

    async def get_staff_rating_ranking(self, org_id: int, period_start) -> list[dict]:
        return await wh_staff.get_staff_rating_ranking(
            self._pool, org_id, period_start
        )

    async def get_staff_utilisation(self, org_id: int, period_start) -> list[dict]:
        return await wh_staff.get_staff_utilisation(self._pool, org_id, period_start)

    async def get_underperforming_staff(
        self, org_id: int, period_start, min_visits: int = 1
    ) -> list[dict]:
        return await wh_staff.get_underperforming_staff(
            self._pool, org_id, period_start, min_visits
        )


class WarehouseServicesClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_service_monthly_performance(self, org_id: int, period_start) -> list[dict]:
        return await wh_services.get_service_monthly_performance(
            self._pool, org_id, period_start
        )

    async def get_top_services(
        self, org_id: int, period_start, limit: int = 10
    ) -> list[dict]:
        return await wh_services.get_top_services(
            self._pool, org_id, period_start, limit
        )

    async def get_service_trend(
        self, org_id: int, service_id: int, months: int = 6
    ) -> list[dict]:
        return await wh_services.get_service_trend(
            self._pool, org_id, service_id, months
        )

    async def get_service_revenue_ranking(self, org_id: int, period_start) -> list[dict]:
        return await wh_services.get_service_revenue_ranking(
            self._pool, org_id, period_start
        )

    async def get_declining_services(
        self, org_id: int, current_period, prev_period
    ) -> list[dict]:
        return await wh_services.get_declining_services(
            self._pool, org_id, current_period, prev_period
        )


class WarehouseClientsClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_churned_clients(self, org_id: int, limit: int = 100) -> list[dict]:
        return await wh_clients.get_churned_clients(self._pool, org_id, limit)

    async def get_top_clients_by_spend(self, org_id: int, limit: int = 20) -> list[dict]:
        return await wh_clients.get_top_clients_by_spend(self._pool, org_id, limit)

    async def get_retention_summary(self, org_id: int) -> dict | None:
        return await wh_clients.get_retention_summary(self._pool, org_id)

    async def get_new_clients(
        self, org_id: int, since_date, limit: int = 100
    ) -> list[dict]:
        return await wh_clients.get_new_clients(self._pool, org_id, since_date, limit)

    async def get_client_detail(self, org_id: int, customer_id: int) -> dict | None:
        return await wh_clients.get_client_detail(self._pool, org_id, customer_id)

    async def get_high_value_clients(
        self, org_id: int, min_spend: float, limit: int = 50
    ) -> list[dict]:
        return await wh_clients.get_high_value_clients(
            self._pool, org_id, min_spend, limit
        )


class WarehouseAppointmentsClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_appointment_monthly_summary(self, org_id: int, period_start) -> dict | None:
        return await wh_appointments.get_appointment_monthly_summary(
            self._pool, org_id, period_start
        )

    async def get_appointment_trend(self, org_id: int, months: int = 6) -> list[dict]:
        return await wh_appointments.get_appointment_trend(
            self._pool, org_id, months
        )

    async def get_cancellation_rate_trend(
        self, org_id: int, months: int = 6
    ) -> list[dict]:
        return await wh_appointments.get_cancellation_rate_trend(
            self._pool, org_id, months
        )

    async def get_walkin_vs_booked_trend(
        self, org_id: int, months: int = 6
    ) -> list[dict]:
        return await wh_appointments.get_walkin_vs_booked_trend(
            self._pool, org_id, months
        )

    async def get_location_appointment_comparison(
        self, org_id: int, period_start
    ) -> list[dict]:
        return await wh_appointments.get_location_appointment_comparison(
            self._pool, org_id, period_start
        )


class WarehouseExpensesClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_expense_monthly_summary(self, org_id: int, period_start) -> list[dict]:
        return await wh_expenses.get_expense_monthly_summary(
            self._pool, org_id, period_start
        )

    async def get_expense_trend(
        self, org_id: int, category_id: int, months: int = 6
    ) -> list[dict]:
        return await wh_expenses.get_expense_trend(
            self._pool, org_id, category_id, months
        )

    async def get_top_expense_categories(
        self, org_id: int, period_start, limit: int = 5
    ) -> list[dict]:
        return await wh_expenses.get_top_expense_categories(
            self._pool, org_id, period_start, limit
        )

    async def get_expense_total(self, org_id: int, period_start) -> dict | None:
        return await wh_expenses.get_expense_total(self._pool, org_id, period_start)

    async def get_expense_comparison(
        self, org_id: int, period_a, period_b
    ) -> list[dict]:
        return await wh_expenses.get_expense_comparison(
            self._pool, org_id, period_a, period_b
        )


class WarehouseReviewsClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_review_monthly_summary(self, org_id: int, period_start) -> dict | None:
        return await wh_reviews.get_review_monthly_summary(
            self._pool, org_id, period_start
        )

    async def get_review_trend(self, org_id: int, months: int = 6) -> list[dict]:
        return await wh_reviews.get_review_trend(self._pool, org_id, months)

    async def get_google_review_trend(self, org_id: int, months: int = 6) -> list[dict]:
        return await wh_reviews.get_google_review_trend(self._pool, org_id, months)

    async def get_rating_decline_periods(
        self, org_id: int, threshold: float = 3.5
    ) -> list[dict]:
        return await wh_reviews.get_rating_decline_periods(
            self._pool, org_id, threshold
        )


class WarehousePaymentsClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_payment_monthly_breakdown(self, org_id: int, period_start) -> dict | None:
        return await wh_payments.get_payment_monthly_breakdown(
            self._pool, org_id, period_start
        )

    async def get_payment_trend(self, org_id: int, months: int = 6) -> list[dict]:
        return await wh_payments.get_payment_trend(self._pool, org_id, months)

    async def get_cash_vs_card_trend(self, org_id: int, months: int = 6) -> list[dict]:
        return await wh_payments.get_cash_vs_card_trend(self._pool, org_id, months)

    async def get_gift_card_usage_trend(self, org_id: int, months: int = 6) -> list[dict]:
        return await wh_payments.get_gift_card_usage_trend(self._pool, org_id, months)


class WarehouseCampaignsClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_campaign_history(self, org_id: int, limit: int = 20) -> list[dict]:
        return await wh_campaigns.get_campaign_history(self._pool, org_id, limit)

    async def get_campaign_detail(self, org_id: int, campaign_id: int) -> list[dict]:
        return await wh_campaigns.get_campaign_detail(self._pool, org_id, campaign_id)

    async def get_top_campaigns_by_open_rate(
        self, org_id: int, limit: int = 10
    ) -> list[dict]:
        return await wh_campaigns.get_top_campaigns_by_open_rate(
            self._pool, org_id, limit
        )

    async def get_campaign_monthly_summary(self, org_id: int, period_start) -> list[dict]:
        return await wh_campaigns.get_campaign_monthly_summary(
            self._pool, org_id, period_start
        )

    async def get_recurring_campaigns(self, org_id: int) -> list[dict]:
        return await wh_campaigns.get_recurring_campaigns(self._pool, org_id)


class WarehouseAttendanceClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_staff_attendance_monthly(self, org_id: int, period_start) -> list[dict]:
        return await wh_attendance.get_staff_attendance_monthly(
            self._pool, org_id, period_start
        )

    async def get_staff_attendance_trend(
        self, org_id: int, employee_id: int, months: int = 6
    ) -> list[dict]:
        return await wh_attendance.get_staff_attendance_trend(
            self._pool, org_id, employee_id, months
        )

    async def get_total_hours_summary(self, org_id: int, period_start) -> dict | None:
        return await wh_attendance.get_total_hours_summary(
            self._pool, org_id, period_start
        )

    async def get_low_attendance_staff(
        self, org_id: int, period_start, min_days: int = 1
    ) -> list[dict]:
        return await wh_attendance.get_low_attendance_staff(
            self._pool, org_id, period_start, min_days
        )


class WarehouseSubscriptionsClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_subscription_monthly_summary(
        self, org_id: int, period_start
    ) -> dict | None:
        return await wh_subscriptions.get_subscription_monthly_summary(
            self._pool, org_id, period_start
        )

    async def get_subscription_trend(self, org_id: int, months: int = 6) -> list[dict]:
        return await wh_subscriptions.get_subscription_trend(
            self._pool, org_id, months
        )

    async def get_mrr_trend(self, org_id: int, months: int = 6) -> list[dict]:
        return await wh_subscriptions.get_mrr_trend(self._pool, org_id, months)

    async def get_subscription_growth(self, org_id: int, months: int = 6) -> list[dict]:
        return await wh_subscriptions.get_subscription_growth(
            self._pool, org_id, months
        )


class WarehouseEtlClient:
    def __init__(self, pool) -> None:
        self._pool = pool

    async def get_recent_runs(self, org_id: int | None = None, limit: int = 50) -> list[dict]:
        return await wh_etl.get_recent_runs(self._pool, org_id, limit)

    async def get_failed_runs(self, limit: int = 20) -> list[dict]:
        return await wh_etl.get_failed_runs(self._pool, limit)

    async def get_last_run_for_table(
        self, target_table: str, org_id: int | None = None
    ) -> dict | None:
        return await wh_etl.get_last_run_for_table(self._pool, target_table, org_id)

    async def get_etl_run_stats(self, target_table: str) -> dict | None:
        return await wh_etl.get_etl_run_stats(self._pool, target_table)


class WarehouseClient:
    """
    Single entry point for all warehouse read queries.

        pool = await PGPool.from_env(PGTarget.WAREHOUSE)
        wh   = WarehouseClient(pool)

        await wh.revenue.get_monthly_trend(org_id)
        await wh.etl.get_failed_runs(limit=10)
    """

    def __init__(self, pool) -> None:
        self._pool = pool
        self.revenue = WarehouseRevenueClient(pool)
        self.staff = WarehouseStaffClient(pool)
        self.services = WarehouseServicesClient(pool)
        self.clients = WarehouseClientsClient(pool)
        self.appointments = WarehouseAppointmentsClient(pool)
        self.expenses = WarehouseExpensesClient(pool)
        self.reviews = WarehouseReviewsClient(pool)
        self.payments = WarehousePaymentsClient(pool)
        self.campaigns = WarehouseCampaignsClient(pool)
        self.attendance = WarehouseAttendanceClient(pool)
        self.subscriptions = WarehouseSubscriptionsClient(pool)
        self.etl = WarehouseEtlClient(pool)

    @classmethod
    def from_pool(cls, pool) -> "WarehouseClient":
        return cls(pool)
