"""Loader for wh_monthly_revenue."""
from __future__ import annotations

from scripts.etl.base import BaseLoader


class RevenueLoader(BaseLoader):
    _SQL = """
        INSERT INTO wh_monthly_revenue (
            business_id, location_id, period_start, period_end,
            total_revenue, total_tips, total_tax, total_discounts,
            total_gc_amount, gross_revenue, visit_count,
            successful_visit_count, refunded_visit_count,
            cancelled_visit_count, avg_visit_value,
            cash_revenue, card_revenue, other_revenue, updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,now()
        )
        ON CONFLICT (business_id, location_id, period_start) DO UPDATE SET
            period_end             = EXCLUDED.period_end,
            total_revenue          = EXCLUDED.total_revenue,
            total_tips             = EXCLUDED.total_tips,
            total_tax              = EXCLUDED.total_tax,
            total_discounts        = EXCLUDED.total_discounts,
            total_gc_amount        = EXCLUDED.total_gc_amount,
            gross_revenue          = EXCLUDED.gross_revenue,
            visit_count            = EXCLUDED.visit_count,
            successful_visit_count = EXCLUDED.successful_visit_count,
            refunded_visit_count   = EXCLUDED.refunded_visit_count,
            cancelled_visit_count  = EXCLUDED.cancelled_visit_count,
            avg_visit_value        = EXCLUDED.avg_visit_value,
            cash_revenue           = EXCLUDED.cash_revenue,
            card_revenue           = EXCLUDED.card_revenue,
            other_revenue          = EXCLUDED.other_revenue,
            updated_at             = now()
    """.strip()

    @staticmethod
    def _param_fn(row: dict) -> tuple:
        return (
            row["business_id"],
            row["location_id"],
            row["period_start"],
            row["period_end"],
            row["total_revenue"],
            row["total_tips"],
            row["total_tax"],
            row["total_discounts"],
            row["total_gc_amount"],
            row["gross_revenue"],
            row["visit_count"],
            row["successful_visit_count"],
            row["refunded_visit_count"],
            row["cancelled_visit_count"],
            row["avg_visit_value"],
            row["cash_revenue"],
            row["card_revenue"],
            row["other_revenue"],
        )

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        return await self.upsert_many(self._SQL, rows, self._param_fn)
