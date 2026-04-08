"""Loader for wh_daily_revenue."""
from __future__ import annotations

from etl.base import BaseLoader


class DailyRevenueLoader(BaseLoader):
    _SQL = """
        INSERT INTO wh_daily_revenue (
            business_id, location_id, revenue_date,
            total_revenue, total_tips, total_tax, total_discounts,
            gross_revenue, visit_count, successful_visit_count, avg_visit_value,
            updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now()
        )
        ON CONFLICT (business_id, location_id, revenue_date) DO UPDATE SET
            total_revenue            = EXCLUDED.total_revenue,
            total_tips               = EXCLUDED.total_tips,
            total_tax                = EXCLUDED.total_tax,
            total_discounts          = EXCLUDED.total_discounts,
            gross_revenue            = EXCLUDED.gross_revenue,
            visit_count              = EXCLUDED.visit_count,
            successful_visit_count   = EXCLUDED.successful_visit_count,
            avg_visit_value          = EXCLUDED.avg_visit_value,
            updated_at               = now()
    """.strip()

    @staticmethod
    def _param_fn(row: dict) -> tuple:
        return (
            row["business_id"],
            row["location_id"],
            row["revenue_date"],
            row["total_revenue"],
            row["total_tips"],
            row["total_tax"],
            row["total_discounts"],
            row["gross_revenue"],
            row["visit_count"],
            row["successful_visit_count"],
            row["avg_visit_value"],
        )

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        return await self.upsert_many(self._SQL, rows, self._param_fn)
