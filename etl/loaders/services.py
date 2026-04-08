"""Loader for wh_service_performance."""
from __future__ import annotations

from scripts.etl.base import BaseLoader


class ServicesLoader(BaseLoader):
    _SQL = """
        INSERT INTO wh_service_performance (
            business_id, service_id, service_name, period_start, period_end,
            booking_count, revenue, avg_price, min_price, max_price, unique_customers,
            updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now()
        )
        ON CONFLICT (business_id, service_id, period_start) DO UPDATE SET
            service_name     = EXCLUDED.service_name,
            period_end       = EXCLUDED.period_end,
            booking_count    = EXCLUDED.booking_count,
            revenue          = EXCLUDED.revenue,
            avg_price        = EXCLUDED.avg_price,
            min_price        = EXCLUDED.min_price,
            max_price        = EXCLUDED.max_price,
            unique_customers = EXCLUDED.unique_customers,
            updated_at       = now()
    """.strip()

    @staticmethod
    def _param_fn(row: dict) -> tuple:
        return (
            row["business_id"],
            row["service_id"],
            row["service_name"],
            row["period_start"],
            row["period_end"],
            row["booking_count"],
            row["revenue"],
            row["avg_price"],
            row["min_price"],
            row["max_price"],
            row["unique_customers"],
        )

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        return await self.upsert_many(self._SQL, rows, self._param_fn)
