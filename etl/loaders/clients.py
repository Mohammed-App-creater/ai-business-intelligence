"""Loader for wh_client_metrics."""
from __future__ import annotations

from scripts.etl.base import BaseLoader


class ClientsLoader(BaseLoader):
    _SQL = """
        INSERT INTO wh_client_metrics (
            business_id, customer_id, first_visit_date, last_visit_date,
            total_visits, total_spend, avg_spend_per_visit, loyalty_points,
            days_since_last_visit, visit_frequency_days, is_churned, updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now()
        )
        ON CONFLICT (business_id, customer_id) DO UPDATE SET
            first_visit_date       = EXCLUDED.first_visit_date,
            last_visit_date        = EXCLUDED.last_visit_date,
            total_visits           = EXCLUDED.total_visits,
            total_spend            = EXCLUDED.total_spend,
            avg_spend_per_visit    = EXCLUDED.avg_spend_per_visit,
            loyalty_points         = EXCLUDED.loyalty_points,
            days_since_last_visit  = EXCLUDED.days_since_last_visit,
            visit_frequency_days   = EXCLUDED.visit_frequency_days,
            is_churned             = EXCLUDED.is_churned,
            updated_at             = now()
    """.strip()

    @staticmethod
    def _param_fn(row: dict) -> tuple:
        return (
            row["business_id"],
            row["customer_id"],
            row["first_visit_date"],
            row["last_visit_date"],
            row["total_visits"],
            row["total_spend"],
            row["avg_spend_per_visit"],
            row["loyalty_points"],
            row["days_since_last_visit"],
            row["visit_frequency_days"],
            row["is_churned"],
        )

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        return await self.upsert_many(self._SQL, rows, self._param_fn)
