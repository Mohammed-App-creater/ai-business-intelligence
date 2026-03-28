"""Loader for wh_staff_performance."""
from __future__ import annotations

from scripts.etl.base import BaseLoader


class StaffLoader(BaseLoader):
    _SQL = """
        INSERT INTO wh_staff_performance (
            business_id, employee_id, employee_name, period_start, period_end,
            total_visits, total_revenue, total_tips, total_commission,
            appointments_booked, appointments_completed, appointments_cancelled,
            avg_rating, review_count, utilisation_rate, updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,now()
        )
        ON CONFLICT (business_id, employee_id, period_start) DO UPDATE SET
            employee_name          = EXCLUDED.employee_name,
            period_end             = EXCLUDED.period_end,
            total_visits           = EXCLUDED.total_visits,
            total_revenue          = EXCLUDED.total_revenue,
            total_tips             = EXCLUDED.total_tips,
            total_commission       = EXCLUDED.total_commission,
            appointments_booked    = EXCLUDED.appointments_booked,
            appointments_completed = EXCLUDED.appointments_completed,
            appointments_cancelled = EXCLUDED.appointments_cancelled,
            avg_rating             = EXCLUDED.avg_rating,
            review_count           = EXCLUDED.review_count,
            utilisation_rate       = EXCLUDED.utilisation_rate,
            updated_at             = now()
    """.strip()

    @staticmethod
    def _param_fn(row: dict) -> tuple:
        return (
            row["business_id"],
            row["employee_id"],
            row["employee_name"],
            row["period_start"],
            row["period_end"],
            row["total_visits"],
            row["total_revenue"],
            row["total_tips"],
            row["total_commission"],
            row["appointments_booked"],
            row["appointments_completed"],
            row["appointments_cancelled"],
            row["avg_rating"],
            row["review_count"],
            row["utilisation_rate"],
        )

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        return await self.upsert_many(self._SQL, rows, self._param_fn)
