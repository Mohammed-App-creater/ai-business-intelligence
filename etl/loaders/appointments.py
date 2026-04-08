"""Loader for wh_appointment_metrics."""
from __future__ import annotations

from scripts.etl.base import BaseLoader


class AppointmentsLoader(BaseLoader):
    _SQL = """
        INSERT INTO wh_appointment_metrics (
            business_id, location_id, period_start, period_end,
            total_booked, confirmed_count, completed_count, cancelled_count,
            no_show_count, walkin_count, app_booking_count,
            cancellation_rate, completion_rate, updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now()
        )
        ON CONFLICT (business_id, location_id, period_start) DO UPDATE SET
            period_end         = EXCLUDED.period_end,
            total_booked       = EXCLUDED.total_booked,
            confirmed_count    = EXCLUDED.confirmed_count,
            completed_count    = EXCLUDED.completed_count,
            cancelled_count    = EXCLUDED.cancelled_count,
            no_show_count      = EXCLUDED.no_show_count,
            walkin_count       = EXCLUDED.walkin_count,
            app_booking_count  = EXCLUDED.app_booking_count,
            cancellation_rate  = EXCLUDED.cancellation_rate,
            completion_rate    = EXCLUDED.completion_rate,
            updated_at         = now()
    """.strip()

    @staticmethod
    def _param_fn(row: dict) -> tuple:
        return (
            row["business_id"],
            row["location_id"],
            row["period_start"],
            row["period_end"],
            row["total_booked"],
            row["confirmed_count"],
            row["completed_count"],
            row["cancelled_count"],
            row["no_show_count"],
            row["walkin_count"],
            row["app_booking_count"],
            row["cancellation_rate"],
            row["completion_rate"],
        )

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        return await self.upsert_many(self._SQL, rows, self._param_fn)
