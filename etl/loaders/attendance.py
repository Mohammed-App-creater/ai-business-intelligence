"""Loader for wh_attendance_summary."""
from __future__ import annotations

from etl.base import BaseLoader


class AttendanceLoader(BaseLoader):
    _SQL = """
        INSERT INTO wh_attendance_summary (
            business_id, employee_id, employee_name, location_id,
            period_start, period_end, days_worked, total_hours_worked, avg_hours_per_day,
            updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,now()
        )
        ON CONFLICT (business_id, employee_id, location_id, period_start) DO UPDATE SET
            employee_name       = EXCLUDED.employee_name,
            period_end          = EXCLUDED.period_end,
            days_worked         = EXCLUDED.days_worked,
            total_hours_worked  = EXCLUDED.total_hours_worked,
            avg_hours_per_day   = EXCLUDED.avg_hours_per_day,
            updated_at          = now()
    """.strip()

    @staticmethod
    def _param_fn(row: dict) -> tuple:
        return (
            row["business_id"],
            row["employee_id"],
            row["employee_name"],
            row["location_id"],
            row["period_start"],
            row["period_end"],
            row["days_worked"],
            row["total_hours_worked"],
            row["avg_hours_per_day"],
        )

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        return await self.upsert_many(self._SQL, rows, self._param_fn)
