"""Appointment metrics extractor — calendar + custsignin."""
from __future__ import annotations

from collections import defaultdict
from datetime import date

from scripts.etl.base import BaseExtractor
from scripts.etl.extractors._util import period_end_exclusive

_SQL_CALENDAR_PER_LOC = """
SELECT
    OrganizationId                                          AS business_id,
    BranchId                                                AS location_id,
    DATE_FORMAT(StartDate, '%Y-%m-01')                    AS period_start,
    LAST_DAY(StartDate)                                     AS period_end,
    COUNT(*)                                                AS total_booked,
    SUM(CASE WHEN Confirmed = 1 THEN 1 ELSE 0 END)         AS confirmed_count,
    SUM(CASE WHEN Complete  = 1 THEN 1 ELSE 0 END)         AS completed_count,
    SUM(CASE WHEN Active    = 0 THEN 1 ELSE 0 END)         AS cancelled_count,
    SUM(CASE WHEN Confirmed = 1
              AND Complete  = 0
              AND Active    = 1
              AND StartDate < NOW() THEN 1 ELSE 0 END)     AS no_show_count
FROM tbl_calendarevent
WHERE OrganizationId = %s
  AND StartDate >= %s
  AND StartDate <  %s
GROUP BY OrganizationId, BranchId, DATE_FORMAT(StartDate, '%Y-%m-01')
""".strip()

_SQL_CALENDAR_ROLLUP = """
SELECT
    OrganizationId                                          AS business_id,
    0                                                       AS location_id,
    DATE_FORMAT(StartDate, '%Y-%m-01')                    AS period_start,
    LAST_DAY(StartDate)                                     AS period_end,
    COUNT(*)                                                AS total_booked,
    SUM(CASE WHEN Confirmed = 1 THEN 1 ELSE 0 END)         AS confirmed_count,
    SUM(CASE WHEN Complete  = 1 THEN 1 ELSE 0 END)         AS completed_count,
    SUM(CASE WHEN Active    = 0 THEN 1 ELSE 0 END)         AS cancelled_count,
    SUM(CASE WHEN Confirmed = 1
              AND Complete  = 0
              AND Active    = 1
              AND StartDate < NOW() THEN 1 ELSE 0 END)     AS no_show_count
FROM tbl_calendarevent
WHERE OrganizationId = %s
  AND StartDate >= %s
  AND StartDate <  %s
GROUP BY OrganizationId, DATE_FORMAT(StartDate, '%Y-%m-01')
""".strip()

_SQL_SIGNIN = """
SELECT
    OrgId                                                   AS business_id,
    COALESCE(LocationId, 0)                                 AS location_id,
    DATE_FORMAT(RecDateTime, '%Y-%m-01')                  AS period_start,
    SUM(CASE WHEN AppType = 1 THEN 1 ELSE 0 END)           AS walkin_count,
    SUM(CASE WHEN AppType = 2 THEN 1 ELSE 0 END)           AS app_booking_count
FROM tbl_custsignin
WHERE OrgId = %s
  AND RecDateTime >= %s
  AND RecDateTime <  %s
  AND IsDeleted IS NULL
GROUP BY OrgId, COALESCE(LocationId, 0), DATE_FORMAT(RecDateTime, '%Y-%m-01')
""".strip()


def _loc_key(row: dict) -> tuple:
    return (row["business_id"], row["location_id"], row["period_start"])


class AppointmentsExtractor(BaseExtractor):
    async def extract(self, org_id: int, period_start: date, period_end: date) -> list[dict]:
        end_excl = period_end_exclusive(period_end)
        params = (org_id, period_start, end_excl)
        cal_per = await self.fetch_all(_SQL_CALENDAR_PER_LOC, params)
        cal_roll = await self.fetch_all(_SQL_CALENDAR_ROLLUP, params)
        signin = await self.fetch_all(_SQL_SIGNIN, params)

        merged: dict[tuple, dict] = {}
        for r in list(cal_per) + list(cal_roll):
            k = _loc_key(r)
            row = dict(r)
            row["walkin_count"] = 0
            row["app_booking_count"] = 0
            merged[k] = row

        signin_agg: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])
        for s in signin:
            bp = (s["business_id"], s["period_start"])
            signin_agg[bp][0] += int(s.get("walkin_count") or 0)
            signin_agg[bp][1] += int(s.get("app_booking_count") or 0)
            k = _loc_key(s)
            if k in merged:
                merged[k]["walkin_count"] += int(s.get("walkin_count") or 0)
                merged[k]["app_booking_count"] += int(s.get("app_booking_count") or 0)

        for k, row in merged.items():
            bid, lid, ps = k[0], k[1], k[2]
            if lid == 0:
                pair = signin_agg.get((bid, ps), [0, 0])
                row["walkin_count"] = pair[0]
                row["app_booking_count"] = pair[1]

        return list(merged.values())
