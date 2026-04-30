"""
etl/transforms/staff_etl.py
============================
Staff Performance domain ETL extractor.

Pulls all 3 staff data slices from the analytics backend,
writes them to the warehouse (wh_staff_* tables), and returns the
same structured documents for immediate use by the doc generator.

Flow:
    Mock Server / Analytics Backend
        ↓  StaffExtractor.run()
        ↓  _write_to_warehouse()  ← writes to 3 wh_staff_* tables
    wh_staff_performance_monthly  ← monthly KPIs per staff × location × period
    wh_staff_summary              ← all-time totals per staff
    wh_staff_attendance           ← hours worked per staff × location × period
        ↓  embed_documents.py reads from warehouse
        ↓  generates chunk text
    pgvector                      ← for RAG retrieval

Usage (with warehouse write):
    extractor = StaffExtractor(client=analytics_client, wh_pool=wh_pool)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)

Usage (without warehouse — pgvector only, e.g. tests):
    extractor = StaffExtractor(client=analytics_client)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)

NOTE: No-shows, cancellation rates, and completion rates per staff are served
by the Appointments domain (wh_appt_staff_breakdown) — no duplication here.
Q38/Q39/Q40 are answered by the AI layer joining this domain's payment-level
cancellations with the appointments domain's appointment-level no-shows.
"""

from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date

from app.services.analytics_client import AnalyticsClient

logger = logging.getLogger(__name__)


def _normalize_hire_date(raw):
    """
    Coerce backend hire_date payloads to a date or None.

    Handles:
      - None / "" / 0 → None
      - .NET DateTime.MinValue sentinel "0001-01-01..." → None
      - ISO date "2024-04-08" → date(2024, 4, 8)
      - ISO datetime "2024-04-08T00:00:00" → date(2024, 4, 8) (strips time portion)

    Never raises. Returns None on any parse failure rather than crashing the batch.
    """
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    # Strip time portion if present — date.fromisoformat doesn't accept it
    date_part = s.split("T", 1)[0].split(" ", 1)[0]
    # .NET DateTime.MinValue sentinel — semantically "no date set"
    if date_part.startswith("0001-01-01"):
        return None
    try:
        return date.fromisoformat(date_part)
    except (ValueError, TypeError):
        return None


class StaffExtractor:
    """
    Pulls and transforms all staff performance data for one tenant.

    Parameters
    ----------
    client:   AnalyticsClient — calls the analytics backend API.
    wh_pool:  Optional asyncpg/PGPool — when provided, writes extracted
              rows to the warehouse before returning. When None, the
              warehouse write is skipped (useful in tests).
    """

    DOMAIN = "staff"

    def __init__(self, client: AnalyticsClient, wh_pool=None):
        self.client  = client
        self.wh_pool = wh_pool

    # ─────────────────────────────────────────────────────────────────────────
    # Public entry point
    # ─────────────────────────────────────────────────────────────────────────

    async def run(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        Fetch all 3 staff slices, write to warehouse, return docs.
        """
        logger.info(
            "StaffExtractor: business_id=%s %s → %s",
            business_id, start_date, end_date,
        )

        # ── 1. Fetch all 3 slices ─────────────────────────────────────────────
        # summary and attendance run in parallel with monthly.
        # summary uses the same endpoint as monthly (mode=summary in payload).
        import asyncio
        monthly_raw, summary_raw, attendance_raw = await asyncio.gather(
            self.client.get_staff_performance_monthly(
                business_id, start_date, end_date
            ),
            self.client.get_staff_performance_summary(
                business_id, start_date, end_date
            ),
            self.client.get_staff_attendance(
                business_id, start_date, end_date
            ),
        )

        # ── 2. Transform each slice into warehouse documents ──────────────────
        docs: list[dict] = []
        docs.extend(self._transform_monthly(business_id, monthly_raw))
        docs.extend(self._transform_summary(business_id, summary_raw))
        docs.extend(self._transform_attendance(business_id, attendance_raw))

        # ── 3. Write to warehouse (if pool provided) ──────────────────────────
        if self.wh_pool is not None:
            await self._write_to_warehouse(docs)
        else:
            logger.debug(
                "StaffExtractor: wh_pool not provided — skipping warehouse write"
            )

        logger.info(
            "StaffExtractor: produced %d documents for business_id=%s",
            len(docs), business_id,
        )
        return docs

    # ─────────────────────────────────────────────────────────────────────────
    # Warehouse write — 3 upsert methods, one per table
    # ─────────────────────────────────────────────────────────────────────────

    async def _write_to_warehouse(self, docs: list[dict]) -> None:
        """Upsert all docs into the 3 wh_staff_* tables. Idempotent."""
        by_type: dict[str, list[dict]] = {}
        for doc in docs:
            by_type.setdefault(doc.get("doc_type", ""), []).append(doc)

        async with self.wh_pool.acquire() as conn:
            await self._upsert_monthly(
                conn, by_type.get("staff_monthly", [])
            )
            await self._upsert_summary(
                conn, by_type.get("staff_summary", [])
            )
            await self._upsert_attendance(
                conn, by_type.get("staff_attendance", [])
            )

        logger.info(
            "StaffExtractor: warehouse write complete — "
            "monthly=%d summary=%d attendance=%d",
            len(by_type.get("staff_monthly", [])),
            len(by_type.get("staff_summary", [])),
            len(by_type.get("staff_attendance", [])),
        )

    async def _upsert_monthly(self, conn, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
INSERT INTO wh_staff_performance_monthly (
    business_id, employee_id, employee_name,
    employee_first_name, employee_last_name,
    is_active, hire_date,
    location_id, location_name,
    period_start, period_end,
    completed_visit_count, unique_customer_count,
    revenue, tips, total_pay, avg_revenue_per_visit, commission_earned,
    cancelled_payment_count, refunded_payment_count, revoked_payment_count,
    review_count, avg_rating
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23
)
ON CONFLICT (business_id, employee_id, location_id, period_start) DO UPDATE SET
    employee_name               = EXCLUDED.employee_name,
    employee_first_name         = EXCLUDED.employee_first_name,
    employee_last_name          = EXCLUDED.employee_last_name,
    is_active                   = EXCLUDED.is_active,
    hire_date                   = EXCLUDED.hire_date,
    location_name               = EXCLUDED.location_name,
    period_end                  = EXCLUDED.period_end,
    completed_visit_count       = EXCLUDED.completed_visit_count,
    unique_customer_count       = EXCLUDED.unique_customer_count,
    revenue                     = EXCLUDED.revenue,
    tips                        = EXCLUDED.tips,
    total_pay                   = EXCLUDED.total_pay,
    avg_revenue_per_visit       = EXCLUDED.avg_revenue_per_visit,
    commission_earned           = EXCLUDED.commission_earned,
    cancelled_payment_count     = EXCLUDED.cancelled_payment_count,
    refunded_payment_count      = EXCLUDED.refunded_payment_count,
    revoked_payment_count       = EXCLUDED.revoked_payment_count,
    review_count                = EXCLUDED.review_count,
    avg_rating                  = EXCLUDED.avg_rating,
    updated_at                  = now()
"""
        records = []
        for r in rows:
            y, m = int(r["period_label"][:4]), int(r["period_label"][5:7])
            ps = date(y, m, 1)
            pe = date(y, m, monthrange(y, m)[1])

            hire_date = r.get("hire_date")  # already a date or None — normalized in transform

            records.append((
                r["tenant_id"],
                r["staff_id"],
                r["staff_full_name"],
                r.get("staff_first_name", ""),
                r.get("staff_last_name", ""),
                r.get("is_active", True),
                hire_date,
                r["location_id"],
                r.get("location_name", ""),
                ps, pe,
                r["completed_visit_count"],
                r.get("unique_customer_count", 0),
                r["revenue"],
                r.get("tips", 0),
                r.get("total_pay", 0),
                r.get("avg_revenue_per_visit", 0),
                r.get("commission_earned", 0),
                r.get("cancelled_payment_count", 0),
                r.get("refunded_payment_count", 0),
                r.get("revoked_payment_count", 0),
                r.get("review_count", 0),
                r.get("avg_rating"),           # None allowed — not 0
            ))
        await conn.executemany(sql, records)

    async def _upsert_summary(self, conn, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
INSERT INTO wh_staff_summary (
    business_id, employee_id, employee_name,
    employee_first_name, employee_last_name,
    is_active, hire_date,
    period_from, period_to,
    total_visits_ytd, total_revenue_ytd,
    total_tips_ytd, total_commission_ytd,
    total_customers_served,
    total_cancelled_ytd, total_refunded_ytd,
    overall_avg_rating, total_review_count,
    lifetime_avg_revenue_per_visit,
    revenue_pct_of_org_latest
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20
)
ON CONFLICT (business_id, employee_id) DO UPDATE SET
    employee_name                   = EXCLUDED.employee_name,
    employee_first_name             = EXCLUDED.employee_first_name,
    employee_last_name              = EXCLUDED.employee_last_name,
    is_active                       = EXCLUDED.is_active,
    hire_date                       = EXCLUDED.hire_date,
    period_from                     = EXCLUDED.period_from,
    period_to                       = EXCLUDED.period_to,
    total_visits_ytd                = EXCLUDED.total_visits_ytd,
    total_revenue_ytd               = EXCLUDED.total_revenue_ytd,
    total_tips_ytd                  = EXCLUDED.total_tips_ytd,
    total_commission_ytd            = EXCLUDED.total_commission_ytd,
    total_customers_served          = EXCLUDED.total_customers_served,
    total_cancelled_ytd             = EXCLUDED.total_cancelled_ytd,
    total_refunded_ytd              = EXCLUDED.total_refunded_ytd,
    overall_avg_rating              = EXCLUDED.overall_avg_rating,
    total_review_count              = EXCLUDED.total_review_count,
    lifetime_avg_revenue_per_visit  = EXCLUDED.lifetime_avg_revenue_per_visit,
    revenue_pct_of_org_latest       = EXCLUDED.revenue_pct_of_org_latest,
    updated_at                      = now()
"""
        records = []
        for r in rows:
            hire_date = r.get("hire_date")  # already a date or None — normalized in transform

            records.append((
                r["tenant_id"],
                r["staff_id"],
                r["staff_full_name"],
                r.get("staff_first_name", ""),
                r.get("staff_last_name", ""),
                r.get("is_active", True),
                hire_date,
                r.get("first_active_period"),
                r.get("last_active_period"),
                r.get("total_visits_ytd", 0),
                r.get("total_revenue_ytd", 0),
                r.get("total_tips_ytd", 0),
                r.get("total_commission_ytd", 0),
                r.get("total_customers_served", 0),
                r.get("total_cancelled_ytd", 0),
                r.get("total_refunded_ytd", 0),
                r.get("overall_avg_rating"),     # None allowed
                r.get("total_review_count", 0),
                r.get("lifetime_avg_revenue_per_visit", 0),
                r.get("revenue_pct_of_org_latest"),  # None for inactive
            ))
        await conn.executemany(sql, records)

    async def _upsert_attendance(self, conn, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
INSERT INTO wh_staff_attendance (
    business_id, employee_id, employee_name, is_active,
    location_id, location_name,
    period_start, period_end,
    days_with_signin, days_fully_recorded, days_missing_signout,
    total_hours_worked, avg_hours_per_day
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
ON CONFLICT (business_id, employee_id, location_id, period_start) DO UPDATE SET
    employee_name           = EXCLUDED.employee_name,
    is_active               = EXCLUDED.is_active,
    location_name           = EXCLUDED.location_name,
    period_end              = EXCLUDED.period_end,
    days_with_signin        = EXCLUDED.days_with_signin,
    days_fully_recorded     = EXCLUDED.days_fully_recorded,
    days_missing_signout    = EXCLUDED.days_missing_signout,
    total_hours_worked      = EXCLUDED.total_hours_worked,
    avg_hours_per_day       = EXCLUDED.avg_hours_per_day,
    updated_at              = now()
"""
        records = []
        for r in rows:
            y, m = int(r["period_label"][:4]), int(r["period_label"][5:7])
            ps = date(y, m, 1)
            pe = date(y, m, monthrange(y, m)[1])

            records.append((
                r["tenant_id"],
                r["staff_id"],
                r["staff_full_name"],
                r.get("is_active", True),
                r["location_id"],
                r.get("location_name", ""),
                ps, pe,
                r.get("days_with_signin", 0),
                r.get("days_fully_recorded", 0),
                r.get("days_missing_signout", 0),
                r.get("total_hours_worked", 0),
                r.get("avg_hours_per_day"),      # None when days_fully_recorded = 0
            ))
        await conn.executemany(sql, records)

    # ─────────────────────────────────────────────────────────────────────────
    # Transform methods — API response → doc dict
    # ─────────────────────────────────────────────────────────────────────────

    def _transform_monthly(self, business_id: int, rows: list[dict]) -> list[dict]:
        """
        One warehouse document per (staff × location × period).
        Produces doc_type='staff_monthly'.
        """
        docs = []
        for row in rows:
            hire_date = _normalize_hire_date(row.get("hire_date"))
            docs.append({
                # Routing keys
                "tenant_id":                business_id,
                "doc_type":                 "staff_monthly",
                "domain":                   self.DOMAIN,

                # Identifiers
                "staff_id":                 row.get("staff_id"),
                "staff_full_name":          row.get("staff_full_name", ""),
                "staff_first_name":         row.get("staff_first_name", ""),
                "staff_last_name":          row.get("staff_last_name", ""),
                "is_active":                row.get("is_active", True),
                "hire_date":                hire_date,
                "location_id":              row.get("location_id", 0),
                "location_name":            row.get("location_name", ""),
                "period_label":             row.get("period_label", ""),

                # Visit metrics
                "completed_visit_count":    row.get("completed_visit_count", 0),
                "unique_customer_count":    row.get("unique_customer_count", 0),

                # Revenue metrics
                "revenue":                  row.get("revenue", 0),
                "tips":                     row.get("tips", 0),
                "total_pay":                row.get("total_pay", 0),
                "avg_revenue_per_visit":    row.get("avg_revenue_per_visit", 0),
                "commission_earned":        row.get("commission_earned", 0),

                # Cancellation counts (payment-level)
                "cancelled_payment_count":  row.get("cancelled_payment_count", 0),
                "refunded_payment_count":   row.get("refunded_payment_count", 0),
                "revoked_payment_count":    row.get("revoked_payment_count", 0),

                # Rating
                "review_count":             row.get("review_count", 0),
                "avg_rating":               row.get("avg_rating"),  # None ok

                # text field is generated by the doc generator, not here
            })
        return docs

    def _transform_summary(self, business_id: int, rows: list[dict]) -> list[dict]:
        """
        One warehouse document per staff member (all-time totals).
        Produces doc_type='staff_summary'.
        """
        docs = []
        for row in rows:
            hire_date = _normalize_hire_date(row.get("hire_date"))
            docs.append({
                "tenant_id":                        business_id,
                "doc_type":                         "staff_summary",
                "domain":                           self.DOMAIN,

                "staff_id":                         row.get("staff_id"),
                "staff_full_name":                  row.get("staff_full_name", ""),
                "staff_first_name":                 row.get("staff_first_name", ""),
                "staff_last_name":                  row.get("staff_last_name", ""),
                "is_active":                        row.get("is_active", True),
                "hire_date":                        hire_date,

                "first_active_period":              row.get("first_active_period"),
                "last_active_period":               row.get("last_active_period"),

                "total_visits_ytd":                 row.get("total_visits_ytd", 0),
                "total_revenue_ytd":                row.get("total_revenue_ytd", 0),
                "total_tips_ytd":                   row.get("total_tips_ytd", 0),
                "total_commission_ytd":             row.get("total_commission_ytd", 0),
                "total_customers_served":           row.get("total_customers_served", 0),
                "total_cancelled_ytd":              row.get("total_cancelled_ytd", 0),
                "total_refunded_ytd":               row.get("total_refunded_ytd", 0),
                "overall_avg_rating":               row.get("overall_avg_rating"),  # None ok
                "total_review_count":               row.get("total_review_count", 0),
                "lifetime_avg_revenue_per_visit":   row.get("lifetime_avg_revenue_per_visit", 0),
                "revenue_pct_of_org_latest":        row.get("revenue_pct_of_org_latest"),  # None ok
            })
        return docs

    def _transform_attendance(self, business_id: int, rows: list[dict]) -> list[dict]:
        """
        One warehouse document per (staff × location × period).
        Produces doc_type='staff_attendance'.
        """
        docs = []
        for row in rows:
            docs.append({
                "tenant_id":            business_id,
                "doc_type":             "staff_attendance",
                "domain":               self.DOMAIN,

                "staff_id":             row.get("staff_id"),
                "staff_full_name":      row.get("staff_full_name", ""),
                "is_active":            row.get("is_active", True),
                "location_id":          row.get("location_id", 0),
                "location_name":        row.get("location_name", ""),
                "period_label":         row.get("period_label", ""),

                "days_with_signin":     row.get("days_with_signin", 0),
                "days_fully_recorded":  row.get("days_fully_recorded", 0),
                "days_missing_signout": row.get("days_missing_signout", 0),
                "total_hours_worked":   row.get("total_hours_worked", 0),
                "avg_hours_per_day":    row.get("avg_hours_per_day"),  # None ok
            })
        return docs