"""
etl/transforms/expenses_etl.py
===============================
Expenses domain ETL extractor.

Pulls all 6 expense data slices from the analytics backend,
writes them to the warehouse (wh_exp_* tables), and returns
structured documents for immediate use by the doc generator.

Flow:
    Mock Server / Analytics Backend
        ↓  ExpensesExtractor.run()
        ↓  _write_to_warehouse()
    wh_exp_monthly_summary
    wh_exp_category_breakdown
    wh_exp_subcategory_breakdown
    wh_exp_location_breakdown
    wh_exp_payment_type_breakdown
    wh_exp_staff_attribution
    wh_exp_category_location_cross
        ↓  returned to doc generator → pgvector

Usage:
    extractor = ExpensesExtractor(client=analytics_client, wh_pool=wh_pool)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Optional

from app.services.analytics_client import AnalyticsClient

logger = logging.getLogger(__name__)


class ExpensesExtractor:
    """
    Pulls and transforms all expense data for one tenant.

    Parameters
    ----------
    client:  AnalyticsClient — calls the analytics backend API.
    wh_pool: Optional asyncpg/PGPool — when provided, writes extracted
             rows to the warehouse before returning. When None, the
             warehouse write is skipped (useful in tests).
    """

    DOMAIN = "expenses"

    def __init__(self, client: AnalyticsClient, wh_pool=None):
        self.client = client
        self.wh_pool = wh_pool

    # ─────────────────────────────────────────────────────────────────────
    # Public entry point
    # ─────────────────────────────────────────────────────────────────────

    async def run(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> dict[str, list[dict]]:
        """
        Fetch all 6 slices in parallel, transform, write to warehouse,
        return as a dict keyed by slice name.

        Returns
        -------
        dict with keys:
            monthly_summary, category_breakdown, subcategory_breakdown,
            location_breakdown, payment_type_breakdown, staff_attribution,
            category_location_cross
        Each value is a list[dict] ready for the doc generator.
        """
        logger.info(
            "ExpensesExtractor: business_id=%s %s → %s",
            business_id, start_date, end_date,
        )

        # ── 1. Fetch 6 slices in parallel ────────────────────────────────
        # category-breakdown is fetched WITH include_subcategories=True so
        # we get the subcategory data in the same call. We then split it
        # into 2 tables (category-level rows + subcategory-level rows).
        (
            monthly_raw,
            category_raw,
            location_raw,
            payment_raw,
            staff_raw,
            cross_raw,
        ) = await asyncio.gather(
            self.client.get_expenses_monthly_summary(
                business_id, start_date, end_date,
            ),
            self.client.get_expenses_category_breakdown(
                business_id, start_date, end_date, include_subcategories=True,
            ),
            self.client.get_expenses_location_breakdown(
                business_id, start_date, end_date,
            ),
            self.client.get_expenses_payment_type_breakdown(
                business_id, start_date, end_date,
            ),
            self.client.get_expenses_staff_attribution(
                business_id, start_date, end_date,
            ),
            self.client.get_expenses_category_location_cross(
                business_id, start_date, end_date,
            ),
        )

        # ── 2. Transform ─────────────────────────────────────────────────
        monthly_rows   = self._transform_monthly_summary(business_id, monthly_raw)
        cat_rows, sub_rows = self._transform_category_breakdown(
            business_id, category_raw,
        )
        location_rows  = self._transform_location_breakdown(business_id, location_raw)
        payment_rows   = self._transform_payment_type(business_id, payment_raw)
        staff_rows     = self._transform_staff_attribution(business_id, staff_raw)
        cross_rows     = self._transform_category_location_cross(business_id, cross_raw)

        # ── 3. Warehouse write (if pool provided) ────────────────────────
        if self.wh_pool is not None:
            await self._write_to_warehouse(
                business_id,
                monthly_rows, cat_rows, sub_rows,
                location_rows, payment_rows, staff_rows, cross_rows,
            )
        else:
            logger.debug(
                "ExpensesExtractor: wh_pool not provided — skipping warehouse write"
            )

        logger.info(
            "ExpensesExtractor: business_id=%s produced "
            "monthly=%d category=%d subcat=%d location=%d payment=%d staff=%d cross=%d",
            business_id,
            len(monthly_rows), len(cat_rows), len(sub_rows),
            len(location_rows), len(payment_rows),
            len(staff_rows), len(cross_rows),
        )

        return {
            "monthly_summary":          monthly_rows,
            "category_breakdown":       cat_rows,
            "subcategory_breakdown":    sub_rows,
            "location_breakdown":       location_rows,
            "payment_type_breakdown":   payment_rows,
            "staff_attribution":        staff_rows,
            "category_location_cross":  cross_rows,
        }

    # ─────────────────────────────────────────────────────────────────────
    # Transforms — one per slice. Stamp business_id, normalize NULLs.
    # ─────────────────────────────────────────────────────────────────────

    def _transform_monthly_summary(
        self, business_id: int, raw,
    ) -> list[dict]:
        """
        Stamp business_id, normalize. Input is either the raw envelope
        {business_id, period_start, ..., data: [...]} or the data list
        directly (depending on how _post is configured — most domains
        already unwrap to `data`, but defensive here).
        """
        rows = self._unwrap(raw)
        out = []
        for r in rows:
            out.append({
                "business_id":             business_id,
                "period":                  self._to_date(r["period"]),
                "total_expenses":          float(r.get("total_expenses") or 0),
                "transaction_count":       int(r.get("transaction_count") or 0),
                "avg_transaction":         float(r.get("avg_transaction") or 0),
                "min_transaction":         float(r.get("min_transaction") or 0),
                "max_transaction":         float(r.get("max_transaction") or 0),
                "prev_month_expenses":     self._nullable_float(r.get("prev_month_expenses")),
                "mom_change_pct":          self._nullable_float(r.get("mom_change_pct")),
                "mom_direction":           r.get("mom_direction"),
                "ytd_total":               float(r.get("ytd_total") or 0),
                "window_cumulative":       float(r.get("window_cumulative") or 0),
                "current_quarter_total":   self._nullable_float(r.get("current_quarter_total")),
                "prev_quarter_total":      self._nullable_float(r.get("prev_quarter_total")),
                "qoq_change_pct":          self._nullable_float(r.get("qoq_change_pct")),
                "expense_rank_in_window":  int(r.get("expense_rank_in_window") or 0),
                "avg_monthly_in_window":   float(r.get("avg_monthly_in_window") or 0),
                "months_in_window":        int(r.get("months_in_window") or 0),
                "large_txn_count":         int(r.get("large_txn_count") or 0),
                "huge_txn_count":          int(r.get("huge_txn_count") or 0),
            })
        return out

    def _transform_category_breakdown(
        self, business_id: int, raw,
    ) -> tuple[list[dict], list[dict]]:
        """
        Split the category-breakdown response into two lists:
          1. cat_rows — category-level rows (always)
          2. sub_rows — subcategory rows (only when the API returned
                        subcategory_breakdown on the category row)

        This way the warehouse has normalized tables rather than a
        nested blob. Doc generator consumes sub_rows separately for Q13.
        """
        rows = self._unwrap(raw)
        cat_rows = []
        sub_rows = []

        for r in rows:
            cat_rows.append({
                "business_id":                business_id,
                "period":                     self._to_date(r["period"]),
                "category_id":                int(r["category_id"]),
                "category_name":              r.get("category_name") or "Uncategorized",
                "category_total":             float(r.get("category_total") or 0),
                "transaction_count":          int(r.get("transaction_count") or 0),
                "month_total":                float(r.get("month_total") or 0),
                "pct_of_month":               float(r.get("pct_of_month") or 0),
                "rank_in_month":              int(r.get("rank_in_month") or 0),
                "prev_month_total":           self._nullable_float(r.get("prev_month_total")),
                "mom_change_pct":             self._nullable_float(r.get("mom_change_pct")),
                "baseline_3mo_avg":           self._nullable_float(r.get("baseline_3mo_avg")),
                "baseline_months_available":  int(r.get("baseline_months_available") or 0),
                "pct_vs_baseline":            self._nullable_float(r.get("pct_vs_baseline")),
                "anomaly_flag":               r.get("anomaly_flag"),  # may be None
            })

            # Optional nested subcategories — flatten into sub_rows
            sub_list = r.get("subcategory_breakdown") or []
            for s in sub_list:
                sub_rows.append({
                    "business_id":           business_id,
                    "period":                self._to_date(r["period"]),
                    "category_id":           int(r["category_id"]),
                    "category_name":         r.get("category_name") or "Uncategorized",
                    "subcategory_id":        int(s["subcategory_id"]),
                    "subcategory_name":      s.get("subcategory_name") or "Unspecified",
                    "subcategory_total":     float(s.get("subcategory_total") or 0),
                    "transaction_count":     int(s.get("transaction_count") or 0),
                    "rank_in_category":      int(s.get("rank_in_category") or 0),
                })

        return cat_rows, sub_rows

    def _transform_location_breakdown(
        self, business_id: int, raw,
    ) -> list[dict]:
        rows = self._unwrap(raw)
        out = []
        for r in rows:
            out.append({
                "business_id":       business_id,
                "period":            self._to_date(r["period"]),
                "location_id":       int(r["location_id"]),
                "location_name":     r.get("location_name") or "Unknown",
                "location_total":    float(r.get("location_total") or 0),
                "transaction_count": int(r.get("transaction_count") or 0),
                "month_total":       float(r.get("month_total") or 0),
                "pct_of_month":      float(r.get("pct_of_month") or 0),
                "rank_in_month":     int(r.get("rank_in_month") or 0),
                "prev_month_total":  self._nullable_float(r.get("prev_month_total")),
                "mom_change_pct":    self._nullable_float(r.get("mom_change_pct")),
            })
        return out

    def _transform_payment_type(
        self, business_id: int, raw,
    ) -> list[dict]:
        rows = self._unwrap(raw)
        out = []
        for r in rows:
            out.append({
                "business_id":         business_id,
                "period":              self._to_date(r["period"]),
                "payment_type_code":   int(r["payment_type_code"]),
                "payment_type_label":  r.get("payment_type_label") or f"Type {r['payment_type_code']}",
                "type_total":          float(r.get("type_total") or 0),
                "transaction_count":   int(r.get("transaction_count") or 0),
                "month_total":         float(r.get("month_total") or 0),
                "pct_of_month":        float(r.get("pct_of_month") or 0),
            })
        return out

    def _transform_staff_attribution(
        self, business_id: int, raw,
    ) -> list[dict]:
        """
        Keep the aggregate counts. total_amount_logged is kept in the
        warehouse row (it's useful for ops) but the doc generator does
        NOT embed it into RAG chunks — per-person dollar values are
        borderline surveillance.
        """
        rows = self._unwrap(raw)
        out = []
        for r in rows:
            out.append({
                "business_id":         business_id,
                "period":              self._to_date(r["period"]),
                "employee_id":         int(r["employee_id"]),
                "employee_name":       r.get("employee_name") or "Unknown",
                "entries_logged":      int(r.get("entries_logged") or 0),
                "total_amount_logged": float(r.get("total_amount_logged") or 0),
                "rank_in_month":       int(r.get("rank_in_month") or 0),
            })
        return out

    def _transform_category_location_cross(
        self, business_id: int, raw,
    ) -> list[dict]:
        rows = self._unwrap(raw)
        out = []
        for r in rows:
            out.append({
                "business_id":             business_id,
                "period":                  self._to_date(r["period"]),
                "location_id":             int(r["location_id"]),
                "location_name":           r.get("location_name") or "Unknown",
                "category_id":             int(r["category_id"]),
                "category_name":           r.get("category_name") or "Uncategorized",
                "cross_total":             float(r.get("cross_total") or 0),
                "transaction_count":       int(r.get("transaction_count") or 0),
                "pct_of_location_month":   float(r.get("pct_of_location_month") or 0),
                "rank_in_location_month":  int(r.get("rank_in_location_month") or 0),
            })
        return out

    # ─────────────────────────────────────────────────────────────────────
    # Warehouse write — 7 idempotent upserts, one per table
    # ─────────────────────────────────────────────────────────────────────

    async def _write_to_warehouse(
        self,
        business_id: int,
        monthly_rows, cat_rows, sub_rows,
        location_rows, payment_rows, staff_rows, cross_rows,
    ) -> None:
        """
        Upsert all 7 tables inside a single transaction. Atomic — either
        the whole write lands or none of it does (consistent with how
        ClientsExtractor + MarketingExtractor handle it).
        """
        async with self.wh_pool.acquire() as conn:
            async with conn.transaction():
                # Wipe this tenant's existing expenses rows for the window
                # being written. Prevents stale rows from earlier runs.
                # Each domain ETL owns its own tables; this is safe.
                if monthly_rows:
                    periods = sorted({r["period"] for r in monthly_rows})
                    p_min, p_max = periods[0], periods[-1]
                    for tbl in (
                        "wh_exp_monthly_summary",
                        "wh_exp_category_breakdown",
                        "wh_exp_subcategory_breakdown",
                        "wh_exp_location_breakdown",
                        "wh_exp_payment_type_breakdown",
                        "wh_exp_staff_attribution",
                        "wh_exp_category_location_cross",
                    ):
                        await conn.execute(
                            f"DELETE FROM {tbl} "
                            f"WHERE business_id = $1 AND period BETWEEN $2 AND $3",
                            business_id, p_min, p_max,
                        )

                await self._upsert_monthly_summary(conn, monthly_rows)
                await self._upsert_category_breakdown(conn, cat_rows)
                await self._upsert_subcategory_breakdown(conn, sub_rows)
                await self._upsert_location_breakdown(conn, location_rows)
                await self._upsert_payment_type_breakdown(conn, payment_rows)
                await self._upsert_staff_attribution(conn, staff_rows)
                await self._upsert_category_location_cross(conn, cross_rows)

        logger.info(
            "warehouse write complete — monthly=%d category=%d subcat=%d "
            "location=%d payment=%d staff=%d cross=%d",
            len(monthly_rows), len(cat_rows), len(sub_rows),
            len(location_rows), len(payment_rows),
            len(staff_rows), len(cross_rows),
        )

    async def _upsert_monthly_summary(self, conn, rows):
        if not rows:
            return
        await conn.executemany(
            """
            INSERT INTO wh_exp_monthly_summary (
                business_id, period, total_expenses, transaction_count,
                avg_transaction, min_transaction, max_transaction,
                prev_month_expenses, mom_change_pct, mom_direction,
                ytd_total, window_cumulative,
                current_quarter_total, prev_quarter_total, qoq_change_pct,
                expense_rank_in_window, avg_monthly_in_window, months_in_window,
                large_txn_count, huge_txn_count
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
            )
            ON CONFLICT (business_id, period) DO UPDATE SET
                total_expenses          = EXCLUDED.total_expenses,
                transaction_count       = EXCLUDED.transaction_count,
                avg_transaction         = EXCLUDED.avg_transaction,
                min_transaction         = EXCLUDED.min_transaction,
                max_transaction         = EXCLUDED.max_transaction,
                prev_month_expenses     = EXCLUDED.prev_month_expenses,
                mom_change_pct          = EXCLUDED.mom_change_pct,
                mom_direction           = EXCLUDED.mom_direction,
                ytd_total               = EXCLUDED.ytd_total,
                window_cumulative       = EXCLUDED.window_cumulative,
                current_quarter_total   = EXCLUDED.current_quarter_total,
                prev_quarter_total      = EXCLUDED.prev_quarter_total,
                qoq_change_pct          = EXCLUDED.qoq_change_pct,
                expense_rank_in_window  = EXCLUDED.expense_rank_in_window,
                avg_monthly_in_window   = EXCLUDED.avg_monthly_in_window,
                months_in_window        = EXCLUDED.months_in_window,
                large_txn_count         = EXCLUDED.large_txn_count,
                huge_txn_count          = EXCLUDED.huge_txn_count
            """,
            [
                (
                    r["business_id"], r["period"], r["total_expenses"], r["transaction_count"],
                    r["avg_transaction"], r["min_transaction"], r["max_transaction"],
                    r["prev_month_expenses"], r["mom_change_pct"], r["mom_direction"],
                    r["ytd_total"], r["window_cumulative"],
                    r["current_quarter_total"], r["prev_quarter_total"], r["qoq_change_pct"],
                    r["expense_rank_in_window"], r["avg_monthly_in_window"], r["months_in_window"],
                    r["large_txn_count"], r["huge_txn_count"],
                )
                for r in rows
            ],
        )

    async def _upsert_category_breakdown(self, conn, rows):
        if not rows:
            return
        await conn.executemany(
            """
            INSERT INTO wh_exp_category_breakdown (
                business_id, period, category_id, category_name,
                category_total, transaction_count, month_total, pct_of_month,
                rank_in_month, prev_month_total, mom_change_pct,
                baseline_3mo_avg, baseline_months_available,
                pct_vs_baseline, anomaly_flag
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
            )
            ON CONFLICT (business_id, period, category_id) DO UPDATE SET
                category_name             = EXCLUDED.category_name,
                category_total            = EXCLUDED.category_total,
                transaction_count         = EXCLUDED.transaction_count,
                month_total               = EXCLUDED.month_total,
                pct_of_month              = EXCLUDED.pct_of_month,
                rank_in_month             = EXCLUDED.rank_in_month,
                prev_month_total          = EXCLUDED.prev_month_total,
                mom_change_pct            = EXCLUDED.mom_change_pct,
                baseline_3mo_avg          = EXCLUDED.baseline_3mo_avg,
                baseline_months_available = EXCLUDED.baseline_months_available,
                pct_vs_baseline           = EXCLUDED.pct_vs_baseline,
                anomaly_flag              = EXCLUDED.anomaly_flag
            """,
            [
                (
                    r["business_id"], r["period"], r["category_id"], r["category_name"],
                    r["category_total"], r["transaction_count"], r["month_total"], r["pct_of_month"],
                    r["rank_in_month"], r["prev_month_total"], r["mom_change_pct"],
                    r["baseline_3mo_avg"], r["baseline_months_available"],
                    r["pct_vs_baseline"], r["anomaly_flag"],
                )
                for r in rows
            ],
        )

    async def _upsert_subcategory_breakdown(self, conn, rows):
        if not rows:
            return
        await conn.executemany(
            """
            INSERT INTO wh_exp_subcategory_breakdown (
                business_id, period, category_id, category_name,
                subcategory_id, subcategory_name, subcategory_total,
                transaction_count, rank_in_category
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9
            )
            ON CONFLICT (business_id, period, category_id, subcategory_id) DO UPDATE SET
                category_name     = EXCLUDED.category_name,
                subcategory_name  = EXCLUDED.subcategory_name,
                subcategory_total = EXCLUDED.subcategory_total,
                transaction_count = EXCLUDED.transaction_count,
                rank_in_category  = EXCLUDED.rank_in_category
            """,
            [
                (
                    r["business_id"], r["period"], r["category_id"], r["category_name"],
                    r["subcategory_id"], r["subcategory_name"], r["subcategory_total"],
                    r["transaction_count"], r["rank_in_category"],
                )
                for r in rows
            ],
        )

    async def _upsert_location_breakdown(self, conn, rows):
        if not rows:
            return
        await conn.executemany(
            """
            INSERT INTO wh_exp_location_breakdown (
                business_id, period, location_id, location_name,
                location_total, transaction_count, month_total, pct_of_month,
                rank_in_month, prev_month_total, mom_change_pct
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
            )
            ON CONFLICT (business_id, period, location_id) DO UPDATE SET
                location_name     = EXCLUDED.location_name,
                location_total    = EXCLUDED.location_total,
                transaction_count = EXCLUDED.transaction_count,
                month_total       = EXCLUDED.month_total,
                pct_of_month      = EXCLUDED.pct_of_month,
                rank_in_month     = EXCLUDED.rank_in_month,
                prev_month_total  = EXCLUDED.prev_month_total,
                mom_change_pct    = EXCLUDED.mom_change_pct
            """,
            [
                (
                    r["business_id"], r["period"], r["location_id"], r["location_name"],
                    r["location_total"], r["transaction_count"], r["month_total"], r["pct_of_month"],
                    r["rank_in_month"], r["prev_month_total"], r["mom_change_pct"],
                )
                for r in rows
            ],
        )

    async def _upsert_payment_type_breakdown(self, conn, rows):
        if not rows:
            return
        await conn.executemany(
            """
            INSERT INTO wh_exp_payment_type_breakdown (
                business_id, period, payment_type_code, payment_type_label,
                type_total, transaction_count, month_total, pct_of_month
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            ON CONFLICT (business_id, period, payment_type_code) DO UPDATE SET
                payment_type_label = EXCLUDED.payment_type_label,
                type_total         = EXCLUDED.type_total,
                transaction_count  = EXCLUDED.transaction_count,
                month_total        = EXCLUDED.month_total,
                pct_of_month       = EXCLUDED.pct_of_month
            """,
            [
                (
                    r["business_id"], r["period"], r["payment_type_code"], r["payment_type_label"],
                    r["type_total"], r["transaction_count"], r["month_total"], r["pct_of_month"],
                )
                for r in rows
            ],
        )

    async def _upsert_staff_attribution(self, conn, rows):
        if not rows:
            return
        await conn.executemany(
            """
            INSERT INTO wh_exp_staff_attribution (
                business_id, period, employee_id, employee_name,
                entries_logged, total_amount_logged, rank_in_month
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7
            )
            ON CONFLICT (business_id, period, employee_id) DO UPDATE SET
                employee_name       = EXCLUDED.employee_name,
                entries_logged      = EXCLUDED.entries_logged,
                total_amount_logged = EXCLUDED.total_amount_logged,
                rank_in_month       = EXCLUDED.rank_in_month
            """,
            [
                (
                    r["business_id"], r["period"], r["employee_id"], r["employee_name"],
                    r["entries_logged"], r["total_amount_logged"], r["rank_in_month"],
                )
                for r in rows
            ],
        )

    async def _upsert_category_location_cross(self, conn, rows):
        if not rows:
            return
        await conn.executemany(
            """
            INSERT INTO wh_exp_category_location_cross (
                business_id, period, location_id, location_name,
                category_id, category_name,
                cross_total, transaction_count,
                pct_of_location_month, rank_in_location_month
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
            )
            ON CONFLICT (business_id, period, location_id, category_id) DO UPDATE SET
                location_name          = EXCLUDED.location_name,
                category_name          = EXCLUDED.category_name,
                cross_total            = EXCLUDED.cross_total,
                transaction_count      = EXCLUDED.transaction_count,
                pct_of_location_month  = EXCLUDED.pct_of_location_month,
                rank_in_location_month = EXCLUDED.rank_in_location_month
            """,
            [
                (
                    r["business_id"], r["period"], r["location_id"], r["location_name"],
                    r["category_id"], r["category_name"],
                    r["cross_total"], r["transaction_count"],
                    r["pct_of_location_month"], r["rank_in_location_month"],
                )
                for r in rows
            ],
        )

    # ─────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _unwrap(raw):
        """
        Accept either the envelope dict {business_id, data: [...]}
        or the data list directly. Marketing/Clients ETLs did the same —
        defensive against analytics_client helper changes.
        """
        if isinstance(raw, dict) and "data" in raw:
            return raw["data"]
        if isinstance(raw, list):
            return raw
        return []

    @staticmethod
    def _nullable_float(v):
        """Keep NULL as None; coerce numerics to float. Honest about missing data."""
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_date(v):
        """
        Coerce a period value to datetime.date for asyncpg parameter binding.
        Accepts:
          - datetime.date (passes through)
          - datetime.datetime (uses .date())
          - 'YYYY-MM-DD' or longer ISO string (parses first 10 chars)
          - None (returns None — for nullable columns)
        Raises ValueError on anything else, so bad data fails loudly at the
        transform boundary rather than silently corrupting warehouse rows.
        """
        if v is None:
            return None
        from datetime import date as _date, datetime as _dt
        if isinstance(v, _dt):
            return v.date()
        if isinstance(v, _date):
            return v
        if isinstance(v, str):
            return _date.fromisoformat(v[:10])
        raise ValueError(f"_to_date: unsupported period value type {type(v).__name__}: {v!r}")