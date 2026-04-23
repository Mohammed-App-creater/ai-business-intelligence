"""
etl/transforms/revenue_etl.py
==============================
Revenue domain ETL extractor.

Pulls all 6 revenue data slices from the analytics backend, writes
them to the warehouse (wh_monthly_revenue, wh_payment_breakdown),
and returns structured documents for immediate embedding into pgvector.

Flow:
    UAT Analytics Backend
        ↓  RevenueExtractor.run()
        ↓  _write_to_warehouse()  ← writes to 2 wh_* tables (when wh_pool set)
    wh_monthly_revenue       ← org rollup + per-location rows
    wh_payment_breakdown     ← cash/card/GC split per org-period
        ↓  docs returned to doc generator → pgvector

Usage (with warehouse write — production path):
    extractor = RevenueExtractor(client=analytics_client, wh_pool=wh_pool)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)

Usage (without warehouse — tests):
    extractor = RevenueExtractor(client=analytics_client)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)
"""

from datetime import date
from calendar import monthrange
import numpy as np
import logging

from app.services.analytics_client import AnalyticsClient

logger = logging.getLogger(__name__)


class RevenueExtractor:
    """
    Pulls and transforms all revenue data for one tenant.

    Parameters
    ----------
    client:   AnalyticsClient — calls the analytics backend API.
    wh_pool:  Optional asyncpg/PGPool — when provided, writes extracted
              rows to wh_monthly_revenue and wh_payment_breakdown before
              returning. When None, the warehouse write is skipped.
    """

    DOMAIN = "revenue"

    def __init__(self, client: AnalyticsClient, wh_pool=None):
        self.client = client
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
        logger.info(
            "Revenue ETL start — business_id=%s %s → %s",
            business_id, start_date, end_date,
        )

        # ── 1. Fetch all 6 slices ────────────────────────────────────────────
        monthly     = await self.client.get_revenue_monthly_summary(business_id, start_date, end_date)
        pay_types   = await self.client.get_revenue_payment_types(business_id, start_date, end_date)
        by_staff    = await self.client.get_revenue_by_staff(business_id, start_date, end_date)
        by_location = await self.client.get_revenue_by_location(business_id, start_date, end_date)
        promos      = await self.client.get_revenue_promo_impact(business_id, start_date, end_date)
        failed      = await self.client.get_revenue_failed_refunds(business_id, start_date, end_date)

        docs: list[dict] = []

        # ── 2. Build docs (same as Step 7) ───────────────────────────────────
        trend_slope = self._compute_trend_slope(monthly)

        for row in monthly:
            docs.append(self._build_monthly_doc(business_id, row, trend_slope))

        if monthly:
            docs.append(self._build_trend_summary_doc(
                business_id, monthly, trend_slope, start_date, end_date,
            ))

        for row in monthly:
            docs.append(self._build_tips_and_extras_doc(business_id, row))

        if pay_types:
            docs.append(self._build_payment_type_doc(
                business_id, pay_types, start_date, end_date,
            ))

        for row in by_staff:
            docs.append(self._build_staff_doc(
                business_id, row, start_date, end_date,
            ))

        for row in by_location:
            docs.append(self._build_location_doc(business_id, row))

        docs.append(self._build_promo_doc(
            business_id, promos, start_date, end_date,
        ))

        if failed:
            docs.append(self._build_failed_doc(
                business_id, failed, start_date, end_date,
            ))

        # ── 3. Write to warehouse (if pool provided) ─────────────────────────
        if self.wh_pool is not None:
            await self._write_to_warehouse(
                business_id, monthly, pay_types, by_location,
            )
        else:
            logger.debug(
                "RevenueExtractor: wh_pool not provided — skipping warehouse write"
            )

        logger.info(
            "Revenue ETL complete — business_id=%s, %d docs produced",
            business_id, len(docs),
        )
        return docs

    # ─────────────────────────────────────────────────────────────────────────
    # Warehouse write
    # ─────────────────────────────────────────────────────────────────────────

    async def _write_to_warehouse(
        self,
        business_id: int,
        monthly: list[dict],
        pay_types: list[dict],
        by_location: list[dict],
    ) -> None:
        """
        Upsert revenue data into the 2 warehouse tables. Idempotent.

        wh_monthly_revenue gets TWO kinds of rows:
          - location_id=0 rows   — org rollup from /monthly-summary
          - location_id>0 rows   — per-location breakdown from /by-location

        wh_payment_breakdown gets ONE row per period at org level
        (location_id=0), prorating window-total payment splits by
        each period's share of visits.
        """
        async with self.wh_pool.acquire() as conn:
            async with conn.transaction():
                await self._upsert_monthly_rollup(conn, business_id, monthly)
                await self._upsert_monthly_locations(conn, business_id, by_location)
                await self._upsert_payment_breakdown(conn, business_id, monthly, pay_types)

        logger.info(
            "RevenueExtractor: warehouse write complete — "
            "monthly_rollup=%d monthly_loc=%d payment_periods=%d",
            len(monthly), len(by_location), len(monthly),
        )

    async def _upsert_monthly_rollup(
        self, conn, business_id: int, monthly: list[dict],
    ) -> None:
        if not monthly:
            return

        sql = """
INSERT INTO wh_monthly_revenue (
    business_id, location_id, period_start, period_end,
    total_revenue, total_tips, total_tax, total_discounts, total_gc_amount,
    gross_revenue, visit_count, successful_visit_count,
    refunded_visit_count, cancelled_visit_count, avg_visit_value,
    cash_revenue, card_revenue, other_revenue
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7, $8, $9,
    $10, $11, $12,
    $13, $14, $15,
    $16, $17, $18
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
"""
        records = []
        for r in monthly:
            ps, pe = self._period_bounds(r.get("period", ""))
            service_rev = float(r.get("service_revenue", 0) or 0)
            tips        = float(r.get("total_tips", 0) or 0)
            tax         = float(r.get("total_tax", 0) or 0)
            discounts   = float(r.get("total_discounts", 0) or 0)
            gc          = float(r.get("gc_redemptions", 0) or 0)
            total_col   = float(r.get("total_collected", 0) or 0)
            visits      = int(r.get("visit_count", 0) or 0)
            refunds     = int(r.get("refund_count", 0) or 0)
            cancels     = int(r.get("cancel_count", 0) or 0)
            avg_ticket  = float(r.get("avg_ticket", 0) or 0)
            # successful ≈ visits minus refunds minus cancels
            successful  = max(0, visits - refunds - cancels)

            records.append((
                business_id,
                0,              # org rollup
                ps,
                pe,
                service_rev,    # total_revenue
                tips,
                tax,
                discounts,
                gc,             # total_gc_amount
                total_col,      # gross_revenue (incl tips+tax)
                visits,
                successful,
                refunds,
                cancels,
                avg_ticket,
                0.0,            # cash_revenue — payment_breakdown table holds the split
                0.0,            # card_revenue
                0.0,            # other_revenue
            ))
        await conn.executemany(sql, records)

    async def _upsert_monthly_locations(
        self, conn, business_id: int, by_location: list[dict],
    ) -> None:
        if not by_location:
            return

        sql = """
INSERT INTO wh_monthly_revenue (
    business_id, location_id, period_start, period_end,
    total_revenue, total_tips, total_discounts, total_gc_amount,
    gross_revenue, visit_count, successful_visit_count, avg_visit_value
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7, $8,
    $9, $10, $11, $12
)
ON CONFLICT (business_id, location_id, period_start) DO UPDATE SET
    period_end             = EXCLUDED.period_end,
    total_revenue          = EXCLUDED.total_revenue,
    total_tips             = EXCLUDED.total_tips,
    total_discounts        = EXCLUDED.total_discounts,
    total_gc_amount        = EXCLUDED.total_gc_amount,
    gross_revenue          = EXCLUDED.gross_revenue,
    visit_count            = EXCLUDED.visit_count,
    successful_visit_count = EXCLUDED.successful_visit_count,
    avg_visit_value        = EXCLUDED.avg_visit_value,
    updated_at             = now()
"""
        records = []
        for r in by_location:
            ps, pe = self._period_bounds(r.get("period", ""))
            loc_id = int(r.get("location_id", 0) or 0)
            if loc_id <= 0:
                continue
            service_rev = float(r.get("service_revenue", 0) or 0)
            tips        = float(r.get("total_tips", 0) or 0)
            discounts   = float(r.get("total_discounts", 0) or 0)
            gc          = float(r.get("gc_redemptions", 0) or 0)
            visits      = int(r.get("visit_count", 0) or 0)
            avg_ticket  = float(r.get("avg_ticket", 0) or 0)
            gross       = service_rev + tips

            records.append((
                business_id,
                loc_id,
                ps,
                pe,
                service_rev,
                tips,
                discounts,
                gc,
                gross,
                visits,
                visits,     # by-location endpoint returns only successful
                avg_ticket,
            ))

        if records:
            await conn.executemany(sql, records)

    async def _upsert_payment_breakdown(
        self,
        conn,
        business_id: int,
        monthly: list[dict],
        pay_types: list[dict],
    ) -> None:
        if not pay_types or not monthly:
            return

        # Aggregate window-total per payment type into canonical buckets
        pt_totals = {
            "cash":      [0.0, 0],
            "card":      [0.0, 0],
            "gift_card": [0.0, 0],
            "other":     [0.0, 0],
        }
        for pt in pay_types:
            ptype = str(pt.get("payment_type", "")).lower()
            revenue = float(pt.get("revenue", 0) or 0)
            count   = int(pt.get("visit_count", 0) or 0)
            if "cash" in ptype:
                k = "cash"
            elif "credit" in ptype or "card" in ptype:
                k = "card"
            elif "gift" in ptype or "giftcard" in ptype:
                k = "gift_card"
            else:
                k = "other"
            pt_totals[k][0] += revenue
            pt_totals[k][1] += count

        total_visits = sum(int(r.get("visit_count", 0) or 0) for r in monthly)
        if total_visits <= 0:
            return

        sql = """
INSERT INTO wh_payment_breakdown (
    business_id, location_id, period_start, period_end,
    cash_amount, cash_count, card_amount, card_count,
    gift_card_amount, gift_card_count, other_amount, other_count,
    total_amount, total_count
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7, $8,
    $9, $10, $11, $12,
    $13, $14
)
ON CONFLICT (business_id, location_id, period_start) DO UPDATE SET
    period_end       = EXCLUDED.period_end,
    cash_amount      = EXCLUDED.cash_amount,
    cash_count       = EXCLUDED.cash_count,
    card_amount      = EXCLUDED.card_amount,
    card_count       = EXCLUDED.card_count,
    gift_card_amount = EXCLUDED.gift_card_amount,
    gift_card_count  = EXCLUDED.gift_card_count,
    other_amount     = EXCLUDED.other_amount,
    other_count      = EXCLUDED.other_count,
    total_amount     = EXCLUDED.total_amount,
    total_count      = EXCLUDED.total_count,
    updated_at       = now()
"""
        records = []
        for r in monthly:
            ps, pe = self._period_bounds(r.get("period", ""))
            visits = int(r.get("visit_count", 0) or 0)
            weight = visits / total_visits if total_visits else 0.0

            cash_amt = round(pt_totals["cash"][0]      * weight, 2)
            cash_cnt = round(pt_totals["cash"][1]      * weight)
            card_amt = round(pt_totals["card"][0]      * weight, 2)
            card_cnt = round(pt_totals["card"][1]      * weight)
            gc_amt   = round(pt_totals["gift_card"][0] * weight, 2)
            gc_cnt   = round(pt_totals["gift_card"][1] * weight)
            oth_amt  = round(pt_totals["other"][0]     * weight, 2)
            oth_cnt  = round(pt_totals["other"][1]     * weight)

            records.append((
                business_id,
                0,                          # org level
                ps,
                pe,
                cash_amt, cash_cnt,
                card_amt, card_cnt,
                gc_amt,   gc_cnt,
                oth_amt,  oth_cnt,
                round(cash_amt + card_amt + gc_amt + oth_amt, 2),
                cash_cnt + card_cnt + gc_cnt + oth_cnt,
            ))
        await conn.executemany(sql, records)

    @staticmethod
    def _period_bounds(period_label: str) -> tuple[date, date]:
        try:
            y = int(period_label[:4])
            m = int(period_label[5:7])
            return date(y, m, 1), date(y, m, monthrange(y, m)[1])
        except (ValueError, IndexError):
            today = date.today()
            return today.replace(day=1), today

    # ─────────────────────────────────────────────────────────────────────────
    # Doc builders  (unchanged from Step 7)
    # ─────────────────────────────────────────────────────────────────────────

    def _build_monthly_doc(
        self, business_id: int, row: dict, trend_slope: float,
    ) -> dict:
        return {
            "tenant_id": business_id,
            "domain": self.DOMAIN,
            "doc_type": "monthly_summary",
            "period": row["period"],
            "period_start": row["period"],
            "visit_count": row.get("visit_count", 0),
            "service_revenue": row.get("service_revenue", 0.0),
            "total_tips": row.get("total_tips", 0.0),
            "total_tax": row.get("total_tax", 0.0),
            "total_collected": row.get("total_collected", 0.0),
            "total_discounts": row.get("total_discounts", 0.0),
            "gc_redemptions": row.get("gc_redemptions", 0.0),
            "avg_ticket": row.get("avg_ticket", 0.0),
            "mom_growth_pct": row.get("mom_growth_pct"),
            "trend_slope": trend_slope,
            "trend_direction": self._direction_label(trend_slope),
            "refund_count": row.get("refund_count", 0),
            "cancel_count": row.get("cancel_count", 0),
            "text": self._monthly_text(row, trend_slope),
        }

    def _build_trend_summary_doc(
        self, business_id: int, monthly_rows: list[dict],
        trend_slope: float, start_date: date, end_date: date,
    ) -> dict:
        direction = self._direction_label(trend_slope)
        direction_word = {"up": "GROWING", "down": "DECLINING", "flat": "FLAT"}[direction]

        mom_lines = []
        for r in monthly_rows:
            period = r.get("period", "?")
            rev    = r.get("service_revenue", 0.0)
            mom    = r.get("mom_growth_pct")
            mom_s  = f" ({mom:+.1f}% MoM)" if mom is not None else " (first period, no MoM)"
            mom_lines.append(f"  {period}: ${rev:,.2f}{mom_s}")

        if direction == "up":
            reconciliation = (
                "IMPORTANT: The overall trend is GROWING even if individual "
                "month-to-month changes look negative in the most recent period. "
                "The linear regression across all months in this window is the "
                "authoritative trend signal — prefer it over any single "
                "month-to-month delta when answering 'is revenue trending up "
                "or down?' type questions."
            )
        elif direction == "down":
            reconciliation = (
                "IMPORTANT: The overall trend is DECLINING across this window. "
                "Any individual month's gain is part of a broader downward "
                "trajectory. The linear regression across all months is the "
                "authoritative trend signal."
            )
        else:
            reconciliation = (
                "IMPORTANT: The overall trend is essentially FLAT. "
                "Individual month spikes or dips cancel out. The linear "
                "regression slope is near zero."
            )

        text = (
            f"Revenue Trend Summary — {start_date} to {end_date}\n\n"
            f"Overall direction: {direction_word}.\n"
            f"Trend slope: {trend_slope:+,.2f} dollars per month "
            f"(linear regression across {len(monthly_rows)} months).\n\n"
            f"Monthly revenue breakdown:\n"
            + "\n".join(mom_lines)
            + f"\n\n{reconciliation}"
        )

        return {
            "tenant_id": business_id,
            "domain": self.DOMAIN,
            "doc_type": "trend_summary",
            "period": f"{start_date} to {end_date}",
            "period_start": start_date.isoformat(),
            "period_end":   end_date.isoformat(),
            "trend_slope":  trend_slope,
            "trend_direction": direction,
            "months_analyzed": len(monthly_rows),
            "text": text,
        }

    def _build_tips_and_extras_doc(self, business_id: int, row: dict) -> dict:
        period    = row.get("period", "?")
        visits    = row.get("visit_count", 0)
        tips      = row.get("total_tips", 0.0)
        tax       = row.get("total_tax", 0.0)
        discounts = row.get("total_discounts", 0.0)
        gc        = row.get("gc_redemptions", 0.0)
        avg_tip   = tips / visits if visits else 0.0

        text = (
            f"Tips and Extra Charges — {period}\n\n"
            f"Tips collected: ${tips:,.2f} "
            f"(average ${avg_tip:.2f} per visit across {visits} visits).\n"
            f"Tax collected: ${tax:,.2f}.\n"
            f"Discounts given (all sources, including manual): ${discounts:,.2f}.\n"
            f"Gift card redemptions: ${gc:,.2f}.\n\n"
            f"NOTE: 'Discounts' here is the gross discount from any source "
            f"(manual or promo). For promo code impact specifically, see "
            f"the promo_impact document."
        )

        return {
            "tenant_id":    business_id,
            "domain":       self.DOMAIN,
            "doc_type":     "tips_and_extras",
            "period":       period,
            "period_start": period,
            "tips":         tips,
            "tax":          tax,
            "discounts":    discounts,
            "gc_redemptions": gc,
            "text":         text,
        }

    def _build_payment_type_doc(
        self, business_id: int, rows: list[dict],
        start_date: date, end_date: date,
    ) -> dict:
        sorted_rows = sorted(rows, key=lambda r: r.get("revenue", 0), reverse=True)
        lines = [
            f"{r['payment_type']}: ${r['revenue']:,.2f} ({r['pct_of_total']}%)"
            for r in sorted_rows
        ]
        return {
            "tenant_id":    business_id,
            "domain":       self.DOMAIN,
            "doc_type":     "payment_type_breakdown",
            "period":       f"{start_date} to {end_date}",
            "period_start": start_date.isoformat(),
            "period_end":   end_date.isoformat(),
            "breakdown":    sorted_rows,
            "text": (
                f"Payment type breakdown from {start_date} to {end_date}: "
                + ", ".join(lines) + ". "
                + f"Top payment method: {sorted_rows[0]['payment_type']} "
                  f"at {sorted_rows[0]['pct_of_total']}% of revenue."
            ),
        }

    def _build_staff_doc(
        self, business_id: int, row: dict,
        start_date: date, end_date: date,
    ) -> dict:
        return {
            "tenant_id":       business_id,
            "domain":          self.DOMAIN,
            "doc_type":        "staff_revenue",
            "period":          f"{start_date} to {end_date}",
            "period_start":    start_date.isoformat(),
            "period_end":      end_date.isoformat(),
            "emp_id":          row.get("emp_id"),
            "staff_name":      row.get("staff_name"),
            "visit_count":     row.get("visit_count", 0),
            "service_revenue": row.get("service_revenue", 0.0),
            "tips_collected":  row.get("tips_collected", 0.0),
            "avg_ticket":      row.get("avg_ticket", 0.0),
            "revenue_rank":    row.get("revenue_rank"),
            "text": (
                f"{row.get('staff_name')} generated ${row.get('service_revenue', 0):,.2f} "
                f"in service revenue from {start_date} to {end_date} "
                f"across {row.get('visit_count', 0)} visits "
                f"(avg ticket ${row.get('avg_ticket', 0):,.2f}, "
                f"tips ${row.get('tips_collected', 0):,.2f}). "
                f"Revenue rank: #{row.get('revenue_rank')}."
            ),
        }

    def _build_location_doc(self, business_id: int, row: dict) -> dict:
        mom = row.get("mom_growth_pct")
        mom_text = f", {mom:+.1f}% vs previous period" if mom is not None else ""
        return {
            "tenant_id":            business_id,
            "domain":               self.DOMAIN,
            "doc_type":             "location_revenue",
            "period":               row.get("period"),
            "period_start":         row.get("period"),
            "location_id":          row.get("location_id"),
            "location_name":        row.get("location_name"),
            "visit_count":          row.get("visit_count", 0),
            "service_revenue":      row.get("service_revenue", 0.0),
            "total_tips":           row.get("total_tips", 0.0),
            "avg_ticket":           row.get("avg_ticket", 0.0),
            "total_discounts":      row.get("total_discounts", 0.0),
            "gc_redemptions":       row.get("gc_redemptions", 0.0),
            "pct_of_total_revenue": row.get("pct_of_total_revenue"),
            "mom_growth_pct":       mom,
            "text": (
                f"{row.get('location_name')} location revenue for {row.get('period')}: "
                f"${row.get('service_revenue', 0):,.2f} "
                f"({row.get('pct_of_total_revenue', 0):.1f}% of total){mom_text}. "
                f"{row.get('visit_count', 0)} visits, avg ticket ${row.get('avg_ticket', 0):,.2f}."
            ),
        }

    def _build_promo_doc(
        self, business_id: int, rows: list[dict],
        start_date: date, end_date: date,
    ) -> dict:
        if not rows:
            text = (
                f"Promo code impact from {start_date} to {end_date}: NONE. "
                f"No promo codes were used during this period. "
                f"Total discount from promo codes: $0.00. "
                f"IMPORTANT: if a user asks 'how much did promos cost me?' "
                f"the answer is $0 for this window. Any 'discounts' shown "
                f"in the monthly revenue summary come from non-promo "
                f"sources (manual discounts, staff adjustments, or system "
                f"credits) and must NOT be reported as promo code cost."
            )
            return {
                "tenant_id":            business_id,
                "domain":               self.DOMAIN,
                "doc_type":             "promo_impact",
                "period":               f"{start_date} to {end_date}",
                "period_start":         start_date.isoformat(),
                "period_end":           end_date.isoformat(),
                "total_discount_given": 0.0,
                "total_promo_uses":     0,
                "has_promo_activity":   False,
                "breakdown":            [],
                "text":                 text,
            }

        total_discount = sum(r.get("total_discount_given", 0) for r in rows)
        total_uses     = sum(r.get("times_used", 0) for r in rows)
        lines = [
            f"'{r['promo_code']}' used {r['times_used']}x = ${r['total_discount_given']:,.2f} off"
            for r in rows
        ]
        return {
            "tenant_id":            business_id,
            "domain":               self.DOMAIN,
            "doc_type":             "promo_impact",
            "period":               f"{start_date} to {end_date}",
            "period_start":         start_date.isoformat(),
            "period_end":           end_date.isoformat(),
            "total_discount_given": total_discount,
            "total_promo_uses":     total_uses,
            "has_promo_activity":   True,
            "breakdown":            rows,
            "text": (
                f"Promo code impact from {start_date} to {end_date}: "
                f"total discount given from promo codes = ${total_discount:,.2f} "
                f"across {total_uses} uses. "
                + " | ".join(lines) + "."
            ),
        }

    def _build_failed_doc(
        self, business_id: int, rows: list[dict],
        start_date: date, end_date: date,
    ) -> dict:
        total_lost   = sum(r.get("lost_revenue", 0) for r in rows)
        total_visits = sum(r.get("visit_count", 0) for r in rows)
        lines = [
            f"{r['status_label']}: {r['visit_count']} visits = ${r['lost_revenue']:,.2f}"
            for r in rows
        ]
        return {
            "tenant_id":             business_id,
            "domain":                self.DOMAIN,
            "doc_type":              "failed_refunds",
            "period":                f"{start_date} to {end_date}",
            "period_start":          start_date.isoformat(),
            "period_end":            end_date.isoformat(),
            "total_lost_revenue":    total_lost,
            "total_affected_visits": total_visits,
            "breakdown":             rows,
            "text": (
                f"Failed and refunded visits from {start_date} to {end_date}: "
                f"{total_visits} affected visits, ${total_lost:,.2f} in "
                f"lost/reversed revenue. "
                + " | ".join(lines) + ". "
                f"Note: no-show cost (tbl_calendarevent) is a known gap — "
                f"not included here."
            ),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Computed helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _compute_trend_slope(rows: list[dict]) -> float:
        revenues = [r.get("service_revenue", 0.0) for r in rows]
        if len(revenues) < 2:
            return 0.0
        x = np.arange(len(revenues), dtype=float)
        y = np.array(revenues, dtype=float)
        x_mean, y_mean = x.mean(), y.mean()
        numerator   = ((x - x_mean) * (y - y_mean)).sum()
        denominator = ((x - x_mean) ** 2).sum()
        return round(
            float(numerator / denominator) if denominator != 0 else 0.0,
            2,
        )

    @staticmethod
    def _direction_label(slope: float) -> str:
        if slope > 0:
            return "up"
        if slope < 0:
            return "down"
        return "flat"

    @staticmethod
    def _monthly_text(row: dict, trend_slope: float) -> str:
        mom = row.get("mom_growth_pct")
        mom_str = f", {mom:+.1f}% vs previous month" if mom is not None else ""

        direction = RevenueExtractor._direction_label(trend_slope)
        direction_word = {"up": "GROWING", "down": "DECLINING", "flat": "FLAT"}[direction]
        trend_str = (
            f"Overall multi-month revenue trend: {direction_word} "
            f"(slope {trend_slope:+,.2f}/month). "
            f"See trend_summary document for the full window analysis."
        )

        return (
            f"Revenue for {row['period']}: "
            f"${row.get('service_revenue', 0):,.2f} service revenue{mom_str}. "
            f"{row.get('visit_count', 0)} visits, "
            f"avg ticket ${row.get('avg_ticket', 0):,.2f}. "
            f"Tips: ${row.get('total_tips', 0):,.2f}. "
            f"Tax collected: ${row.get('total_tax', 0):,.2f}. "
            f"Discounts given: ${row.get('total_discounts', 0):,.2f}. "
            f"Gift card redemptions: ${row.get('gc_redemptions', 0):,.2f}. "
            f"Refunds: {row.get('refund_count', 0)}, "
            f"cancellations: {row.get('cancel_count', 0)}. "
            f"{trend_str}"
        )