"""
LEO AI BI — Revenue ETL Extractor
Revenue Domain — Step 7 Refinement (post Step 6 findings)

Pulls all 6 revenue data slices from the analytics backend, computes
warehouse-layer fields, and returns structured documents ready for
embedding into pgvector.

Step 7 changes (2026-04-22):
  + New doc_type 'trend_summary' — one per period window, answers
    "is my revenue trending up or down?" by quoting the backend's
    trend_slope directly instead of letting the AI recompute M-to-M deltas.
  + New doc_type 'tips_and_extras' — one per month, surfaces tips/tax/
    discounts/gc_redemptions as retrievable chunks (was buried in a
    comma-separated list inside monthly_summary, so RAG kept missing it).
  + promo_impact doc now emits even when backend returns zero rows,
    with explicit "NONE" text that disambiguates promo codes from
    general discounts (was causing the AI to quote total_discounts
    as promo cost).
  + Every doc now carries period_start in metadata for future filtering.

Usage:
    extractor = RevenueExtractor(client=analytics_client)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)
"""

from datetime import date
import numpy as np
import logging

from app.services.analytics_client import AnalyticsClient

logger = logging.getLogger(__name__)


class RevenueExtractor:
    """
    Pulls and transforms all revenue data for one tenant.
    Output: a list of structured embedding documents.
    """

    DOMAIN = "revenue"

    def __init__(self, client: AnalyticsClient):
        self.client = client

    async def run(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        Main entry point. Fetches all 6 slices and returns warehouse-ready
        documents for this tenant.
        """
        logger.info(
            "Revenue ETL start — business_id=%s %s → %s",
            business_id, start_date, end_date,
        )

        # Fetch all 6 slices (parallelise with asyncio.gather once stable)
        monthly     = await self.client.get_revenue_monthly_summary(business_id, start_date, end_date)
        pay_types   = await self.client.get_revenue_payment_types(business_id, start_date, end_date)
        by_staff    = await self.client.get_revenue_by_staff(business_id, start_date, end_date)
        by_location = await self.client.get_revenue_by_location(business_id, start_date, end_date)
        promos      = await self.client.get_revenue_promo_impact(business_id, start_date, end_date)
        failed      = await self.client.get_revenue_failed_refunds(business_id, start_date, end_date)

        docs: list[dict] = []

        # ── 1. Monthly summary — one doc per period ───────────────────────────
        trend_slope = self._compute_trend_slope(monthly)
        for row in monthly:
            docs.append(self._build_monthly_doc(business_id, row, trend_slope))

        # ── 1b. NEW: Trend summary — one doc for the whole window ─────────────
        # Answers "overall trend" questions with an unambiguous verdict instead
        # of letting the AI compute a 2-month delta and call that the trend.
        if monthly:
            docs.append(self._build_trend_summary_doc(
                business_id, monthly, trend_slope, start_date, end_date,
            ))

        # ── 1c. NEW: Tips & extras — one doc per month ────────────────────────
        # Tips, tax, discounts, gift-card redemptions as their own retrievable
        # chunks. In monthly_summary these were buried in a comma-separated
        # list, so embedding retrieval kept missing them.
        for row in monthly:
            docs.append(self._build_tips_and_extras_doc(business_id, row))

        # ── 2. Payment type summary (one aggregate doc) ──────────────────────
        if pay_types:
            docs.append(self._build_payment_type_doc(
                business_id, pay_types, start_date, end_date,
            ))

        # ── 3. Staff revenue — one doc per staff member ──────────────────────
        for row in by_staff:
            docs.append(self._build_staff_doc(
                business_id, row, start_date, end_date,
            ))

        # ── 4. Location revenue — one doc per location per period ────────────
        for row in by_location:
            docs.append(self._build_location_doc(business_id, row))

        # ── 5. Promo impact (always emit, even when empty) ───────────────────
        # Step 7 change: was `if promos: ...`. An empty response used to
        # produce NO doc, so RAG would grab the next-best chunk (monthly
        # total_discounts) and the AI would confuse manual discounts with
        # promo code cost. Now we emit a "NONE" chunk with disambiguation.
        docs.append(self._build_promo_doc(
            business_id, promos, start_date, end_date,
        ))

        # ── 6. Failed / refunded visits (one aggregate doc) ──────────────────
        if failed:
            docs.append(self._build_failed_doc(
                business_id, failed, start_date, end_date,
            ))

        logger.info(
            "Revenue ETL complete — business_id=%s, %d docs produced",
            business_id, len(docs),
        )
        return docs

    # ── Document builders ────────────────────────────────────────────────────

    def _build_monthly_doc(
        self, business_id: int, row: dict, trend_slope: float,
    ) -> dict:
        """One document per period — the core revenue time-series row."""
        return {
            "tenant_id": business_id,
            "domain": self.DOMAIN,
            "doc_type": "monthly_summary",
            "period": row["period"],
            "period_start": row["period"],     # for retrieval filtering
            # Core metrics
            "visit_count": row.get("visit_count", 0),
            "service_revenue": row.get("service_revenue", 0.0),
            "total_tips": row.get("total_tips", 0.0),
            "total_tax": row.get("total_tax", 0.0),
            "total_collected": row.get("total_collected", 0.0),
            "total_discounts": row.get("total_discounts", 0.0),
            "gc_redemptions": row.get("gc_redemptions", 0.0),
            "avg_ticket": row.get("avg_ticket", 0.0),
            # Computed
            "mom_growth_pct": row.get("mom_growth_pct"),
            "trend_slope": trend_slope,
            "trend_direction": self._direction_label(trend_slope),
            # Edge-case counts
            "refund_count": row.get("refund_count", 0),
            "cancel_count": row.get("cancel_count", 0),
            # Embedding text
            "text": self._monthly_text(row, trend_slope),
        }

    def _build_trend_summary_doc(
        self,
        business_id: int,
        monthly_rows: list[dict],
        trend_slope: float,
        start_date: date,
        end_date: date,
    ) -> dict:
        """
        Step 7 NEW: an overall-trend answer chunk.

        Key insight from Step 6: the AI was computing its own trend from
        the latest month-to-month delta (e.g. Feb→Mar -83.9%) and
        reporting "declining" even when the backend's regression says
        "+3949.55 slope / growing". This doc forces the AI to quote the
        backend's verdict and includes the reconciliation rationale so
        the AI doesn't contradict it.
        """
        direction = self._direction_label(trend_slope)
        direction_word = {
            "up":   "GROWING",
            "down": "DECLINING",
            "flat": "FLAT",
        }[direction]

        # Build per-month mini-summary for context
        mom_lines = []
        for i, r in enumerate(monthly_rows):
            period = r.get("period", "?")
            rev    = r.get("service_revenue", 0.0)
            mom    = r.get("mom_growth_pct")
            mom_s  = f" ({mom:+.1f}% MoM)" if mom is not None else " (first period, no MoM)"
            mom_lines.append(f"  {period}: ${rev:,.2f}{mom_s}")

        # Reconciliation text — teaches the AI to prefer slope over MoM
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
            f"Revenue Trend Summary — {start_date} to {end_date}\n"
            f"\n"
            f"Overall direction: {direction_word}.\n"
            f"Trend slope: {trend_slope:+,.2f} dollars per month "
            f"(linear regression across {len(monthly_rows)} months).\n"
            f"\n"
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
        """
        Step 7 NEW: per-month chunk for tips, tax, discounts, GC redemptions.

        Key insight from Step 6: Q_TIPS reliably got "insufficient data"
        even though tips were in monthly_summary — buried in a
        comma-separated list. This focused chunk puts the number up
        front so embedding-based retrieval actually finds it.
        """
        period       = row.get("period", "?")
        visits       = row.get("visit_count", 0)
        tips         = row.get("total_tips", 0.0)
        tax          = row.get("total_tax", 0.0)
        discounts    = row.get("total_discounts", 0.0)
        gc           = row.get("gc_redemptions", 0.0)
        avg_tip      = tips / visits if visits else 0.0

        text = (
            f"Tips and Extra Charges — {period}\n"
            f"\n"
            f"Tips collected: ${tips:,.2f} "
            f"(average ${avg_tip:.2f} per visit across {visits} visits).\n"
            f"Tax collected: ${tax:,.2f}.\n"
            f"Discounts given (all sources, including manual): ${discounts:,.2f}.\n"
            f"Gift card redemptions: ${gc:,.2f}.\n"
            f"\n"
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
                + ", ".join(lines)
                + ". "
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
        """
        Step 7 change: now always emits a doc — with a 'NONE' chunk when
        the backend returns zero rows. The explicit disambiguation text
        prevents the AI from substituting 'general discounts' when asked
        about promo codes specifically.
        """
        if not rows:
            # Empty-state chunk — the one we were missing before
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

        # Populated case — same as before, just adds period_start metadata
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

    # ── Computed column helpers ──────────────────────────────────────────────

    @staticmethod
    def _compute_trend_slope(rows: list[dict]) -> float:
        """
        Linear regression slope over service_revenue values in time order.
        Positive = growing, negative = shrinking, 0 = flat.
        Returns 0.0 if fewer than 2 periods.
        """
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
        """Canonical label used consistently across all docs."""
        if slope > 0:
            return "up"
        if slope < 0:
            return "down"
        return "flat"

    @staticmethod
    def _monthly_text(row: dict, trend_slope: float) -> str:
        """Human-readable embedding text for a monthly summary row."""
        mom = row.get("mom_growth_pct")
        mom_str = f", {mom:+.1f}% vs previous month" if mom is not None else ""

        # Step 7: explicit trend verdict with backend's slope as the
        # authoritative signal. AI should quote this rather than
        # recomputing trend from MoM deltas.
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