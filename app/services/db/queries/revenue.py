from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from app.services.db.db_pool import DBPool, DBTarget


# ---------------------------------------------------------------------------
# Return types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RevenueRow:
    """One month of revenue data for a single business."""
    business_id: str
    month: date                   # first day of the month
    revenue: Decimal
    appointments: int
    cancellations: int
    cancellation_rate: float      # 0.0–1.0
    avg_ticket: Decimal           # revenue / completed appointments


@dataclass(frozen=True)
class RevenueResult:
    business_id: str
    rows: list[RevenueRow]        # ordered oldest → newest

    # Convenience helpers used by the prompt builder
    @property
    def latest(self) -> RevenueRow | None:
        return self.rows[-1] if self.rows else None

    @property
    def previous(self) -> RevenueRow | None:
        return self.rows[-2] if len(self.rows) >= 2 else None


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

_SQL = """
    SELECT
        business_id,
        DATE_FORMAT(appointment_date, '%%Y-%%m-01')          AS month,
        SUM(total_price)                                      AS revenue,
        COUNT(*)                                              AS appointments,
        SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancellations
    FROM appointments
    WHERE business_id = %s
      AND appointment_date >= DATE_SUB(CURDATE(), INTERVAL %s MONTH)
    GROUP BY business_id, month
    ORDER BY month ASC
"""


async def get_revenue(
    pool: DBPool,
    business_id: str,
    months: int = 6,
) -> RevenueResult:
    """
    Return monthly revenue aggregates for *business_id* over the last
    *months* calendar months (default 6).

    Reads from DBTarget.WAREHOUSE (pre-aggregated) if available; falls
    back to DBTarget.PRODUCTION for the raw appointments table shown in
    the architecture doc.
    """
    rows: list[RevenueRow] = []

    async with pool.acquire(DBTarget.PRODUCTION) as conn:
        async with conn.cursor() as cur:
            await cur.execute(_SQL, (business_id, months))
            raw = await cur.fetchall()  # list[dict] — DictCursor

    for r in raw:
        completed = r["appointments"] - r["cancellations"]
        avg_ticket = (
            Decimal(str(r["revenue"])) / completed
            if completed > 0
            else Decimal("0")
        )
        cancellation_rate = (
            r["cancellations"] / r["appointments"]
            if r["appointments"] > 0
            else 0.0
        )
        rows.append(
            RevenueRow(
                business_id=business_id,
                month=r["month"] if isinstance(r["month"], date) else date.fromisoformat(str(r["month"])),
                revenue=Decimal(str(r["revenue"])),
                appointments=r["appointments"],
                cancellations=r["cancellations"],
                cancellation_rate=round(cancellation_rate, 4),
                avg_ticket=avg_ticket.quantize(Decimal("0.01")),
            )
        )

    return RevenueResult(business_id=business_id, rows=rows)
