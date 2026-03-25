from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from app.services.db.db_pool import DBPool, DBTarget


# ---------------------------------------------------------------------------
# Return types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ServiceRow:
    """Popularity + revenue stats for one service in one month."""
    business_id: str
    month: date
    service_name: str
    bookings: int
    revenue: Decimal
    avg_price: Decimal
    cancellations: int
    cancellation_rate: float   # 0.0–1.0


@dataclass(frozen=True)
class ServiceResult:
    business_id: str
    rows: list[ServiceRow]     # ordered by month ASC, revenue DESC

    def top_by_revenue(self, month: date, n: int = 5) -> list[ServiceRow]:
        """Return the top-n services for a given month, ranked by revenue."""
        return sorted(
            [r for r in self.rows if r.month == month],
            key=lambda r: r.revenue,
            reverse=True,
        )[:n]

    def top_by_bookings(self, month: date, n: int = 5) -> list[ServiceRow]:
        """Return the top-n services for a given month, ranked by bookings."""
        return sorted(
            [r for r in self.rows if r.month == month],
            key=lambda r: r.bookings,
            reverse=True,
        )[:n]

    @property
    def months(self) -> list[date]:
        """Unique months present in the result set, oldest first."""
        return sorted({r.month for r in self.rows})


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

_SQL = """
    SELECT
        a.business_id,
        DATE_FORMAT(a.appointment_date, '%%Y-%%m-01')            AS month,
        s.name                                                    AS service_name,
        COUNT(*)                                                  AS bookings,
        SUM(a.total_price)                                        AS revenue,
        SUM(CASE WHEN a.status = 'cancelled' THEN 1 ELSE 0 END)  AS cancellations
    FROM appointments a
    JOIN services s ON s.id = a.service_id
                    AND s.business_id = a.business_id
    WHERE a.business_id = %s
      AND a.appointment_date >= DATE_SUB(CURDATE(), INTERVAL %s MONTH)
    GROUP BY a.business_id, month, s.name
    ORDER BY month ASC, revenue DESC
"""


async def get_services(
    pool: DBPool,
    business_id: str,
    months: int = 6,
) -> ServiceResult:
    """
    Return per-service booking and revenue stats for *business_id* over
    the last *months* calendar months (default 6).

    Joins the appointments table with the services lookup table so the
    prompt builder gets human-readable service names rather than IDs.
    """
    async with pool.acquire(DBTarget.PRODUCTION) as conn:
        async with conn.cursor() as cur:
            await cur.execute(_SQL, (business_id, months))
            raw = await cur.fetchall()

    rows: list[ServiceRow] = []
    for r in raw:
        bookings = r["bookings"]
        cancellations = r["cancellations"]
        completed = bookings - cancellations
        avg_price = (
            Decimal(str(r["revenue"])) / completed
            if completed > 0
            else Decimal("0")
        )
        rows.append(
            ServiceRow(
                business_id=business_id,
                month=r["month"] if isinstance(r["month"], date) else date.fromisoformat(str(r["month"])),
                service_name=r["service_name"],
                bookings=bookings,
                revenue=Decimal(str(r["revenue"])),
                avg_price=avg_price.quantize(Decimal("0.01")),
                cancellations=cancellations,
                cancellation_rate=round(cancellations / bookings, 4) if bookings > 0 else 0.0,
            )
        )

    return ServiceResult(business_id=business_id, rows=rows)
