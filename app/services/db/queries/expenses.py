from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from app.services.db.db_pool import DBPool, DBTarget


# ---------------------------------------------------------------------------
# Return types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExpenseRow:
    """One month of expense data for a single business."""
    business_id: str
    month: date
    category: str          # e.g. "rent", "supplies", "payroll", "marketing"
    total: Decimal


@dataclass(frozen=True)
class ExpenseSummaryRow:
    """Cross-category monthly roll-up — used by the prompt builder."""
    business_id: str
    month: date
    total_expenses: Decimal
    breakdown: dict[str, Decimal]   # category → amount


@dataclass(frozen=True)
class ExpenseResult:
    business_id: str
    rows: list[ExpenseRow]               # one row per (month, category)
    summaries: list[ExpenseSummaryRow]   # one row per month

    @property
    def latest_summary(self) -> ExpenseSummaryRow | None:
        return self.summaries[-1] if self.summaries else None

    @property
    def previous_summary(self) -> ExpenseSummaryRow | None:
        return self.summaries[-2] if len(self.summaries) >= 2 else None


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

_DETAIL_SQL = """
    SELECT
        business_id,
        DATE_FORMAT(expense_date, '%%Y-%%m-01') AS month,
        category,
        SUM(amount)                              AS total
    FROM expenses
    WHERE business_id = %s
      AND expense_date >= DATE_SUB(CURDATE(), INTERVAL %s MONTH)
    GROUP BY business_id, month, category
    ORDER BY month ASC, category ASC
"""


def _build_summaries(
    business_id: str,
    rows: list[ExpenseRow],
) -> list[ExpenseSummaryRow]:
    """Aggregate detail rows into per-month summaries in Python —
    avoids a second round-trip to the DB."""
    month_map: dict[date, dict[str, Decimal]] = {}
    for r in rows:
        month_map.setdefault(r.month, {})
        month_map[r.month][r.category] = (
            month_map[r.month].get(r.category, Decimal("0")) + r.total
        )

    summaries = []
    for month in sorted(month_map):
        breakdown = month_map[month]
        summaries.append(
            ExpenseSummaryRow(
                business_id=business_id,
                month=month,
                total_expenses=sum(breakdown.values(), Decimal("0")),
                breakdown=breakdown,
            )
        )
    return summaries


async def get_expenses(
    pool: DBPool,
    business_id: str,
    months: int = 6,
) -> ExpenseResult:
    """
    Return monthly expense detail and roll-up summaries for *business_id*
    over the last *months* calendar months (default 6).
    """
    async with pool.acquire(DBTarget.PRODUCTION) as conn:
        async with conn.cursor() as cur:
            await cur.execute(_DETAIL_SQL, (business_id, months))
            raw = await cur.fetchall()

    rows: list[ExpenseRow] = [
        ExpenseRow(
            business_id=business_id,
            month=r["month"] if isinstance(r["month"], date) else date.fromisoformat(str(r["month"])),
            category=r["category"],
            total=Decimal(str(r["total"])),
        )
        for r in raw
    ]

    summaries = _build_summaries(business_id, rows)
    return ExpenseResult(business_id=business_id, rows=rows, summaries=summaries)
