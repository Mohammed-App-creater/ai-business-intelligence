"""Client lifetime metrics extractor."""
from __future__ import annotations

from etl.base import BaseExtractor

_SQL_VISITS = """
SELECT
    v.OrganizationId            AS business_id,
    v.CustID                    AS customer_id,
    MIN(DATE(v.RecDateTime))    AS first_visit_date,
    MAX(DATE(v.RecDateTime))    AS last_visit_date,
    COUNT(*)                    AS total_visits,
    SUM(v.TotalPay)             AS total_spend
FROM tbl_visit v
WHERE v.OrganizationId = %s
  AND v.PaymentStatus = 1
GROUP BY v.OrganizationId, v.CustID
""".strip()

_SQL_LOYALTY = """
SELECT
    CustID      AS customer_id,
    Points      AS loyalty_points
FROM tbl_custorg
WHERE OrgID = %s
  AND Active = 1
""".strip()


class ClientsExtractor(BaseExtractor):
    async def extract(self, org_id: int) -> list[dict]:
        visits = await self.fetch_all(_SQL_VISITS, (org_id,))
        loyalty = await self.fetch_all(_SQL_LOYALTY, (org_id,))
        by_cust = {r["customer_id"]: r for r in loyalty}
        out: list[dict] = []
        for row in visits:
            cid = row["customer_id"]
            lp = by_cust.get(cid)
            points = lp["loyalty_points"] if lp else 0
            out.append({**row, "loyalty_points": points})
        return out
