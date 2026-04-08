"""Campaign execution extractor."""
from __future__ import annotations

from datetime import date

from etl.base import BaseExtractor
from etl.extractors._util import period_end_exclusive

_SQL = """
SELECT
    c.TenantID                                  AS business_id,
    ec.CampaignId                               AS campaign_id,
    c.Name                                      AS campaign_name,
    ec.ExecutionDate                            AS execution_date,
    c.Recurring                                 AS is_recurring,
    COALESCE(ec.Total, 0)                       AS total_sent,
    COALESCE(ec.Successed, 0)                   AS successful_sent,
    COALESCE(ec.Failed, 0)                      AS failed_count,
    COALESCE(ec.Opened, 0)                      AS opened_count,
    COALESCE(ec.Clicked, 0)                     AS clicked_count
FROM tbl_executecampaign ec
JOIN tbl_mrkcampaign c ON ec.CampaignId = c.id
WHERE c.TenantID = %s
  AND ec.ExecutionDate >= %s
  AND ec.ExecutionDate <  %s
  AND c.Status != 'Delete'
ORDER BY ec.ExecutionDate DESC
""".strip()


class CampaignsExtractor(BaseExtractor):
    async def extract(self, org_id: int, period_start: date, period_end: date) -> list[dict]:
        end_excl = period_end_exclusive(period_end)
        return await self.fetch_all(_SQL, (org_id, period_start, end_excl))
