# Appointments — Phase 0 contract check report

- **Run UTC:** 2026-04-28T15:35:00.008117+00:00
- **Base:** `https://uat-ext-api-a3bre0gyhzaxhbau.eastus2-01.azurewebsites.net`
- **Test biz:** `40` | **Foreign biz:** `128`
- **Window:** `2025-10-01` → `2026-03-31`

**Total: 8 checks** — 1 pass, 5 blocker, 2 workaround, 0 cosmetic

> 🚫 **5 blocker(s) — DO NOT proceed to Phase 1.**

## monthly-summary

| | Check | Severity | Detail |
|---|---|---|---|
| 🚫 | `http_status` | blocker | Expected 200, got 401. Body: '' |

## staff-breakdown

| | Check | Severity | Detail |
|---|---|---|---|
| 🚫 | `http_status` | blocker | Expected 200, got 401. Body: '' |

## service-breakdown

| | Check | Severity | Detail |
|---|---|---|---|
| 🚫 | `http_status` | blocker | Expected 200, got 401. Body: '' |

## staff-service-cross

| | Check | Severity | Detail |
|---|---|---|---|
| 🚫 | `http_status` | blocker | Expected 200, got 401. Body: '' |

## tenant_isolation

| | Check | Severity | Detail |
|---|---|---|---|
| ⚠️ | `tenant_isolation_403` | workaround | Foreign business_id returned 401 (expected 403). Acceptable if backend conflates 'no auth' with 'wrong tenant', but flag for backend review. |

## auth_failure

| | Check | Severity | Detail |
|---|---|---|---|
| ✅ | `auth_required` | pass | Missing API key correctly rejected with 401. |

## pagination

| | Check | Severity | Detail |
|---|---|---|---|
| 🚫 | `pagination_basic` | blocker | Pagination request failed: p1=401 p2=401. |

## empty_window

| | Check | Severity | Detail |
|---|---|---|---|
| ⚠️ | `empty_response_shape` | workaround | Cannot verify empty shape: status=401 on far-future request. Backend may reject 2099 dates. Verify manually with a known empty window. |

## Raw responses (source of truth)

Every request was saved verbatim under `raw/` for replay, diff against future runs,
and as a baseline for source-of-truth comparisons. Each file contains:
`request_url`, `request_method`, `request_params`, `status`, `headers`, `body`, `body_raw`, `elapsed_ms`.

- `raw/monthly_summary.json` — status `401`, 242ms, 0B
- `raw/staff_breakdown.json` — status `401`, 52ms, 0B
- `raw/service_breakdown.json` — status `401`, 53ms, 0B
- `raw/staff_service_cross.json` — status `401`, 39ms, 0B
- `raw/tenant_isolation.json` — status `401`, 46ms, 0B
- `raw/auth_failure.json` — status `401`, 47ms, 0B
- `raw/pagination_p1.json` — status `401`, 38ms, 0B
- `raw/pagination_p2.json` — status `401`, 43ms, 0B
- `raw/empty_window.json` — status `401`, 39ms, 0B

## How to use this run as a baseline

1. Commit `raw/` and `report.json` to a private location alongside the dated folder name.
2. On a future run, diff the new `raw/0*_*.json` against the baseline:
   `jq '.body' raw/01_monthly_summary.json > /tmp/new && jq '.body' BASELINE/raw/01_monthly_summary.json > /tmp/old && diff /tmp/old /tmp/new`
3. Or compare summaries: `jq '.summary' report.json` against the baseline's.
4. Any new blockers → stop; any new workarounds → log in KNOWN_ISSUES §2.