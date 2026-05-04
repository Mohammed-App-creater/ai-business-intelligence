# APPOINTMENTS — Known Issues & Drift Ledger

> Living document for the appointments domain integration sprint.
> Started: 2026-04-28. Spec version: v1.1 (post Step-7 refinement).
> Last verified: 2026-04-30.
>
> Conventions:
>   - Severity: blocker / workaround / cosmetic
>   - Status: open / queued / accepted / fixed
>   - Section numbers preserve their original meaning even after items are closed.

---

## 1. Open backend questions (carried from spec §7)

| # | Question | Severity | Status |
|---|----------|----------|--------|
| 1 | Should `IsGoogleEvent = 0` filter apply by default? Verified by Phase 0 query: API returns 311 vs source 119 IsGoogleEvent=0 rows — backend appears to *include* Google events. Spec needs to clarify intended behavior. | workaround | open |
| 2 | No-show derivation correctness — backend uses `Confirmed=1 AND Complete=0 AND StartDate<NOW()`. Verified that `Confirmed` is rarely populated for tenant 40 (zero rows in test window), so this metric returns 0 across all rows. | workaround | open — confirm with backend |
| 3 | `tbl_custsignin.AppType` reliability for walk-in vs app-booking split. Verified some tenants don't use kiosk sign-in. | workaround | open |
| 4 | Inactive locations (`tbl_organization_locations.IsActive=0`) treatment in monthly-summary. | workaround | open |

## 2. Bugs filed and their status

| Section | Bug | Status |
|---|---|---|
| §3.6 | `staff-breakdown` defaults `limit=10`; ETL passes 10000 | workaround in place; spec ask v1.2 |
| §3.7 | Empty `staff_name` / `service_name` across endpoints | ✅ FIXED 2026-04-30, verified 0/183 empty rows |
| §3.8 | Per-location `mom_growth_pct` returning rollup value | ✅ FIXED 2026-04-30, verified per-row reconciliation |
| §3.9 | `confirmed_count` always 0 for tenant 40 | not a bug — source data |
| §3.10 | 311 vs 126 row gap | not a bug — local DB was partial copy |
| §3.11 | Missing v1.1 spec fields | partial — see below |

## 3. Detailed entries

### §3.6 Inconsistent default `limit` across appointments endpoints (workaround)

Tested 2026-04-29 against tenant 40, window Oct 2025 → Mar 2026:

- monthly-summary: 32 rows w/o limit, 32 with limit=10000 (no default truncation)
- **staff-breakdown: 10 rows w/o limit, 61 with limit=10000 (default truncates at 10)**
- service-breakdown: 43 rows w/o limit, 43 with limit=10000 (no default truncation)
- staff-service-cross: 79 rows w/o limit, 79 with limit=10000 (no default truncation)

**Workaround:** ETL passes `limit=settings.APPOINTMENTS_PAGE_SIZE` (default 10000) on all four endpoints via `AnalyticsClient`.

**Spec ask v1.2:** document `limit` parameter; harmonize defaults or document each per endpoint.

**Status:** workaround active; spec doc update queued for backend.

### §3.7 ✅ FIXED — Empty `staff_name` / `service_name`

Found 2026-04-29: every row of staff-breakdown, service-breakdown, and staff-service-cross returned empty strings for the denormalized name fields. Source data confirmed populated for sampled IDs.

Fix deployed 2026-04-30. Verified PASS:
- staff-breakdown: 0/61 empty
- service-breakdown: 0/43 empty
- staff-service-cross: 0/79 both fields empty

No ETL workaround needed.

### §3.8 ✅ FIXED — Per-location `mom_growth_pct` returning rollup value

Found 2026-04-29: every per-location row's `mom_growth_pct` equaled the rollup `__ALL__` row's value for the same period, regardless of location's actual MoM. First-appearance rows incorrectly returned the rollup value instead of null. Likely cause: SQL window function not partitioned by `location_id`.

Fix deployed 2026-04-30. Verified PASS: per-row recompute matches API value for every location × period. First-appearance rows correctly return null.

`mom_growth_pct` also added to staff-breakdown.

No ETL workaround needed.

### §3.9 `confirmed_count` always 0 (not a bug)

Tenant 40 source data has zero rows with `tbl_calendarevent.Confirmed=1` in the test window. API correctly reports 0. Other tenants using the confirmation workflow should see non-zero values.

No action.

### §3.10 311 vs 126 row gap (not a bug)

Initial investigation suggested API was inflating numbers (analytics returned 311 appointments for tenant 40 vs 126 in source DB query). Root cause: developer's local DB was a partial copy. API was correct against the real production source DB.

No action. Lesson logged: never compare API totals against local DB without verifying the local DB is current.

### §3.11 Missing v1.1 spec fields (workarounds)

Four fields the v1.1 spec lists but the API doesn't return. As of 2026-04-30:

| Field | Endpoint(s) | Status | Workaround |
|---|---|---|---|
| `period_end` | monthly-summary | still missing | derived from `period` (date arithmetic) |
| `peak_slot` | monthly-summary, service-breakdown | still missing | derived from time-bucket counts |
| `completion_rate_pct` | staff-breakdown | still missing | derived: `completed_count / total_booked × 100` |
| `mom_growth_pct` | staff-breakdown | ✅ added by backend 2026-04-30 | — |

All workarounds in `etl/transforms/appointments_field_derivations.py`. All self-healing — once backend ships the missing fields, derived values match and helpers become no-ops.

**Spec ask v1.2:** clarify which of these three remaining fields backend will compute; remove from spec any backend doesn't intend to deliver.

## 4. Decisions log

| Date | Decision | Rationale |
|---|---|---|
| 2026-04-28 | Use `wh_appt_*` prefix for warehouse tables | Integration skill rule for new domains; avoids legacy unprefixed pattern |
| 2026-04-29 | Pass `limit=10000` from ETL on all four endpoints | Consumer should control; avoids dependence on undocumented backend defaults |
| 2026-04-29 | File one consolidated backend ticket for bugs #2 and #3 | Same domain, same backend dev's day, fix patterns rhyme |
| 2026-04-30 | Use 2-decimal precision for completion_rate_pct in chunk text | Matches API/warehouse/spec; avoids RAG vs dashboard mismatch |
| 2026-04-30 | Disambiguate AppointmentsExtractor → `LegacyMysqlAppointmentMetricsExtractor` for legacy SQL path | `from etl.extractors import AppointmentsExtractor` will now fail rather than silently return wrong class |

## 5. Sign-off checklist (Phase 6)

- [ ] All 29 spec questions pass against UAT data (target: 29/29, accept ≥27/29 with documented gaps)
- [ ] No hallucinations detected (manual review of 5 random answers per category)
- [ ] Tenant isolation: foreign `business_id` returns 403 from each endpoint
- [ ] Warehouse counts match expectation: `verify_wh_appt.py` passes for every `wh_appt_*` table
- [ ] Pgvector audit clean: chunks fall inside locked embed window, doc_type counts == expected
- [ ] Validator tripped once with a wrong answer, confirmed it failed
- [ ] All §1 backend questions answered or accepted
- [ ] Skill update proposed for Phase 6: replace "appointments" with "revenue" as canonical reference in `leo-domain-integration` Phase 0.1
