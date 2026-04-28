"""
tests/mocks/memberships_fixtures.py
====================================
Mock fixtures for the Memberships analytics endpoints.

Two payloads:
  • get_memberships_fixture()         — Set A (unit grain, 32 rows)
  • get_memberships_monthly_fixture() — Set B (location-month grain, derived)

Set B is COMPUTED from Set A's seed data, not hand-typed.  This guarantees
the two payloads stay internally consistent — if you tweak a seed in
MEMBERSHIP_SEEDS, the monthly summary updates automatically on next call.

Test tenant:    business_id = 99
As-of default:  2026-04-27  (matches the system clock)

Coverage map: see the docstring at the bottom of MEMBERSHIP_SEEDS for which
seed rows exercise which Step 1 test question.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Optional


# =============================================================================
#  Configuration
# =============================================================================

TEST_BUSINESS_ID = 99
DEFAULT_AS_OF = date(2026, 4, 27)
REACTIVATION_WINDOW_DAYS = 90  # G5 heuristic from Step 2

LOCATIONS: dict[int, str] = {
    101: "Downtown",
    102: "Westside",
    103: "Northpark",
}

# (name, price, interval_days, interval_bucket)
SERVICES: dict[int, tuple[str, float, int, str]] = {
    501: ("Monthly Massage",       100.00, 30, "monthly"),
    502: ("Weekly Facial Pass",     50.00,  7, "weekly"),
    503: ("Quarterly Detox",       300.00, 90, "quarterly"),
    504: ("Bi-weekly Manicure",     40.00, 14, "bi-weekly"),
    505: ("Custom Wellness Plan",   75.00, 45, "other"),
}

CUSTOMER_NAMES: dict[int, str] = {
    70001: "Maria Hernandez",   70002: "James Park",
    70003: "Sarah Chen",        70004: "Robert Johnson",
    70005: "Emily Davis",       70006: "Michael Brown",
    70007: "Lisa Anderson",     70008: "David Wilson",
    70009: "Jennifer Lee",      70010: "Thomas Garcia",
    70011: "Patricia Martinez", 70012: "William Thomas",
    70013: "Linda Anderson",    70014: "Charles Wright",
    70015: "Barbara Lopez",     70016: "Joseph Clark",
    70017: "Susan Lewis",       70018: "Richard Walker",
    70019: "Karen Hall",        70020: "Daniel Allen",
    70021: "Nancy Young",       70022: "Mark King",
    70023: "Jessica Wright",    70024: "Steven Scott",
    70025: "Helen Green",       70026: "Brian Adams",
    70027: "Christine Baker",   70028: "Edward Nelson",
    70029: "Sandra Carter",     70030: "Frank Mitchell",
}


# =============================================================================
#  Seed data — every membership in plain form
# =============================================================================

@dataclass(frozen=True)
class MembershipSeed:
    sub_id: int
    location_id: int
    customer_id: int
    service_id: int
    discount: float
    signup_date: date
    cancel_date: Optional[date]      # None = still active
    is_reactivation: bool = False
    failed_charge_dates: tuple[date, ...] = field(default_factory=tuple)
    visit_count: int = 0
    last_visit_date: Optional[date] = None


MEMBERSHIP_SEEDS: list[MembershipSeed] = [
    # ─────────────────────────────────────────────────────────────────────────
    # Location 101 — Downtown (12 subs: 11 active, 1 canceled+reactivated)
    # Profile: established, growing, healthy retention
    # ─────────────────────────────────────────────────────────────────────────
    MembershipSeed(  1, 101, 70001, 501,  0.0,  date(2024,10,15), None,
                    visit_count=18, last_visit_date=date(2026,4,18)),  # longest tenure ✓ Q10
    MembershipSeed(  2, 101, 70002, 501, 10.0, date(2024,12, 5), None,
                    visit_count=14, last_visit_date=date(2026,4,12)),  # discount ✓ Q17
    MembershipSeed(  3, 101, 70003, 502,  0.0,  date(2025, 1,10), None,
                    visit_count=42, last_visit_date=date(2026,4,20)),  # weekly bucket ✓ Q9
    MembershipSeed(  4, 101, 70004, 501,  0.0,  date(2025, 2,14), None,
                    visit_count=11, last_visit_date=date(2026,4,10)),
    MembershipSeed(  5, 101, 70005, 503, 25.0, date(2024,12,20), None,
                    visit_count= 5, last_visit_date=date(2026,3,15)),  # quarterly + discount ✓ Q9 ✓ Q17
    MembershipSeed(  6, 101, 70006, 504,  0.0,  date(2025, 3,15), None,
                    visit_count=22, last_visit_date=date(2026,4,21)),  # bi-weekly bucket ✓ Q9
    MembershipSeed(  7, 101, 70007, 505,  0.0,  date(2025, 5,10), None,
                    visit_count= 7, last_visit_date=date(2026,4, 5)),  # "other" bucket ✓ Q9
    MembershipSeed(  8, 101, 70008, 501,  0.0,  date(2025, 7,20), None,
                    visit_count= 0, last_visit_date=None),             # GHOST member ✓ Q19
    MembershipSeed(  9, 101, 70009, 502,  5.0,  date(2025, 9, 1), None,
                    visit_count=18, last_visit_date=date(2026,4,22)),
    MembershipSeed( 10, 101, 70010, 501,  0.0,  date(2025,11, 5), None,
                    failed_charge_dates=(date(2026,3, 5),),            # Q21 ✓
                    visit_count= 4, last_visit_date=date(2026,4, 8)),
    MembershipSeed( 11, 101, 70011, 501,  0.0,  date(2025, 6,15), date(2025, 9,15),
                    visit_count= 3, last_visit_date=date(2025,9, 1)),   # cancel — followed by sub 12
    MembershipSeed( 12, 101, 70011, 501,  0.0,  date(2025,11, 1), None,
                    is_reactivation=True,                               # ✓ reactivation (47d after sub 11)
                    visit_count= 5, last_visit_date=date(2026,4,18)),

    # ─────────────────────────────────────────────────────────────────────────
    # Location 102 — Westside (10 subs: 4 active, 6 canceled — DECLINING)
    # Profile: Feb 2026 churn cluster, March 2026 payment failures
    # Drives ✓ Q12, Q13, M-LQ5, M-LQ8
    # ─────────────────────────────────────────────────────────────────────────
    MembershipSeed( 13, 102, 70012, 501,  0.0,  date(2025, 1,10), None,
                    visit_count=12, last_visit_date=date(2026,4,15)),
    MembershipSeed( 14, 102, 70013, 501,  0.0,  date(2025, 2, 5), date(2026, 2,15),
                    visit_count= 8, last_visit_date=date(2026,1,28)),  # long-tenure cancel ✓ Q13
    MembershipSeed( 15, 102, 70014, 502,  0.0,  date(2025, 3,20), date(2026, 2,20),
                    visit_count=24, last_visit_date=date(2026,2,15)),  # ✓ Feb cluster
    MembershipSeed( 16, 102, 70015, 501,  0.0,  date(2025, 4,10), date(2026, 2,10),
                    visit_count= 4, last_visit_date=date(2026,1,30)),  # ✓ Feb cluster
    MembershipSeed( 17, 102, 70016, 504,  0.0,  date(2025, 5,15), date(2026, 2, 8),
                    visit_count= 9, last_visit_date=date(2026,2, 1)),  # ✓ Feb cluster
    MembershipSeed( 18, 102, 70017, 501,  0.0,  date(2025, 6,10), date(2026, 2,25),
                    visit_count= 5, last_visit_date=date(2026,2,18)),  # ✓ Feb cluster
    MembershipSeed( 19, 102, 70018, 501,  0.0,  date(2025, 8, 1), None,
                    failed_charge_dates=(date(2026,3, 1), date(2026,3, 3)),  # ✓ Q21 (2 failures)
                    visit_count= 3, last_visit_date=date(2026,3,20)),
    MembershipSeed( 20, 102, 70019, 503,  0.0,  date(2025, 9,10), None,
                    failed_charge_dates=(date(2026,3, 9),),            # ✓ Q21 (quarterly failure)
                    visit_count= 2, last_visit_date=date(2026,2,28)),
    MembershipSeed( 21, 102, 70020, 505, 10.0, date(2024,11,10), date(2026, 1, 5),
                    visit_count=11, last_visit_date=date(2025,12,15)),  # canceled — followed by sub 22
    MembershipSeed( 22, 102, 70020, 505, 10.0, date(2026, 3,10), None,
                    is_reactivation=True,                               # ✓ reactivation (64d after sub 21)
                    visit_count= 0, last_visit_date=None),

    # ─────────────────────────────────────────────────────────────────────────
    # Location 103 — Northpark (10 subs: 9 active, 1 canceled — NEWEST/GROWING)
    # Drives ✓ Q4, Q7, M-LQ4 (signup velocity)
    # ─────────────────────────────────────────────────────────────────────────
    MembershipSeed( 23, 103, 70021, 501,  0.0,  date(2025, 9,15), None,
                    visit_count= 7, last_visit_date=date(2026,4,15)),
    MembershipSeed( 24, 103, 70022, 502,  0.0,  date(2025,10, 1), None,
                    visit_count=30, last_visit_date=date(2026,4,23)),  # weekly — drives Q18 (next due May 1)
    MembershipSeed( 25, 103, 70023, 504,  5.0, date(2025,10,20), None,
                    visit_count=13, last_visit_date=date(2026,4,18)),  # bi-weekly — drives Q18 ✓
    MembershipSeed( 26, 103, 70024, 501,  0.0,  date(2025,11,15), None,
                    failed_charge_dates=(date(2026,3,15),),            # ✓ Q21
                    visit_count= 5, last_visit_date=date(2026,4, 5)),
    MembershipSeed( 27, 103, 70025, 503,  0.0,  date(2025,12, 5), None,
                    visit_count= 1, last_visit_date=date(2026,1,10)),  # quarterly — almost no visits
    MembershipSeed( 28, 103, 70026, 501,  0.0,  date(2026, 1,10), None,
                    visit_count= 4, last_visit_date=date(2026,4, 8)),
    MembershipSeed( 29, 103, 70027, 502,  0.0,  date(2026, 2,15), None,
                    visit_count=11, last_visit_date=date(2026,4,22)),  # weekly — drives Q18 ✓
    MembershipSeed( 30, 103, 70028, 501,  0.0,  date(2026, 3, 1), None,
                    visit_count= 0, last_visit_date=None),             # ghost ✓ Q19  + due Apr 30 ✓ Q18
    MembershipSeed( 31, 103, 70029, 505, 15.0, date(2026, 3,20), None,
                    visit_count= 1, last_visit_date=date(2026,4, 1)),  # discount ✓ Q17
    MembershipSeed( 32, 103, 70030, 501,  0.0,  date(2026, 4, 5), None,
                    visit_count= 0, last_visit_date=None),             # newest signup
]


# =============================================================================
#  Helpers — derive Set A from seeds
# =============================================================================

def _iso_z(dt: datetime | date) -> str:
    """Return ISO 8601 Zulu string."""
    if isinstance(dt, date) and not isinstance(dt, datetime):
        dt = datetime.combine(dt, datetime.min.time())
    return dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _scheduled_charge_dates(seed: MembershipSeed, as_of: date) -> list[date]:
    """All approved charge dates: signup + every interval thereafter,
    bounded by cancel_date or as_of_date."""
    _, _, interval, _ = SERVICES[seed.service_id]
    end = seed.cancel_date or as_of
    dates: list[date] = []
    next_d = seed.signup_date
    while next_d <= end:
        dates.append(next_d)
        next_d += timedelta(days=interval)
    return dates


def _next_execution_date(seed: MembershipSeed, as_of: date) -> Optional[date]:
    """Next scheduled charge after as_of, or None if canceled."""
    if seed.cancel_date is not None:
        return None
    _, _, interval, _ = SERVICES[seed.service_id]
    next_d = seed.signup_date
    while next_d <= as_of:
        next_d += timedelta(days=interval)
    return next_d


def derive_membership_row(seed: MembershipSeed, as_of: date) -> dict:
    """Compute one Set A row from a seed."""
    name, amount, interval, bucket = SERVICES[seed.service_id]
    net = round(amount - seed.discount, 2)
    end = seed.cancel_date or as_of
    tenure_days = (end - seed.signup_date).days

    approved_dates = _scheduled_charge_dates(seed, as_of)
    approved_count = len(approved_dates)
    failed_count = len(seed.failed_charge_dates)
    total_charge_count = approved_count + failed_count
    total_billed = round(approved_count * net, 2)

    last_charge = approved_dates[-1] if approved_dates else None
    days_since_last = (as_of - last_charge).days if last_charge else None

    next_exec = _next_execution_date(seed, as_of)
    days_until = (next_exec - as_of).days if next_exec else None
    is_due = 1 if (next_exec and 0 <= days_until <= 7) else 0

    is_active = 0 if seed.cancel_date else 1
    monthly_eq = round(net * 30.0 / interval, 2)
    estimated_ltv = round(tenure_days / interval * net, 2)

    return {
        "subscription_id":            seed.sub_id,
        "business_id":                TEST_BUSINESS_ID,
        "location_id":                seed.location_id,
        "customer_id":                seed.customer_id,
        "customer_name":              CUSTOMER_NAMES.get(seed.customer_id, f"Customer {seed.customer_id}"),
        "service_id":                 seed.service_id,
        "service_name":               name,
        "amount":                     amount,
        "discount":                   seed.discount,
        "net_amount":                 net,
        "interval_days":              interval,
        "interval_bucket":            bucket,
        "created_at":                 _iso_z(datetime.combine(seed.signup_date, datetime.min.time().replace(hour=10))),
        "next_execution_date":        _iso_z(next_exec) if next_exec else None,
        "is_active":                  is_active,
        "canceled_at":                _iso_z(datetime.combine(seed.cancel_date, datetime.min.time().replace(hour=14))) if seed.cancel_date else None,
        "tenure_days":                tenure_days,
        "monthly_equivalent_revenue": monthly_eq,
        "estimated_ltv":              estimated_ltv,
        "total_charge_count":         total_charge_count,
        "approved_charge_count":      approved_count,
        "failed_charge_count":        failed_count,
        "total_billed":               total_billed,
        "last_successful_charge_at":  _iso_z(last_charge) if last_charge else None,
        "days_since_last_charge":     days_since_last,
        "visit_count_in_window":      seed.visit_count,
        "last_visit_at":              _iso_z(seed.last_visit_date) if seed.last_visit_date else None,
        "is_used":                    1 if seed.visit_count > 0 else 0,
        "is_reactivation":            1 if seed.is_reactivation else 0,
        "days_until_next_charge":     days_until,
        "is_due_in_7_days":           is_due,
    }


# =============================================================================
#  Set A — public fixture
# =============================================================================

def get_memberships_fixture(business_id: int, as_of_date: Optional[date] = None) -> dict:
    """Mock GET /api/v1/analytics/memberships response."""
    as_of = as_of_date or DEFAULT_AS_OF

    if business_id != TEST_BUSINESS_ID:
        # Tenant isolation — unknown tenant returns empty
        return {
            "business_id":  business_id,
            "as_of_date":   as_of.isoformat(),
            "generated_at": _iso_z(_now_utc()),
            "row_count":    0,
            "data":         [],
        }

    rows = [derive_membership_row(s, as_of) for s in MEMBERSHIP_SEEDS]
    # Sort spec: is_active DESC, created_at DESC
    rows.sort(key=lambda r: r["created_at"], reverse=True)
    rows.sort(key=lambda r: r["is_active"],  reverse=True)

    return {
        "business_id":  business_id,
        "as_of_date":   as_of.isoformat(),
        "generated_at": _iso_z(_now_utc()),
        "row_count":    len(rows),
        "data":         rows,
    }


# =============================================================================
#  Set B — derived monthly summary
# =============================================================================

def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def _add_month(d: date) -> date:
    return _month_start(d + timedelta(days=32))


def _last_day_of_month(d: date) -> date:
    return _add_month(d) - timedelta(days=1)


def _is_followed_by_reactivation(seed: MembershipSeed) -> bool:
    """Per G5 heuristic: a cancellation is excluded from the cancel count if
    it's followed within 90d by a same-customer + same-service reactivation."""
    if seed.cancel_date is None:
        return False
    for other in MEMBERSHIP_SEEDS:
        if (other.is_reactivation
            and other.customer_id == seed.customer_id
            and other.service_id == seed.service_id
            and other.signup_date > seed.cancel_date
            and (other.signup_date - seed.cancel_date).days <= REACTIVATION_WINDOW_DAYS):
            return True
    return False


def _seed_active_at(seed: MembershipSeed, as_of_eom: date) -> bool:
    if seed.signup_date > as_of_eom:
        return False
    return seed.cancel_date is None or seed.cancel_date > as_of_eom


def get_memberships_monthly_fixture(business_id: int,
                                    start_date: date,
                                    end_date: date) -> dict:
    """Mock GET /api/v1/analytics/memberships/monthly response.
    Derived from the same seeds as Set A — guaranteed consistent."""

    if business_id != TEST_BUSINESS_ID:
        return {
            "business_id":  business_id,
            "period_start": start_date.isoformat(),
            "period_end":   end_date.isoformat(),
            "generated_at": _iso_z(_now_utc()),
            "row_count":    0,
            "data":         [],
        }

    # Build month grid for every (location, month_start)
    months: list[date] = []
    cursor = _month_start(start_date)
    while cursor <= end_date:
        months.append(cursor)
        cursor = _add_month(cursor)

    # Pre-aggregate from seeds
    bucket: dict[tuple[int, date], dict] = {}
    for loc_id in LOCATIONS:
        for m in months:
            bucket[(loc_id, m)] = {
                "new_signups":         0,
                "reactivations":       0,
                "cancellations":       0,
                "active_at_month_end": 0,
                "mrr":                 0.0,
                "gross_billed":        0.0,
                "approved_charges":    0,
                "failed_charges":      0,
                "discount_sum":        0.0,
                "discount_n":          0,
            }

    for seed in MEMBERSHIP_SEEDS:
        _, amount, interval, _ = SERVICES[seed.service_id]
        net = amount - seed.discount
        monthly_eq = net * 30.0 / interval

        # Signups & reactivations
        signup_m = _month_start(seed.signup_date)
        if (seed.location_id, signup_m) in bucket:
            if seed.is_reactivation:
                bucket[(seed.location_id, signup_m)]["reactivations"] += 1
            else:
                bucket[(seed.location_id, signup_m)]["new_signups"] += 1

        # Cancellations (excluding those followed by reactivation within 90d)
        if seed.cancel_date and not _is_followed_by_reactivation(seed):
            cancel_m = _month_start(seed.cancel_date)
            if (seed.location_id, cancel_m) in bucket:
                bucket[(seed.location_id, cancel_m)]["cancellations"] += 1

        # Approved charges per month
        for cd in _scheduled_charge_dates(seed, end_date):
            if cd > end_date:
                break
            cm = _month_start(cd)
            key = (seed.location_id, cm)
            if key in bucket:
                bucket[key]["approved_charges"] += 1
                bucket[key]["gross_billed"]    += net

        # Failed charges per month
        for fd in seed.failed_charge_dates:
            fm = _month_start(fd)
            key = (seed.location_id, fm)
            if key in bucket:
                bucket[key]["failed_charges"] += 1

        # Active-at-month-end + MRR + discount snapshot
        for m in months:
            eom = _last_day_of_month(m)
            if _seed_active_at(seed, eom):
                key = (seed.location_id, m)
                bucket[key]["active_at_month_end"] += 1
                bucket[key]["mrr"]                 += monthly_eq
                bucket[key]["discount_sum"]        += seed.discount
                bucket[key]["discount_n"]          += 1

    # Materialize, sort, add LAG-derived fields
    rows: list[dict] = []
    for loc_id in sorted(LOCATIONS):
        prev_mrr: Optional[float] = None
        prev_active: Optional[int] = None
        for m in months:
            agg = bucket[(loc_id, m)]
            mrr = round(agg["mrr"], 2)
            active = agg["active_at_month_end"]
            cancellations = agg["cancellations"]

            mrr_mom_pct: Optional[float] = None
            if prev_mrr not in (None, 0):
                mrr_mom_pct = round((mrr - prev_mrr) / prev_mrr * 100, 2)

            churn_rate: Optional[float] = None
            if prev_active not in (None, 0):
                churn_rate = round(cancellations / prev_active * 100, 2)

            avg_discount: Optional[float] = None
            if agg["discount_n"] > 0:
                avg_discount = round(agg["discount_sum"] / agg["discount_n"], 2)

            rows.append({
                "business_id":         TEST_BUSINESS_ID,
                "location_id":         loc_id,
                "month_start":         m.isoformat(),
                "new_signups":         agg["new_signups"],
                "reactivations":       agg["reactivations"],
                "cancellations":       cancellations,
                "active_at_month_end": active,
                "mrr":                 mrr,
                "gross_billed":        round(agg["gross_billed"], 2),
                "approved_charges":    agg["approved_charges"],
                "failed_charges":      agg["failed_charges"],
                "avg_discount":        avg_discount,
                "prev_mrr":            prev_mrr,
                "mrr_mom_pct":         mrr_mom_pct,
                "prev_active":         prev_active,
                "churn_rate_pct":      churn_rate,
            })
            prev_mrr = mrr
            prev_active = active

    return {
        "business_id":  business_id,
        "period_start": start_date.isoformat(),
        "period_end":   end_date.isoformat(),
        "generated_at": _iso_z(_now_utc()),
        "row_count":    len(rows),
        "data":         rows,
    }


# =============================================================================
#  Sanity check — guarantees Set A and Set B agree
# =============================================================================

def verify_consistency(verbose: bool = True) -> None:
    """Run cross-check assertions. Used by CI and on import in dev."""
    as_of = DEFAULT_AS_OF

    set_a = get_memberships_fixture(TEST_BUSINESS_ID, as_of)
    set_b = get_memberships_monthly_fixture(
        TEST_BUSINESS_ID, date(2024, 10, 1), as_of
    )

    rows_a = set_a["data"]
    rows_b = set_b["data"]

    # 1. Totals
    active_count = sum(1 for r in rows_a if r["is_active"] == 1)
    canceled_count = sum(1 for r in rows_a if r["is_active"] == 0)
    react_count = sum(1 for r in rows_a if r["is_reactivation"] == 1)
    failed_charge_total = sum(r["failed_charge_count"] for r in rows_a)
    due_in_7_count = sum(1 for r in rows_a if r["is_due_in_7_days"] == 1)
    ghost_count = sum(1 for r in rows_a if r["is_active"] == 1 and r["is_used"] == 0)

    # 2. Active-at-month-end of latest month should match Set A active count
    latest_eom = rows_b[-1]["month_start"]
    active_in_b_latest = sum(
        r["active_at_month_end"] for r in rows_b if r["month_start"] == latest_eom
    )

    # 3. Reactivation count matches across views
    react_in_b = sum(r["reactivations"] for r in rows_b)

    # 4. Failed charges in March 2026 (where we placed all of them)
    march_failed = sum(
        r["failed_charges"] for r in rows_b if r["month_start"] == "2026-03-01"
    )

    if verbose:
        print(f"✓ rows in Set A: {len(rows_a)}")
        print(f"✓ active members: {active_count}, canceled: {canceled_count}")
        print(f"✓ reactivations: A={react_count}  B(sum)={react_in_b}  match={react_count == react_in_b}")
        print(f"✓ failed charges: A(sum)={failed_charge_total}  B(March)={march_failed}")
        print(f"✓ active at latest month-end: B={active_in_b_latest}  vs A={active_count}")
        print(f"✓ ghost members (active but never visited): {ghost_count}")
        print(f"✓ due in next 7 days: {due_in_7_count}")

    assert react_count == react_in_b, "Reactivation count mismatch"
    assert active_in_b_latest == active_count, (
        f"Active count mismatch: B latest month = {active_in_b_latest}, "
        f"A current = {active_count}"
    )
    assert failed_charge_total == march_failed, (
        "All failed charges were seeded in March 2026 — totals must match"
    )
    assert due_in_7_count >= 3, "Need at least 3 'due in 7 days' rows for Q18 testing"
    assert ghost_count >= 2, "Need at least 2 ghost members for Q19 testing"
    assert react_count >= 2, "Need at least 2 reactivations"


if __name__ == "__main__":
    verify_consistency()
    print("\nAll consistency checks passed ✅")