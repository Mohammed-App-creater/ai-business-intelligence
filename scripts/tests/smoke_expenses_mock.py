"""
scripts/tests/smoke_expenses_mock.py

Smoke test for the Expenses domain mock server wiring.
Run this right after dropping in expenses_fixtures.py + the updated
mock_analytics_server.py. Verifies:
  1. Mock server is up and advertises 6 expenses endpoints
  2. All 6 endpoints respond 200 with data
  3. Key story fields are present and correct
  4. Tenant isolation works (unknown biz → 403)

Usage:
    # Terminal 1
    uvicorn tests.mocks.mock_analytics_server:app --port 8001

    # Terminal 2
    python scripts/tests/smoke_expenses_mock.py
"""
import sys
import httpx

BASE_URL = "http://localhost:8001"
EXPECTED_ENDPOINTS = [
    "/api/v1/leo/expenses/monthly-summary",
    "/api/v1/leo/expenses/category-breakdown",
    "/api/v1/leo/expenses/location-breakdown",
    "/api/v1/leo/expenses/payment-type-breakdown",
    "/api/v1/leo/expenses/staff-attribution",
    "/api/v1/leo/expenses/category-location-cross",
]

def main() -> int:
    fail_count = 0

    def check(label, condition, details=""):
        nonlocal fail_count
        mark = "✅" if condition else "❌"
        print(f"  {mark} {label}" + (f"  — {details}" if details else ""))
        if not condition:
            fail_count += 1

    with httpx.Client(timeout=5.0) as client:
        print("\n── 1. Health check ──")
        r = client.get(f"{BASE_URL}/health")
        check("Server responding", r.status_code == 200, f"status={r.status_code}")
        if r.status_code == 200:
            h = r.json()
            check("Version is 1.7.0", h.get("version") == "1.7.0", f"got {h.get('version')}")
            check("Expenses count = 6", h.get("endpoints", {}).get("expenses") == 6,
                  f"got {h.get('endpoints', {}).get('expenses')}")
            # Total = len(ALL_PATHS) + 1 for the mode-switched /staff-performance route.
            # 6 revenue + 4 appointments + 1 staff-standard + 5 services + 3 clients
            # + 3 marketing + 6 expenses = 28, + 1 staff-performance = 29.
            check("Total endpoints = 29", h.get("endpoints", {}).get("total") == 29,
                  f"got {h.get('endpoints', {}).get('total')}")

        print("\n── 2. All 6 expenses endpoints respond 200 for biz 42 ──")
        responses = {}
        for path in EXPECTED_ENDPOINTS:
            r = client.post(f"{BASE_URL}{path}", json={"business_id": 42})
            ok = r.status_code == 200
            check(path, ok, f"status={r.status_code}")
            if ok:
                responses[path] = r.json()

        if len(responses) == 6:
            print("\n── 3. Monthly summary — Mar 2026 story checks ──")
            mar = next(
                (r for r in responses["/api/v1/leo/expenses/monthly-summary"]["data"]
                 if r["period"] == "2026-03-01"),
                None
            )
            check("Mar 2026 row exists", mar is not None)
            if mar:
                check("total_expenses == 4320", mar["total_expenses"] == 4320.0,
                      f"got {mar['total_expenses']}")
                check("ytd_total == 12810", mar["ytd_total"] == 12810.0,
                      f"got {mar['ytd_total']}")
                check("qoq_change_pct ≈ -7.17", abs(mar["qoq_change_pct"] - (-7.17)) < 0.1,
                      f"got {mar['qoq_change_pct']}")
                check("months_in_window == 6", mar["months_in_window"] == 6,
                      f"got {mar['months_in_window']}")

            print("\n── 4. Category breakdown — Feb 2026 Marketing spike ──")
            feb_mkt = next(
                (r for r in responses["/api/v1/leo/expenses/category-breakdown"]["data"]
                 if r["period"] == "2026-02-01" and r["category_id"] == 15),
                None
            )
            check("Feb Marketing row exists", feb_mkt is not None)
            if feb_mkt:
                check("anomaly_flag == 'spike'", feb_mkt["anomaly_flag"] == "spike",
                      f"got {feb_mkt['anomaly_flag']}")
                check("pct_vs_baseline ≈ 82.86", abs(feb_mkt["pct_vs_baseline"] - 82.86) < 0.5,
                      f"got {feb_mkt['pct_vs_baseline']}")

            print("\n── 5. Category breakdown — Office/Admin dormant ──")
            cat_rows = responses["/api/v1/leo/expenses/category-breakdown"]["data"]
            admin_2026 = [r for r in cat_rows
                          if r["category_id"] == 18 and r["period"] >= "2026-01-01"]
            check("Office/Admin has 0 rows in 2026 (dormant)", len(admin_2026) == 0,
                  f"got {len(admin_2026)} rows")

            print("\n── 6. Staff attribution — Feb has Maria + James ──")
            staff_rows = responses["/api/v1/leo/expenses/staff-attribution"]["data"]
            feb_staff = [r for r in staff_rows if r["period"] == "2026-02-01"]
            check("Feb has 2 staff rows", len(feb_staff) == 2, f"got {len(feb_staff)}")
            if len(feb_staff) == 2:
                names = sorted(r["employee_name"] for r in feb_staff)
                check("Maria + James present", names == ["James Carter", "Maria Lopez"],
                      f"got {names}")

            print("\n── 7. Payment type — Cash dominates ──")
            pay_rows = responses["/api/v1/leo/expenses/payment-type-breakdown"]["data"]
            mar_pay = [r for r in pay_rows if r["period"] == "2026-03-01"]
            check("Mar has 3 payment types", len(mar_pay) == 3, f"got {len(mar_pay)}")
            cash_row = next((r for r in mar_pay if r["payment_type_label"] == "Cash"), None)
            check("Cash pct ≈ 80%", cash_row and abs(cash_row["pct_of_month"] - 80.0) < 1.0,
                  f"got {cash_row['pct_of_month'] if cash_row else None}%")

            print("\n── 8. Subcategory drill-down — Rent subcats present ──")
            mar_rent = next(
                (r for r in responses["/api/v1/leo/expenses/category-breakdown"]["data"]
                 if r["period"] == "2026-03-01" and r["category_id"] == 14),
                None
            )
            check("Mar Rent has subcategory_breakdown", mar_rent and "subcategory_breakdown" in mar_rent)
            if mar_rent and "subcategory_breakdown" in mar_rent:
                check("3 subcategories returned", len(mar_rent["subcategory_breakdown"]) == 3,
                      f"got {len(mar_rent['subcategory_breakdown'])}")

        print("\n── 9. Tenant isolation ──")
        r = client.post(
            f"{BASE_URL}/api/v1/leo/expenses/monthly-summary",
            json={"business_id": 7}
        )
        check("biz_id=7 returns 403", r.status_code == 403, f"got {r.status_code}")

    print("\n" + "=" * 60)
    if fail_count == 0:
        print("  ✅ SMOKE TEST PASSED — expenses fixtures wired correctly")
        print("=" * 60)
        return 0
    else:
        print(f"  ❌ SMOKE TEST FAILED — {fail_count} check(s) failed")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())