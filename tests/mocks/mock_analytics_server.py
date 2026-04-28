"""
tests/mocks/mock_analytics_server.py

A lightweight FastAPI server that returns fixture data for all endpoints:
  - 6 revenue endpoints
  - 4 appointments endpoints
  - 3 staff performance endpoints (2 modes + attendance)
  - 5 services endpoints
  - 3 clients endpoints
  - 3 marketing endpoints
  - 6 expenses endpoints
  - 4 promos endpoints (+ 2 shape-switched aliases)
  - 8 giftcards endpoints      (Domain 9)
  - 4 forms endpoints          (Domain 10)
  - 2 memberships endpoints    ← NEW (Domain 11)

Run this locally while the real Analytics Backend is under development —
the ETL, embeddings, and chat pipeline can all be tested end-to-end without
waiting for the backend team.

Usage (standalone):
    uvicorn tests.mocks.mock_analytics_server:app --port 8001 --reload

Then point your .env at:
    ANALYTICS_BACKEND_URL=http://localhost:8001

Usage (in pytest — ephemeral):
    from tests.mocks.mock_analytics_server import start_mock_server
    server = start_mock_server()   # starts on a free port
    yield server.base_url
    server.stop()
"""

import copy
import socket
import threading
from datetime import date, datetime

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from tests.mocks.revenue_fixtures import FIXTURES as REVENUE_FIXTURES
from tests.mocks.appointments_fixtures import FIXTURES as APPOINTMENTS_FIXTURES
from tests.mocks.appointments_fixtures_2026 import (
    MONTHLY_SUMMARY_2026_ROWS,
    STAFF_BREAKDOWN_2026_ROWS,
    SERVICE_BREAKDOWN_2026_ROWS,
)
from tests.mocks.staff_performance_fixtures import (
    MONTHLY_PERFORMANCE,
    SUMMARY_PERFORMANCE,
)
from tests.mocks.staff_appointments_fixtures import STAFF_ATTENDANCE
from tests.mocks.services_fixtures import FIXTURES as SERVICES_FIXTURES
from tests.mocks.clients_fixtures import FIXTURES as CLIENTS_FIXTURES
from tests.mocks.marketing_fixtures import FIXTURES as MARKETING_FIXTURES
from tests.mocks.expenses_fixtures import FIXTURES as EXPENSES_FIXTURES
from tests.mocks.promos_fixtures import FIXTURES as PROMOS_FIXTURES
from tests.mocks.giftcards_fixtures import FIXTURES as GIFTCARDS_FIXTURES
from tests.mocks.forms_fixtures import (   # (Domain 10)
    BUSINESS_ID    as FORMS_BIZ,
    ANCHORS        as FORMS_ANCHORS,
    SUBMISSIONS    as FORMS_SUBMISSIONS,
    STUCK_THRESHOLD as FORMS_STUCK_THRESHOLD,
)
from tests.mocks.memberships_fixtures import (   # NEW (Domain 11)
    get_memberships_fixture,
    get_memberships_monthly_fixture,
)


# ── Merge 2026 data into appointments fixtures ────────────────────────────────
# Deep-copy to avoid mutating the imported module-level dicts
_appt_fixtures = copy.deepcopy(APPOINTMENTS_FIXTURES)

_appt_fixtures["/api/v1/leo/appointments/monthly-summary"]["data"].extend(
    MONTHLY_SUMMARY_2026_ROWS
)
_appt_fixtures["/api/v1/leo/appointments/by-staff"]["data"].extend(
    STAFF_BREAKDOWN_2026_ROWS
)
_appt_fixtures["/api/v1/leo/appointments/by-service"]["data"].extend(
    SERVICE_BREAKDOWN_2026_ROWS
)
# Update meta to reflect 2026 best period
_appt_fixtures["/api/v1/leo/appointments/monthly-summary"]["meta"]["best_period"] = "2026-03"


# ── Build staff performance fixtures ─────────────────────────────────────────
_staff_fixtures = {
    "/api/v1/leo/staff-performance":         MONTHLY_PERFORMANCE,
    "/api/v1/leo/staff-performance-summary": SUMMARY_PERFORMANCE,
    "/api/v1/leo/staff-attendance":          STAFF_ATTENDANCE,
}

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="LEO Mock Analytics Server", version="1.11.0")

# Merge all fixtures into one lookup
ALL_FIXTURES: dict[str, dict] = {
    **REVENUE_FIXTURES,
    **_appt_fixtures,
    **_staff_fixtures,
    **SERVICES_FIXTURES,
    **CLIENTS_FIXTURES,
    **MARKETING_FIXTURES,
    **EXPENSES_FIXTURES,
    **PROMOS_FIXTURES,
    **GIFTCARDS_FIXTURES,   # NEW — 8 simple POST routes, no shape switching
}


# Authorised test business IDs (simulate tenant isolation)
AUTHORISED_BUSINESS_IDS = {42, 99, 101}


def _response_for(path: str, business_id: int) -> dict | None:
    """
    Look up the fixture for this path and patch in the business_id
    so tenant isolation checks pass in tests.
    """
    fixture = ALL_FIXTURES.get(path)
    if fixture is None:
        return None
    data = copy.deepcopy(fixture)
    data["business_id"] = business_id
    return data


def _make_handler(captured_path: str):
    """
    Returns a POST handler for a single endpoint path.
    Uses a closure to capture the path correctly across the loop.
    """
    async def handler(request: Request):
        body = await request.json()
        business_id = body.get("business_id", 0)

        # Reject unknown business IDs (simulate tenant isolation)
        if business_id not in AUTHORISED_BUSINESS_IDS:
            return JSONResponse(
                status_code=403,
                content={"error": f"business_id {business_id} not authorised"},
            )

        data = _response_for(captured_path, business_id)
        if data is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"no fixture found for {captured_path}"},
            )

        return JSONResponse(status_code=200, content=data)

    # FastAPI requires unique function names per route
    handler.__name__ = captured_path.replace("/", "_").strip("_")
    return handler


def _make_staff_performance_handler():
    """
    Handles /api/v1/leo/staff-performance.
    Reads mode from request body: 'monthly' (default) or 'summary'.
    """
    async def handler(request: Request):
        body        = await request.json()
        business_id = body.get("business_id", 0)
        mode        = body.get("mode", "monthly")

        if business_id not in AUTHORISED_BUSINESS_IDS:
            return JSONResponse(
                status_code=403,
                content={"error": f"business_id {business_id} not authorised"},
            )

        path = (
            "/api/v1/leo/staff-performance-summary"
            if mode == "summary"
            else "/api/v1/leo/staff-performance"
        )

        data = _response_for(path, business_id)
        if data is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"no fixture for mode={mode}"},
            )

        return JSONResponse(status_code=200, content=data)

    handler.__name__ = "staff_performance_handler"
    return handler


def _make_promos_codes_handler():
    """
    Handles /api/v1/leo/promos/codes.
    Reads granularity from request body: 'monthly' (default) or 'window'.
    """
    async def handler(request: Request):
        body        = await request.json()
        business_id = body.get("business_id", 0)
        granularity = body.get("granularity", "monthly")

        if business_id not in AUTHORISED_BUSINESS_IDS:
            return JSONResponse(
                status_code=403,
                content={"error": f"business_id {business_id} not authorised"},
            )

        path = (
            "/api/v1/leo/promos/codes-window"
            if granularity == "window"
            else "/api/v1/leo/promos/codes"
        )

        data = _response_for(path, business_id)
        if data is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"no fixture for granularity={granularity}"},
            )

        return JSONResponse(status_code=200, content=data)

    handler.__name__ = "promos_codes_handler"
    return handler


def _make_promos_locations_handler():
    """
    Handles /api/v1/leo/promos/locations.
    Reads shape from request body: 'rollup' (default) or 'by_code'.
    """
    async def handler(request: Request):
        body        = await request.json()
        business_id = body.get("business_id", 0)
        shape       = body.get("shape", "rollup")

        if business_id not in AUTHORISED_BUSINESS_IDS:
            return JSONResponse(
                status_code=403,
                content={"error": f"business_id {business_id} not authorised"},
            )

        path = (
            "/api/v1/leo/promos/locations-by-code"
            if shape == "by_code"
            else "/api/v1/leo/promos/locations"
        )

        data = _response_for(path, business_id)
        if data is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"no fixture for shape={shape}"},
            )

        return JSONResponse(status_code=200, content=data)

    handler.__name__ = "promos_locations_handler"
    return handler


# ── Revenue endpoints (6) ─────────────────────────────────────────────────────
REVENUE_PATHS = [
    "/api/v1/leo/revenue/monthly-summary",
    "/api/v1/leo/revenue/payment-types",
    "/api/v1/leo/revenue/by-staff",
    "/api/v1/leo/revenue/by-location",
    "/api/v1/leo/revenue/promo-impact",
    "/api/v1/leo/revenue/failed-refunds",
]

# ── Appointments endpoints (4) ────────────────────────────────────────────────
APPOINTMENTS_PATHS = [
    "/api/v1/leo/appointments/monthly-summary",
    "/api/v1/leo/appointments/by-staff",
    "/api/v1/leo/appointments/by-service",
    "/api/v1/leo/appointments/staff-service-cross",
]

# ── Staff performance endpoints (1 standard + 1 mode-switched) ───────────────
STAFF_STANDARD_PATHS = [
    "/api/v1/leo/staff-attendance",
]

# ── Services endpoints (5) ────────────────────────────────────────────────────
SERVICES_PATHS = [
    "/api/v1/leo/services/monthly-summary",
    "/api/v1/leo/services/booking-stats",
    "/api/v1/leo/services/staff-matrix",
    "/api/v1/leo/services/co-occurrence",
    "/api/v1/leo/services/catalog",
]

# ── Clients endpoints (3) ─────────────────────────────────────────────────────
CLIENTS_PATHS = [
    "/api/v1/leo/clients/retention-snapshot",
    "/api/v1/leo/clients/cohort-monthly",
    "/api/v1/leo/clients/per-location-monthly",
]

# ── Marketing endpoints (3) ───────────────────────────────────────────────────
MARKETING_PATHS = [
    "/api/v1/leo/marketing/campaign-summary",
    "/api/v1/leo/marketing/channel-monthly",
    "/api/v1/leo/marketing/promo-attribution-monthly",
]

# ── Expenses endpoints (6) ────────────────────────────────────────────────────
EXPENSES_PATHS = [
    "/api/v1/leo/expenses/monthly-summary",
    "/api/v1/leo/expenses/category-breakdown",
    "/api/v1/leo/expenses/location-breakdown",
    "/api/v1/leo/expenses/payment-type-breakdown",
    "/api/v1/leo/expenses/staff-attribution",
    "/api/v1/leo/expenses/category-location-cross",
]

# ── Promos endpoints (4 standard + 2 shape-switched) ──────────────────────────
# /codes and /locations also get custom handlers (below) that switch shape
# based on a request-body parameter (granularity / shape). The -window and
# -by-code paths are also registered directly because the real client POSTs
# to them as distinct endpoints.
PROMOS_STANDARD_PATHS = [
    "/api/v1/leo/promos/monthly",
    "/api/v1/leo/promos/catalog-health",
    "/api/v1/leo/promos/codes-window",
    "/api/v1/leo/promos/locations-by-code",
]

# ── Gift Cards endpoints (8) ── NEW (Domain 9) ────────────────────────────────
# All 8 use the standard _make_handler pattern (no shape switching).
# EP6 (anomalies-snapshot) is always-emit — fixture returns single object even
# when all counts are zero. The doc generator must emit a chunk regardless,
# so the AI can answer "are there any refunds?" with "no, zero" instead of
# "I don't have data" (Q31 acceptance criterion).
GIFTCARDS_PATHS = [
    "/api/v1/leo/giftcards/monthly",
    "/api/v1/leo/giftcards/liability-snapshot",
    "/api/v1/leo/giftcards/by-staff",
    "/api/v1/leo/giftcards/by-location",
    "/api/v1/leo/giftcards/aging-snapshot",
    "/api/v1/leo/giftcards/anomalies-snapshot",
    "/api/v1/leo/giftcards/denomination-snapshot",
    "/api/v1/leo/giftcards/health-snapshot",
]

ALL_PATHS = (
    REVENUE_PATHS
    + APPOINTMENTS_PATHS
    + STAFF_STANDARD_PATHS
    + SERVICES_PATHS
    + CLIENTS_PATHS
    + MARKETING_PATHS
    + EXPENSES_PATHS
    + PROMOS_STANDARD_PATHS
    + GIFTCARDS_PATHS   # NEW
)

for _path in ALL_PATHS:
    app.add_api_route(_path, _make_handler(_path), methods=["POST"])

# Staff performance needs its own handler (mode switching)
app.add_api_route(
    "/api/v1/leo/staff-performance",
    _make_staff_performance_handler(),
    methods=["POST"],
)

# Promos /codes and /locations need their own handlers (shape switching)
app.add_api_route(
    "/api/v1/leo/promos/codes",
    _make_promos_codes_handler(),
    methods=["POST"],
)
app.add_api_route(
    "/api/v1/leo/promos/locations",
    _make_promos_locations_handler(),
    methods=["POST"],
)


# ── Forms endpoints (4) ── NEW (Domain 10) ───────────────────────────────────
# Forms uses a different request shape (date windows / snapshot dates as
# pydantic models) and a {data, meta} envelope distinct from the {business_id,
# data, meta} shape used by the older domains. Date-range filtering for the
# monthly endpoint and the always-emit contract on lifecycle don't fit the
# generic _make_handler factory, so each route gets its own handler.

class FormsWindowRequest(BaseModel):
    business_id: int
    start_date: date
    end_date: date


class FormsSnapshotRequest(BaseModel):
    business_id: int
    snapshot_date: date


class FormsLifecycleRequest(BaseModel):
    business_id: int
    snapshot_date: date


def _serialize_dates(obj):
    if isinstance(obj, dict):
        return {k: _serialize_dates(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize_dates(x) for x in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    return obj


def _forms_envelope(data, business_id: int) -> dict:
    row_count = len(data) if isinstance(data, list) else 1
    return {
        "data": _serialize_dates(data),
        "meta": {
            "business_id":  business_id,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "row_count":    row_count,
        },
    }


def _forms_auth_or_403(business_id: int):
    if business_id not in AUTHORISED_BUSINESS_IDS:
        return JSONResponse(
            status_code=403,
            content={"error": f"business_id {business_id} not authorised"},
        )
    return None


@app.post("/api/v1/leo/forms/catalog-snapshot")
async def forms_catalog_snapshot(req: FormsSnapshotRequest):
    deny = _forms_auth_or_403(req.business_id)
    if deny is not None:
        return deny
    if req.business_id != FORMS_BIZ:
        return _forms_envelope({}, req.business_id)
    return _forms_envelope(FORMS_ANCHORS["catalog"], req.business_id)


@app.post("/api/v1/leo/forms/monthly")
async def forms_monthly(req: FormsWindowRequest):
    deny = _forms_auth_or_403(req.business_id)
    if deny is not None:
        return deny
    if req.business_id != FORMS_BIZ:
        return _forms_envelope([], req.business_id)
    rows = [
        r for r in FORMS_ANCHORS["monthly"]
        if req.start_date <= r["period_start"] <= req.end_date
    ]
    return _forms_envelope(rows, req.business_id)


@app.post("/api/v1/leo/forms/per-form-snapshot")
async def forms_per_form_snapshot(req: FormsSnapshotRequest):
    deny = _forms_auth_or_403(req.business_id)
    if deny is not None:
        return deny
    if req.business_id != FORMS_BIZ:
        return _forms_envelope([], req.business_id)
    return _forms_envelope(FORMS_ANCHORS["per_form"], req.business_id)


@app.post("/api/v1/leo/forms/lifecycle-snapshot")
async def forms_lifecycle_snapshot(req: FormsLifecycleRequest):
    # ALWAYS-EMIT contract (FQ4): even when the biz has no submissions, the
    # response must include a single zero-row object so the doc generator can
    # answer "no forms data" without erroring.
    deny = _forms_auth_or_403(req.business_id)
    if deny is not None:
        return deny
    if req.business_id != FORMS_BIZ:
        return _forms_envelope({
            "snapshot_date":               req.snapshot_date,
            "total_submissions":           0,
            "ready_count":                 0,
            "complete_count":              0,
            "approved_count":              0,
            "unknown_status_count":        0,
            "completion_rate_pct":         None,
            "stuck_ready_count":           0,
            "stuck_ready_total_age_days":  0,
            "most_recent_submission_at":   None,
            "stuck_ready_submission_ids":  [],
        }, req.business_id)

    stuck = [
        s for s in FORMS_SUBMISSIONS
        if s["Status"] == "ready" and s["RecDate"].date() < FORMS_STUCK_THRESHOLD
    ]
    total_age = sum((req.snapshot_date - s["RecDate"].date()).days for s in stuck)

    out = dict(FORMS_ANCHORS["lifecycle"])
    out["stuck_ready_total_age_days"] = total_age
    return _forms_envelope(out, req.business_id)


FORMS_PATHS = [
    "/api/v1/leo/forms/catalog-snapshot",
    "/api/v1/leo/forms/monthly",
    "/api/v1/leo/forms/per-form-snapshot",
    "/api/v1/leo/forms/lifecycle-snapshot",
]


# ── Memberships endpoints (2) ── NEW (Domain 11) ──────────────────────────────
# Memberships uses GET with query params (different from the POST-with-body
# pattern used by older domains). The fixtures already build their own envelope
# (business_id, as_of_date, generated_at, row_count, data) and handle tenant
# isolation by returning empty data for unknown business_ids.

@app.get("/api/v1/analytics/memberships")
def memberships(
    business_id: int = Query(..., description="Tenant ID — server-enforced in prod"),
    as_of_date: date | None = Query(None, description="Snapshot date; defaults to today"),
    include_canceled: bool = Query(True, description="Include canceled memberships"),
):
    """Set A — unit-grain memberships."""
    payload = get_memberships_fixture(business_id, as_of_date)
    if not include_canceled:
        payload["data"] = [r for r in payload["data"] if r["is_active"] == 1]
        payload["row_count"] = len(payload["data"])
    return payload


@app.get("/api/v1/analytics/memberships/monthly")
def memberships_monthly(
    business_id: int = Query(...),
    start_date: date = Query(...),
    end_date: date = Query(...),
):
    """Set B — location-month rollup."""
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    months_in_range = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1
    if months_in_range > 36:
        raise HTTPException(status_code=400, detail="Max date range is 36 months")
    return get_memberships_monthly_fixture(business_id, start_date, end_date)


MEMBERSHIPS_PATHS = [
    "/api/v1/analytics/memberships",
    "/api/v1/analytics/memberships/monthly",
]


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "mode": "mock",
        "version": "1.11.0",
        "endpoints": {
            "revenue":      len(REVENUE_PATHS),
            "appointments": len(APPOINTMENTS_PATHS),
            "staff":        len(STAFF_STANDARD_PATHS) + 1,  # +1 for mode-switched
            "services":     len(SERVICES_PATHS),
            "clients":      len(CLIENTS_PATHS),
            "marketing":    len(MARKETING_PATHS),
            "expenses":     len(EXPENSES_PATHS),
            "promos":       len(PROMOS_STANDARD_PATHS) + 2,   # +2 for shape-switched /codes and /locations
            "giftcards":    len(GIFTCARDS_PATHS),
            "forms":        len(FORMS_PATHS),                  # 4 custom-handler routes
            "memberships":  len(MEMBERSHIPS_PATHS),             # NEW — 2 GET routes
            "total":        len(ALL_PATHS) + 3 + len(FORMS_PATHS) + len(MEMBERSHIPS_PATHS),
        },
    }


# ── Programmatic server for pytest ───────────────────────────────────────────

class MockAnalyticsServer:
    """
    Starts the mock server in a background thread.
    Picks a free port automatically.
    """

    def __init__(self, host: str = "127.0.0.1"):
        self.host = host
        self.port = self._free_port()
        self.base_url = f"http://{self.host}:{self.port}"
        self._thread: threading.Thread | None = None
        self._server: uvicorn.Server | None = None

    @staticmethod
    def _free_port() -> int:
        with socket.socket() as s:
            s.bind(("", 0))
            return s.getsockname()[1]

    def start(self):
        config = uvicorn.Config(
            app=app,
            host=self.host,
            port=self.port,
            log_level="warning",
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        import time
        for _ in range(20):
            try:
                with socket.create_connection((self.host, self.port), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.1)

    def stop(self):
        if self._server:
            self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=3)


def start_mock_server() -> MockAnalyticsServer:
    server = MockAnalyticsServer()
    server.start()
    return server


# ── Standalone entry point ────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Starting LEO Mock Analytics Server v1.11.0 on http://localhost:8001")
    print()
    print("Revenue endpoints (6):")
    for p in REVENUE_PATHS:
        print(f"  POST {p}")
    print()
    print("Appointments endpoints (4):")
    for p in APPOINTMENTS_PATHS:
        print(f"  POST {p}")
    print()
    print("Staff performance endpoints (3):")
    print("  POST /api/v1/leo/staff-performance   {mode: 'monthly'|'summary'}")
    print("  POST /api/v1/leo/staff-attendance")
    print()
    print("Services endpoints (5):")
    for p in SERVICES_PATHS:
        print(f"  POST {p}")
    print()
    print("Clients endpoints (3):")
    for p in CLIENTS_PATHS:
        print(f"  POST {p}")
    print()
    print("Marketing endpoints (3):")
    for p in MARKETING_PATHS:
        print(f"  POST {p}")
    print()
    print("Expenses endpoints (6):")
    for p in EXPENSES_PATHS:
        print(f"  POST {p}")
    print()
    print("Promos endpoints (6):")
    for p in PROMOS_STANDARD_PATHS:
        print(f"  POST {p}")
    print("  POST /api/v1/leo/promos/codes      {granularity: 'monthly'|'window'}")
    print("  POST /api/v1/leo/promos/locations  {shape: 'rollup'|'by_code'}")
    print()
    print("Giftcards endpoints (8):")
    for p in GIFTCARDS_PATHS:
        print(f"  POST {p}")
    print()
    print("Forms endpoints (4):")   # (Domain 10)
    for p in FORMS_PATHS:
        print(f"  POST {p}")
    print()
    print("Memberships endpoints (2):")   # NEW (Domain 11)
    for p in MEMBERSHIPS_PATHS:
        print(f"  GET  {p}")
    print()
    print(f"Total: {len(ALL_PATHS) + 3 + len(FORMS_PATHS) + len(MEMBERSHIPS_PATHS)} endpoints")
    print("Health: GET http://localhost:8001/health")
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=True)
