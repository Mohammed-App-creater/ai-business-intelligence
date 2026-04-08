"""
tests/mocks/mock_analytics_server.py

A lightweight FastAPI server that returns fixture data for all 6 revenue
endpoints. Run this locally while the real Analytics Backend is under
development — the ETL, embeddings, and chat pipeline can all be tested
end-to-end without waiting for the backend team.

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

import threading
import socket
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from revenue_fixtures import FIXTURES

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="LEO Mock Analytics Server", version="1.0.0")


def _response_for(path: str, business_id: int) -> dict:
    """
    Look up the fixture for this path and patch in the business_id
    so tenant isolation checks pass in tests.
    """
    fixture = FIXTURES.get(path)
    if fixture is None:
        return None
    # Deep-copy to avoid mutating the shared fixture
    import copy
    data = copy.deepcopy(fixture)
    data["business_id"] = business_id
    return data


# Register all 6 revenue endpoints
REVENUE_PATHS = [
    "/api/v1/leo/revenue/monthly-summary",
    "/api/v1/leo/revenue/payment-types",
    "/api/v1/leo/revenue/by-staff",
    "/api/v1/leo/revenue/by-location",
    "/api/v1/leo/revenue/promo-impact",
    "/api/v1/leo/revenue/failed-refunds",
]

for _path in REVENUE_PATHS:
    # Use a closure to capture the path correctly in the loop
    def _make_handler(captured_path: str):
        async def handler(request: Request):
            body = await request.json()
            business_id = body.get("business_id", 0)

            # Reject unknown business IDs (simulate tenant isolation)
            if business_id not in (42, 99, 101):
                return JSONResponse(
                    status_code=403,
                    content={"error": f"business_id {business_id} not authorised"},
                )

            data = _response_for(captured_path, business_id)
            if data is None:
                return JSONResponse(status_code=404, content={"error": "not found"})

            return JSONResponse(status_code=200, content=data)

        handler.__name__ = captured_path.replace("/", "_").strip("_")
        return handler

    app.add_api_route(_path, _make_handler(_path), methods=["POST"])


@app.get("/health")
async def health():
    return {"status": "ok", "mode": "mock"}


# ── Programmatic server for pytest ───────────────────────────────────────────

class MockAnalyticsServer:
    """
    Starts the mock server in a background thread.
    Picks a free port automatically.

    Usage:
        server = MockAnalyticsServer()
        server.start()
        yield server.base_url
        server.stop()
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
            log_level="warning",   # quiet during tests
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        # Wait until the server is actually accepting connections
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
    print("Starting LEO Mock Analytics Server on http://localhost:8001")
    print("Available endpoints:")
    for p in REVENUE_PATHS:
        print(f"  POST {p}")
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=True)
