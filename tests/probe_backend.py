#!/usr/bin/env python3
"""
Backend probe — figure out what's reachable and what auth scheme is needed.

Hits a list of common discovery endpoints (root, health, swagger, openapi)
with no auth, then probes the appointments endpoint with every auth scheme
you provide a value for. Prints status + key response headers (especially
WWW-Authenticate, which is the server's hint about what auth it wants), and
saves every full response to disk.

Usage
-----
    # Minimum — just discovery + no-auth probe
    python tests/probe_backend.py --biz 40

    # Try multiple auth schemes — provide the ones you have, skip the rest
    python tests/probe_backend.py --biz 40 \\
        --api-key SOME_KEY \\
        --bearer eyJ... \\
        --functions-key abc123

Anything you don't provide is simply skipped. Each provided scheme is tried
once against /analytics/{biz}/appointments/monthly-summary so you can see
which (if any) gets a 200.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any

import httpx


DEFAULT_BASE = "https://uat-ext-api-a3bre0gyhzaxhbau.eastus2-01.azurewebsites.net"


def probe(client: httpx.Client, method: str, url: str,
          headers: dict, params: dict) -> dict:
    """Make one request, return a structured result. Never raises."""
    # Redact secret values when storing — keep header names visible.
    safe_headers = {}
    for k, v in headers.items():
        if any(s in k.lower() for s in ("key", "auth", "token")):
            safe_headers[k] = f"<set:{len(v)} chars>"
        else:
            safe_headers[k] = v

    try:
        r = client.request(method, url, headers=headers, params=params)
        return {
            "url": str(r.request.url),
            "method": method,
            "request_headers": safe_headers,
            "status": r.status_code,
            "response_headers": dict(r.headers),
            "body_preview": r.text[:1000],
            "body_length": len(r.text),
            "elapsed_ms": int(r.elapsed.total_seconds() * 1000),
        }
    except Exception as e:
        return {
            "url": url,
            "method": method,
            "request_headers": safe_headers,
            "status": -1,
            "response_headers": {},
            "body_preview": "",
            "body_length": 0,
            "elapsed_ms": 0,
            "error": str(e),
        }


def print_row(label: str, r: dict) -> None:
    """One-line summary per probe."""
    status = r["status"]
    tag = {
        200: "[ OK ]",
        301: "[ -> ]", 302: "[ -> ]", 307: "[ -> ]", 308: "[ -> ]",
        401: "[401 ]",
        403: "[403 ]",
        404: "[404 ]",
        500: "[500 ]",
        -1: "[ERR ]",
    }.get(status, f"[{status:3d} ]")

    # Body preview in one line
    body = (r.get("body_preview") or "").strip().replace("\n", " ")
    body_hint = " | " + body[:90] if body else ""

    print(f"  {tag} {label:42s}{body_hint}")

    # Anything useful in response headers? Highlight the big ones.
    rh = r.get("response_headers", {}) or {}
    # Headers that hint at what's going on
    interesting = {
        "www-authenticate": "  >>> WWW-Authenticate (server tells you what auth it wants)",
        "location":         "  >>> Location (redirect target)",
        "x-powered-by":     "  >>> X-Powered-By",
        "server":           "  >>> Server",
    }
    # httpx lowercases header names already
    for hk, hint in interesting.items():
        if hk in rh and (status >= 300 or hk == "www-authenticate"):
            value = rh[hk]
            if len(value) > 120:
                value = value[:120] + "..."
            print(f"        {hint}: {value}")

    if r.get("error"):
        print(f"        >>> error: {r['error']}")


def main() -> int:
    # Force UTF-8 stdout so any unicode in response bodies prints cleanly.
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--base", default=os.environ.get("ANALYTICS_BASE", DEFAULT_BASE))
    p.add_argument("--biz",  default=os.environ.get("BIZ_ID", "40"))
    p.add_argument("--from", dest="frm", default="2025-10-01")
    p.add_argument("--to",   default="2026-03-31")

    # Auth schemes — if a value is set, the corresponding probe runs.
    p.add_argument("--api-key", default=os.environ.get("API_KEY", ""),
                   help="Tries header: X-API-Key: <value>")
    p.add_argument("--bearer", default=os.environ.get("BEARER", ""),
                   help="Tries header: Authorization: Bearer <value>")
    p.add_argument("--functions-key", default=os.environ.get("FUNCTIONS_KEY", ""),
                   help="Tries header: x-functions-key: <value> (Azure Functions)")
    p.add_argument("--apim-key", default=os.environ.get("APIM_KEY", ""),
                   help="Tries header: Ocp-Apim-Subscription-Key: <value> (Azure APIM)")

    p.add_argument("--out", default="probe_results")
    args = p.parse_args()

    base = args.base.rstrip("/")
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.out) / timestamp
    out_dir.mkdir(parents=True, exist_ok=True)

    appt_url = f"{base}/analytics/{args.biz}/appointments/monthly-summary"
    appt_params = {"from": args.frm, "to": args.to}

    # ---- Discovery (no auth, see what the server even is) ----
    discovery = [
        ("GET /",                       f"{base}/"),
        ("GET /health",                 f"{base}/health"),
        ("GET /api/health",             f"{base}/api/health"),
        ("GET /docs (FastAPI swagger)", f"{base}/docs"),
        ("GET /redoc (FastAPI redoc)",  f"{base}/redoc"),
        ("GET /openapi.json",           f"{base}/openapi.json"),
        ("GET /swagger/index.html",     f"{base}/swagger/index.html"),
        ("GET /swagger/v1/swagger.json", f"{base}/swagger/v1/swagger.json"),
        ("GET /analytics/{biz}/appointments/monthly-summary  (no auth)",
         appt_url),
    ]

    # ---- Auth probes ----
    auth_probes = []
    if args.api_key:
        auth_probes.append(("X-API-Key",                   {"X-API-Key": args.api_key}))
    if args.bearer:
        auth_probes.append(("Authorization Bearer",        {"Authorization": f"Bearer {args.bearer}"}))
    if args.functions_key:
        auth_probes.append(("x-functions-key",             {"x-functions-key": args.functions_key}))
    if args.apim_key:
        auth_probes.append(("Ocp-Apim-Subscription-Key",   {"Ocp-Apim-Subscription-Key": args.apim_key}))

    results: list[dict[str, Any]] = []

    print()
    print("=" * 78)
    print(f"Probing {base}")
    print("=" * 78)

    with httpx.Client(follow_redirects=False, timeout=15) as client:
        print()
        print("--- Discovery (no auth) ---")
        for label, url in discovery:
            params = appt_params if "monthly-summary" in url else {}
            r = probe(client, "GET", url, {}, params)
            results.append({"label": label, **r})
            print_row(label, r)

        if auth_probes:
            print()
            print(f"--- Auth probes against /analytics/{args.biz}/appointments/monthly-summary ---")
            for label, headers in auth_probes:
                r = probe(client, "GET", appt_url, headers, appt_params)
                results.append({"label": f"AUTH: {label}", **r})
                print_row(f"AUTH: {label}", r)
        else:
            print()
            print("--- No auth schemes provided — skipping auth probes ---")
            print("    Re-run with --api-key / --bearer / --functions-key / --apim-key")
            print("    to test specific auth headers.")

    # Save full results
    out_path = out_dir / "results.json"
    out_path.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")

    print()
    print("=" * 78)
    print(f"Full responses saved -> {out_path}")
    print("=" * 78)
    print()
    print("How to interpret:")
    print("  [ OK ]  200 — endpoint works (or auth scheme works for that auth probe)")
    print("  [ -> ]  3xx — redirect; look at Location header")
    print("  [401 ]  401 — auth required; look at WWW-Authenticate header for the scheme")
    print("  [403 ]  403 — auth received but rejected (wrong tenant or scope)")
    print("  [404 ]  404 — endpoint not present at this path")
    print("  [500 ]  500 — backend error; body preview shows the cause")
    print("  [ERR ]  network/socket failure")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())