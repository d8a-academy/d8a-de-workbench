#!/usr/bin/env python3
"""
saas_api: the six SaaS APIs of Marea's growth team (marketing context).

One small HTTP server that behaves like the ad platforms, the CRM, the email
tool and the analytics tool a marketing team pulls from. Standard library only.

Endpoints (all GET, all paginated, all behind `Authorization: Bearer <token>`):
  /v1/health
  /v1/ads/campaigns                 the campaigns (id, name, channel, objective, dates, budget, status)
  /v1/ads/spend?since=YYYY-MM-DD    daily spend per campaign (impressions, clicks, spend|cost, conversions, currency)
  /v1/crm/leads?since=              leads (id, campaign_id, created_at, source, country, status, value)
  /v1/crm/contacts                  contacts (id, email_hash, country, plan, created_at)
  /v1/email/sends?since=            email sends (id, contact_id, campaign_id, sent_at, opened, clicked)
  /v1/analytics/sessions?since=     daily sessions by source and country (date, source, country, sessions, conversions)

Response shape: {"data": [...], "next_cursor": "..." | null, "has_more": bool}.
Pagination: `page_size` up to 100 (default 100), follow `next_cursor` until it is null.

Contract (what your extractor can rely on, and what it cannot):
  - RATE LIMIT: every 300th request gets a 429 with a Retry-After header (seconds).
    Honour it and retry; a client that gives up loses a page in the middle of a run.
  - CURSORS EXPIRE after 10 minutes: 410 Gone. Restart the page from `since`, not
    from the beginning.
  - SCHEMA DRIFT: in /v1/ads/spend, rows dated 2024-07-01 or later carry `cost`
    instead of `spend`. The vendor renamed the field and told nobody.
  - Some campaign ids in spend and leads do not exist in /v1/ads/campaigns (a
    campaign deleted from the platform but still billed).
  - `since` is inclusive and filters on the row's date; rows can be updated after
    their date (a lead changes status), so an incremental extractor that only
    pulls new dates misses those updates. The `updated_at` field is what to use.
  - Wrong or missing token: 401.

Deterministic per seed: the data is generated once at startup.

  python saas_api.py --port 8080 [--token d8a-test-token] [--seed 42]
  python saas_api.py --stats            (row counts per endpoint, no server)
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import random
import sys
import threading
import time
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

DEFAULT_SEED = 42
DEFAULT_TOKEN = os.environ.get("SAAS_API_TOKEN", "d8a-test-token")
CHANNELS = ["search", "social", "display", "email", "video", "affiliate"]
COUNTRIES = ["France", "Germany", "Spain", "Italy", "Belgium", "Netherlands", "USA", "UK"]
RENAME_FROM = "2024-07-01"
START, END = date(2024, 1, 1), date(2024, 12, 31)


def build_data(seed: int = DEFAULT_SEED) -> dict[str, list[dict]]:
    rng = random.Random(f"{seed}:saas")
    campaigns = []
    for i in range(1, 121):
        ch = rng.choice(CHANNELS)
        start = START + timedelta(days=rng.randint(0, 300))
        end = min(END, start + timedelta(days=rng.randint(14, 90)))
        campaigns.append({"id": i, "name": f"{ch.title()} {rng.choice(['awareness', 'retention', 'launch', 'promo'])} {i:03d}", "channel": ch, "objective": rng.choice(["awareness", "conversion", "retention"]),
                          "start_date": start.isoformat(), "end_date": end.isoformat(), "budget": round(rng.uniform(1500, 25000), 2), "status": "completed" if end < END else "running",
                          "updated_at": f"{end.isoformat()}T06:00:00Z"})
    deleted = [9001, 9002]  # billed, no longer listed
    spend = []
    sid = 1
    for c in campaigns + [{"id": d, "start_date": "2024-05-01", "end_date": "2024-06-10"} for d in deleted]:
        d = date.fromisoformat(c["start_date"])
        while d <= date.fromisoformat(c["end_date"]):
            imp = rng.randint(2000, 40000)
            clicks = int(imp * rng.uniform(0.002, 0.02))
            amount = round(clicks * rng.uniform(0.4, 1.8), 2)
            row = {"id": sid, "campaign_id": c["id"], "date": d.isoformat(), "impressions": imp, "clicks": clicks, "conversions": int(clicks * rng.uniform(0, 0.08)), "currency": "USD", "updated_at": f"{(d + timedelta(days=1)).isoformat()}T03:00:00Z"}
            row["cost" if d.isoformat() >= RENAME_FROM else "spend"] = amount
            spend.append(row)
            sid += 1
            d += timedelta(days=1)
    leads = []
    for i in range(1, 6001):
        created = START + timedelta(days=rng.randint(0, 365))
        status = rng.choices(["new", "qualified", "won", "lost"], [17, 30, 22, 31])[0]
        updated = created + timedelta(days=rng.randint(0, 40))
        leads.append({"id": i, "campaign_id": rng.choice([c["id"] for c in campaigns] + deleted), "created_at": created.isoformat(), "source": rng.choice(CHANNELS), "country": rng.choice(COUNTRIES),
                      "status": status, "value": round(rng.uniform(40, 900), 2) if status == "won" else None, "updated_at": f"{min(updated, END).isoformat()}T12:00:00Z"})
    contacts = [{"id": i, "email_hash": f"{rng.getrandbits(128):032x}", "country": rng.choice(COUNTRIES), "plan": rng.choices(["basic", "plus", "pro"], [62, 28, 10])[0],
                 "created_at": (START + timedelta(days=rng.randint(0, 365))).isoformat(), "updated_at": f"{END.isoformat()}T00:00:00Z"} for i in range(1, 3001)]
    sends = []
    for i in range(1, 20001):
        sent = START + timedelta(days=rng.randint(0, 365))
        opened = rng.random() < 0.38
        sends.append({"id": i, "contact_id": rng.randint(1, 3000), "campaign_id": rng.choice([c["id"] for c in campaigns if c["channel"] == "email"] or [1]), "sent_at": f"{sent.isoformat()}T{rng.randint(6, 20):02d}:00:00Z",
                      "opened": opened, "clicked": opened and rng.random() < 0.3, "updated_at": f"{sent.isoformat()}T23:00:00Z"})
    sessions = []
    d = START
    while d <= END:
        for src in CHANNELS + ["direct", "organic"]:
            for ctry in COUNTRIES[:5]:
                n = rng.randint(20, 900)
                sessions.append({"date": d.isoformat(), "source": src, "country": ctry, "sessions": n, "conversions": int(n * rng.uniform(0.005, 0.04)), "updated_at": f"{(d + timedelta(days=1)).isoformat()}T01:00:00Z"})
        d += timedelta(days=1)
    return {"ads/campaigns": campaigns, "ads/spend": spend, "crm/leads": leads, "crm/contacts": contacts, "email/sends": sends, "analytics/sessions": sessions}


DATE_FIELD = {"ads/campaigns": "start_date", "ads/spend": "date", "crm/leads": "created_at", "crm/contacts": "created_at", "email/sends": "sent_at", "analytics/sessions": "date"}


def stats(seed: int = DEFAULT_SEED) -> dict:
    data = build_data(seed)
    return {
        "source": "saas_api", "seed": seed,
        "rows": {k: len(v) for k, v in data.items()},
        "spend_rows_with_cost_field": sum(1 for r in data["ads/spend"] if "cost" in r),
        "spend_rows_for_deleted_campaigns": sum(1 for r in data["ads/spend"] if r["campaign_id"] >= 9000),
        "leads_for_deleted_campaigns": sum(1 for r in data["crm/leads"] if r["campaign_id"] >= 9000),
        "spend_total": round(sum(r.get("spend", r.get("cost", 0)) for r in data["ads/spend"]), 2),
        "won_leads_value": round(sum(r["value"] or 0 for r in data["crm/leads"] if r["status"] == "won"), 2),
        "rate_limit_every": 300, "cursor_ttl_seconds": 600, "rename_from": RENAME_FROM,
    }


class Api(BaseHTTPRequestHandler):
    data: dict[str, list[dict]] = {}
    token = DEFAULT_TOKEN
    calls = 0
    lock = threading.Lock()

    def log_message(self, fmt, *args):  # quieter logs: one line per request
        sys.stderr.write("saas_api: %s %s\n" % (self.command, self.path))

    def _send(self, code: int, body: dict, headers: dict | None = None) -> None:
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802
        url = urlparse(self.path)
        if url.path == "/v1/health":
            return self._send(200, {"ok": True, "endpoints": sorted(self.data)})
        if self.headers.get("Authorization") != f"Bearer {self.token}":
            return self._send(401, {"error": "unauthorized", "hint": "Authorization: Bearer <token>"})
        with self.lock:
            Api.calls += 1
            n = Api.calls
        if n % 300 == 0:
            return self._send(429, {"error": "rate_limited", "retry_after": 2}, {"Retry-After": "2"})
        key = url.path.removeprefix("/v1/")
        rows = self.data.get(key)
        if rows is None:
            return self._send(404, {"error": "not_found", "endpoints": sorted(self.data)})
        qs = parse_qs(url.query)
        size = max(1, min(100, int(qs.get("page_size", ["100"])[0])))
        since = qs.get("since", [None])[0]
        offset = 0
        if "cursor" in qs:
            try:
                off, issued = base64.urlsafe_b64decode(qs["cursor"][0].encode()).decode().split(":")
                offset = int(off)
                if time.time() - float(issued) > 600:
                    return self._send(410, {"error": "cursor_expired", "hint": "restart from since, not from the beginning"})
            except Exception:
                return self._send(400, {"error": "bad_cursor"})
        field = DATE_FIELD[key]
        selected = [r for r in rows if since is None or r[field][:10] >= since]
        page = selected[offset: offset + size]
        has_more = offset + size < len(selected)
        cursor = base64.urlsafe_b64encode(f"{offset + size}:{time.time():.0f}".encode()).decode() if has_more else None
        self._send(200, {"data": page, "next_cursor": cursor, "has_more": has_more, "total": len(selected)})


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--token", default=DEFAULT_TOKEN)
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--stats", action="store_true")
    a = ap.parse_args()
    if a.stats:
        print(json.dumps(stats(a.seed), indent=1))
        return
    Api.data = build_data(a.seed)
    Api.token = a.token
    print(f"saas_api: http://0.0.0.0:{a.port}/v1/health, token {a.token!r}, {sum(len(v) for v in Api.data.values()):,} rows across {len(Api.data)} endpoints", file=sys.stderr)
    ThreadingHTTPServer(("0.0.0.0", a.port), Api).serve_forever()


if __name__ == "__main__":
    main()
