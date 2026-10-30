#!/usr/bin/env python3
"""
app_events: the event stream of Volta, an online electronics shop (app context).

Publishes the shop's app events (page views, product views, add to cart,
checkout, purchase) as JSON, one per line, the way a mobile and web client
would send them to a collector. Standard library; kafka-python-ng is used when
--kafka is given.

Contract (what your pipeline can rely on, and what it cannot):
  - Every event has an `event_id` (16 hex chars), `event_type`, `session_id`,
    `event_ts` (when it happened on the device, ISO 8601 UTC), `received_at`
    (when the collector got it), `page`, `app_version`. `user_id` is null for
    anonymous sessions (about 20%). `product_id` is set on view_product and
    add_to_cart, `order_id` and the amount on purchase.
  - Delivery is AT LEAST ONCE: about 3% of events are sent twice, same
    event_id, same payload, a later received_at. Dedupe on event_id.
  - About 2% of events arrive LATE and out of order: event_ts up to two hours
    before received_at, sometimes across midnight. A daily aggregate closed at
    00:05 is wrong by then.
  - SCHEMA DRIFT: app_version 1.x sends the purchase amount as `value`;
    app_version 2.x (about 35% of sessions) sends `amount` plus `currency`.
  - About 0.4% of product ids are not in the catalogue (a product deleted from
    the shop's database after the view).
  - User ids are the shop's users (1 to 1200), product ids its products
    (1 to 200). Purchase order ids are the app's own (100000 and up).

Deterministic per (seed, date): the same day always produces the same events,
so a test can assert on `stats()`.

  python app_events.py --sink file --out data/app_events --rate 20
  python app_events.py --sink stdout --date 2026-11-03 | head
  python app_events.py --backfill 3 --out data/app_events     (three past days, at once)
  python app_events.py --stats [--date 2026-11-03]            (reference counts, no output files)
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import date, datetime, timedelta, timezone

PAGES = ["/", "/search", "/category/laptops", "/category/chairs", "/category/desks", "/category/monitors", "/cart", "/checkout", "/account"]
N_USERS, N_PRODUCTS = 1200, 200
DEFAULT_SEED = 42


def _hex(rng: random.Random, n: int) -> str:
    return "".join(rng.choice("0123456789abcdef") for _ in range(n))


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def day_events(day: date, seed: int = DEFAULT_SEED, sessions: int = 4000) -> list[dict]:
    """The day's events in emission (received_at) order, duplicates included."""
    rng = random.Random(f"{seed}:{day.isoformat()}")
    d0 = datetime(day.year, day.month, day.day)
    base: list[dict] = []
    order_seq = 100000 + (day - date(2026, 1, 1)).days * 1000
    for _ in range(sessions):
        session_id = _hex(rng, 12)
        user_id = None if rng.random() < 0.2 else rng.randint(1, N_USERS)
        v2 = rng.random() < 0.35
        app_version = "2.0.0" if v2 else "1.8.2"
        t = d0 + timedelta(seconds=rng.randint(0, 86399))
        cart: list[int] = []
        for i in range(max(1, int(rng.gauss(3.5, 1.8)))):
            t += timedelta(seconds=rng.randint(5, 240))
            etype = "page_view" if i == 0 else rng.choices(["page_view", "view_product", "add_to_cart", "checkout"], [45, 35, 15, 5])[0]
            page = rng.choice(PAGES)
            product_id = None
            if etype in ("view_product", "add_to_cart"):
                product_id = rng.randint(1, N_PRODUCTS) if rng.random() > 0.004 else rng.randint(900, 960)
                page = f"/product/{product_id}"
                if etype == "add_to_cart":
                    cart.append(product_id)
            if etype == "checkout":
                page = "/checkout"
            base.append({"session_id": session_id, "user_id": user_id, "event_type": etype, "ts": t, "page": page, "product_id": product_id, "order_id": None, "value": None, "app_version": app_version})
        if cart and rng.random() < 0.3:
            t += timedelta(seconds=rng.randint(20, 400))
            order_seq += 1
            value = round(sum(rng.uniform(19, 1900) for _ in cart[:3]), 2)
            base.append({"session_id": session_id, "user_id": user_id, "event_type": "purchase", "ts": t, "page": "/checkout/done", "product_id": None, "order_id": order_seq, "value": value, "app_version": app_version})
    for e in base:
        e["event_id"] = _hex(rng, 16)
        e["received_at"] = e["ts"] + timedelta(seconds=rng.randint(0, 4))
        if rng.random() < 0.02:
            e["received_at"] = e["ts"] + timedelta(seconds=rng.randint(600, 7200))
            e["late"] = True
    dups = []
    for e in rng.sample(base, int(len(base) * 0.03)):
        d = dict(e)
        d["received_at"] = e["received_at"] + timedelta(seconds=rng.randint(1, 90))
        d["duplicate"] = True
        dups.append(d)
    out = base + dups
    out.sort(key=lambda e: e["received_at"])
    return out


def to_json(e: dict) -> str:
    v2 = e["app_version"].startswith("2.")
    rec = {
        "event_id": e["event_id"], "event_type": e["event_type"], "user_id": e["user_id"], "session_id": e["session_id"],
        "event_ts": _iso(e["ts"]), "received_at": _iso(e["received_at"]), "page": e["page"], "product_id": e["product_id"], "order_id": e["order_id"],
        "app_version": e["app_version"],
    }
    if e["event_type"] == "purchase":
        if v2:
            rec["amount"] = e["value"]
            rec["currency"] = "USD"
        else:
            rec["value"] = e["value"]
    return json.dumps(rec, separators=(",", ":"))


def stats(day: date | None = None, seed: int = DEFAULT_SEED) -> dict:
    day = day or date.today()
    events = day_events(day, seed)
    uniq = [e for e in events if not e.get("duplicate")]
    by_type: dict[str, int] = {}
    for e in uniq:
        by_type[e["event_type"]] = by_type.get(e["event_type"], 0) + 1
    return {
        "source": "app_events", "day": day.isoformat(), "seed": seed,
        "rows": len(events), "unique_events": len(uniq), "duplicate_rows": len(events) - len(uniq),
        "late_events": sum(1 for e in uniq if e.get("late")),
        "crossed_midnight": sum(1 for e in uniq if e["received_at"].date() != e["ts"].date()),
        "sessions": len({e["session_id"] for e in uniq}),
        "anonymous_events": sum(1 for e in uniq if e["user_id"] is None),
        "by_type": by_type,
        "purchases_v2_with_amount": sum(1 for e in uniq if e["event_type"] == "purchase" and e["app_version"].startswith("2.")),
        "unknown_product_events": sum(1 for e in uniq if e["product_id"] is not None and e["product_id"] > N_PRODUCTS),
        "purchase_total": round(sum(e["value"] for e in uniq if e["event_type"] == "purchase"), 2),
    }


def emit(events, sink: str, out: str, day: date, rate: float, kafka: str | None, realtime: bool) -> None:
    producer = None
    if kafka:
        try:
            from kafka import KafkaProducer  # type: ignore
            producer = KafkaProducer(bootstrap_servers=kafka.split(","), value_serializer=lambda v: v.encode(), key_serializer=lambda k: k.encode())
        except Exception as exc:  # the broker is optional; files keep flowing
            print(f"app_events: kafka unavailable ({exc}); writing files only", file=sys.stderr)
    fh = None
    if sink == "file":
        os.makedirs(out, exist_ok=True)
        fh = open(os.path.join(out, f"events_{day.isoformat()}.jsonl"), "a")
    try:
        for e in events:
            line = to_json(e)
            if sink == "stdout":
                print(line)
            elif fh:
                fh.write(line + "\n")
                fh.flush()
            if producer:
                producer.send("app.events", key=e["session_id"], value=line)
            if realtime and rate > 0:
                time.sleep(1.0 / rate)
    finally:
        if fh:
            fh.close()
        if producer:
            producer.flush()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sink", choices=["file", "stdout"], default="file")
    ap.add_argument("--out", default="data/app_events")
    ap.add_argument("--rate", type=float, default=20, help="events per second in live mode")
    ap.add_argument("--kafka", default="", help="bootstrap servers, e.g. redpanda:29092 (topic app.events)")
    ap.add_argument("--date", help="simulate this day (default: today, UTC)")
    ap.add_argument("--backfill", type=int, default=0, help="write this many past days at once, then stop")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--stats", action="store_true", help="print the day's reference counts as JSON and exit")
    a = ap.parse_args()
    day = date.fromisoformat(a.date) if a.date else datetime.now(timezone.utc).date()
    if a.stats:
        print(json.dumps(stats(day, a.seed), indent=1))
        return
    if a.backfill:
        for i in range(a.backfill, 0, -1):
            d = day - timedelta(days=i)
            emit(day_events(d, a.seed), a.sink, a.out, d, a.rate, a.kafka or None, realtime=False)
            print(f"app_events: wrote {d}", file=sys.stderr)
        return
    print(f"app_events: live, {a.rate}/s, day {day}, sink {a.sink}{' + kafka ' + a.kafka if a.kafka else ''}", file=sys.stderr)
    while True:
        emit(day_events(day, a.seed), a.sink, a.out, day, a.rate, a.kafka or None, realtime=True)
        day = day + timedelta(days=1)


if __name__ == "__main__":
    main()
