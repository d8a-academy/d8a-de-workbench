#!/usr/bin/env python3
"""
telemetry: the fleet telemetry of Nordvik Logistics (operations context).

400 vehicles report every 10 seconds: speed, fuel, engine temperature, GPS,
odometer. That is 3.46 million readings a day, written as a partitioned lake:

  data/lake/telemetry/dt=YYYY-MM-DD/hour=HH/part-<minute>.parquet   (gzip CSV when pyarrow is missing)

Contract (what your job can rely on, and what it cannot):
  - One row per (vehicle_id, ts) is the intent. About 1% of rows are DUPLICATES
    (the gateway retried). Dedupe on (vehicle_id, ts).
  - GAPS: vehicles whose id is a multiple of 37 send nothing between minute 20
    and 25 of every hour (a tunnel, a dead zone). A utilisation metric that
    counts rows undercounts them.
  - A SENSOR THAT LIES: vehicle 63 reports speeds of 300 to 420 km/h on about 2%
    of its rows. Vehicle 3 has a FROZEN GPS: same position all day while moving.
  - LATE DATA: fifteen minutes into each hour, a part-late.* file is added to
    the PREVIOUS hour's partition with about 0.5% more rows. A job that reads a
    partition once and never again misses them.
  - Timestamps are UTC. The depots are in Europe/Oslo. The day a reading belongs
    to depends on which one you pick; say which in your model.
  - Volume: one hour is about 144,000 rows, one day 3.46 million. A pandas
    single-node read of a week is where memory runs out: that is the point of
    the Spark project.

Deterministic per (seed, date, hour, minute).

  python telemetry.py --out data/lake/telemetry --backfill-days 1        (yesterday in full, then live by the minute)
  python telemetry.py --stats --date 2026-11-03 --hour 8                  (reference counts for one hour, no files)
  python telemetry.py --s3 http://minio:9000 ...                         (also upload each part to the lake bucket)
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import hmac
import io
import json
import os
import random
import sys
import time
import urllib.request

DEFAULT_SEED = 42
DEPOTS = [(59.91, 10.75), (60.39, 5.32), (63.43, 10.39), (58.97, 5.73), (59.74, 10.20), (59.22, 10.93), (58.15, 7.99), (69.65, 18.96), (62.47, 6.15), (67.28, 14.40), (61.11, 10.47), (59.41, 5.27)]
HEADER = ["vehicle_id", "ts", "speed_kmh", "fuel_pct", "engine_temp_c", "lat", "lon", "odometer_km"]
FROZEN_GPS_VEHICLE, LIAR_VEHICLE, GAP_EVERY = 3, 63, 37


def minute_rows(day: dt.date, hour: int, minute: int, vehicles: int, seed: int = DEFAULT_SEED) -> list[list]:
    """The readings of one minute (six per vehicle), planted failures included."""
    rng = random.Random(f"{seed}:{day.isoformat()}:{hour}:{minute}")
    rows: list[list] = []
    for vid in range(1, vehicles + 1):
        if vid % GAP_EVERY == 0 and 20 <= minute < 25:
            continue
        vr = random.Random(f"{seed}:v{vid}:{day.isoformat()}:{hour}")
        depot = DEPOTS[(vid - 1) % len(DEPOTS)]
        lat, lon = depot[0] + vr.uniform(-0.3, 0.3), depot[1] + vr.uniform(-0.5, 0.5)
        speed, fuel, odo = vr.uniform(0, 60), vr.uniform(20, 95), vr.uniform(20000, 180000)
        for s in range(6):
            t = dt.datetime(day.year, day.month, day.day, hour, minute, 10 * s)
            speed = max(0.0, min(110.0, speed + rng.gauss(0, 5)))
            if vid != FROZEN_GPS_VEHICLE:
                lat += (speed / 3600 / 111) * 10 * (1 if (vid + s) % 2 else -1)
                lon += (speed / 3600 / 55) * 10 * (1 if (vid + minute) % 2 else -1)
            fuel = max(2.0, fuel - speed / 9000)
            odo += speed / 360
            sp = rng.uniform(300, 420) if vid == LIAR_VEHICLE and rng.random() < 0.02 else speed
            rows.append([vid, t.strftime("%Y-%m-%dT%H:%M:%SZ"), round(sp, 1), round(fuel, 1), round(rng.uniform(78, 96), 1), round(lat, 5), round(lon, 5), round(odo, 1)])
    for r in rng.sample(rows, max(1, len(rows) // 100)):
        rows.append(list(r))
    return rows


def late_rows(day: dt.date, hour: int, vehicles: int, seed: int = DEFAULT_SEED) -> list[list]:
    """About 0.5% of the hour, re-sent fifteen minutes after it closed."""
    rng = random.Random(f"{seed}:late:{day.isoformat()}:{hour}")
    out: list[list] = []
    for minute in range(60):
        rows = minute_rows(day, hour, minute, vehicles, seed)
        out.extend(rows[i] for i in rng.sample(range(len(rows)), max(1, len(rows) // 200)))
    return out


def write_part(rows: list[list], out: str, day: dt.date, hour: int, name: str, s3: str | None) -> str:
    folder = os.path.join(out, f"dt={day.isoformat()}", f"hour={hour:02d}")
    os.makedirs(folder, exist_ok=True)
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
        cols = list(zip(*rows)) if rows else [[] for _ in HEADER]
        table = pa.table({h: list(c) for h, c in zip(HEADER, cols)})
        path = os.path.join(folder, f"{name}.parquet")
        pq.write_table(table, path, compression="snappy")
    except ImportError:
        path = os.path.join(folder, f"{name}.csv.gz")
        with gzip.open(path, "wt", newline="") as f:
            w = csv.writer(f)
            w.writerow(HEADER)
            w.writerows(rows)
    if s3:
        s3_put(s3, "lake", os.path.relpath(path, out).replace(os.sep, "/"), path)
    return path


# ─── A minimal S3 PUT (AWS Signature v4), so MinIO needs no SDK ───────────────

def s3_put(endpoint: str, bucket: str, key: str, path: str) -> None:
    access = os.environ.get("MINIO_ROOT_USER", "d8a")
    secret = os.environ.get("MINIO_ROOT_PASSWORD", "d8a-lake-password")
    region, service = "us-east-1", "s3"
    with open(path, "rb") as f:
        body = f.read()
    host = endpoint.split("://", 1)[1]
    now = dt.datetime.utcnow()
    amz_date, datestamp = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    canonical_uri = f"/{bucket}/" + "/".join(urllib.request.quote(p, safe="=") for p in key.split("/"))
    headers = {"host": host, "x-amz-content-sha256": payload_hash, "x-amz-date": amz_date}
    signed = ";".join(sorted(headers))
    canonical = "\n".join(["PUT", canonical_uri, "", "".join(f"{k}:{headers[k]}\n" for k in sorted(headers)), signed, payload_hash])
    scope = f"{datestamp}/{region}/{service}/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    k = hmac.new(("AWS4" + secret).encode(), datestamp.encode(), hashlib.sha256).digest()
    for part in (region, service, "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    auth = f"AWS4-HMAC-SHA256 Credential={access}/{scope}, SignedHeaders={signed}, Signature={signature}"
    req = urllib.request.Request(endpoint + canonical_uri, data=body, method="PUT", headers={**headers, "Authorization": auth, "Content-Length": str(len(body))})
    try:
        urllib.request.urlopen(req, timeout=30).read()
    except Exception as exc:  # the lake is optional; the files are on disk anyway
        print(f"telemetry: s3 upload failed for {key}: {exc}", file=sys.stderr)


def stats(day: dt.date | None = None, hour: int = 8, vehicles: int = 400, seed: int = DEFAULT_SEED) -> dict:
    day = day or dt.date.today()
    rows = [r for m in range(60) for r in minute_rows(day, hour, m, vehicles, seed)]
    uniq = {(r[0], r[1]) for r in rows}
    return {
        "source": "telemetry", "day": day.isoformat(), "hour": hour, "vehicles": vehicles, "seed": seed,
        "rows": len(rows), "unique_readings": len(uniq), "duplicate_rows": len(rows) - len(uniq),
        "expected_without_gaps": vehicles * 360,
        "gap_vehicles": [v for v in range(1, vehicles + 1) if v % GAP_EVERY == 0], "gap_minutes_per_hour": 5,
        "speed_over_200_rows": sum(1 for r in rows if r[2] > 200), "liar_vehicle": LIAR_VEHICLE, "frozen_gps_vehicle": FROZEN_GPS_VEHICLE,
        "late_rows_added_at_minute_15": len(late_rows(day, hour, vehicles, seed)),
        "rows_per_day_approx": vehicles * 8640,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="data/lake/telemetry")
    ap.add_argument("--vehicles", type=int, default=400)
    ap.add_argument("--backfill-days", type=int, default=1)
    ap.add_argument("--s3", default="", help="MinIO endpoint, e.g. http://minio:9000 (bucket lake)")
    ap.add_argument("--date", help="for --stats")
    ap.add_argument("--hour", type=int, default=8, help="for --stats")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--stats", action="store_true")
    a = ap.parse_args()
    if a.stats:
        print(json.dumps(stats(dt.date.fromisoformat(a.date) if a.date else None, a.hour, a.vehicles, a.seed), indent=1))
        return
    s3 = a.s3 or None
    today = dt.datetime.utcnow().date()
    for i in range(a.backfill_days, 0, -1):
        day = today - dt.timedelta(days=i)
        for hour in range(24):
            rows = [r for m in range(60) for r in minute_rows(day, hour, m, a.vehicles, a.seed)]
            write_part(rows, a.out, day, hour, "part-0000", s3)
            write_part(late_rows(day, hour, a.vehicles, a.seed), a.out, day, hour, "part-late", s3)
        print(f"telemetry: backfilled {day} (24 partitions)", file=sys.stderr)
    print(f"telemetry: live, {a.vehicles} vehicles, one part per minute into {a.out}", file=sys.stderr)
    done_late: set[tuple[dt.date, int]] = set()
    while True:
        now = dt.datetime.utcnow()
        write_part(minute_rows(now.date(), now.hour, now.minute, a.vehicles, a.seed), a.out, now.date(), now.hour, f"part-{now.minute:02d}", s3)
        prev = now - dt.timedelta(hours=1)
        if now.minute >= 15 and (prev.date(), prev.hour) not in done_late:
            write_part(late_rows(prev.date(), prev.hour, a.vehicles, a.seed), a.out, prev.date(), prev.hour, "part-late", s3)
            done_late.add((prev.date(), prev.hour))
        time.sleep(max(1.0, 60 - dt.datetime.utcnow().second))


if __name__ == "__main__":
    main()
