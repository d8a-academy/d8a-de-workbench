#!/usr/bin/env python3
"""
eod_files: the end-of-day files of Solde's core banking system (finance context).

Every night at 02:00 the core system drops the previous day's files into an
incoming folder, the way a mainframe export does:

  transactions_YYYYMMDD.csv   one row per transaction: id, account_id, txn_ts, type,
                              amount, channel, merchant_category, currency
  ledger_YYYYMMDD.csv         the double-entry postings: entry_id, txn_id, account_id,
                              house_account, side (debit|credit), amount, currency
  control_YYYYMMDD.json       the control totals the file must reconcile to: row counts,
                              sum of amounts, sha256 of each file
  processor_YYYYMMDD.csv      the card processor's settlement statement for the day, one row
                              per channel (mobile, online): txn_count, gross_amount, fee_amount,
                              net_amount, currency, statement_ref. This is the OTHER party's view
                              of the day's card withdrawals and transfers.

Contract (what your pipeline can rely on, and what it cannot):
  - Account ids are the bank's accounts (1 to 2044). Types: deposit, withdrawal,
    transfer, fee, interest. Channels: mobile, online, branch, atm.
  - The files are complete for the day when the control totals match. Check them
    before loading anything: one day in fifty is CORRUPTED (the last row is
    truncated and the sum does not match the control). Quarantine it, do not load it.
  - One day in ten arrives LATE (45 minutes after 02:00). A job that assumes the
    file is there at 02:05 fails loudly on those days, which is what you want.
  - About every twenty days the core system RESTATES a previous day: a
    transactions_YYYYMMDD_v2.csv for a day you already loaded, with a few amounts
    changed. Your warehouse must be able to replace that day.
  - Twelve ledger postings per quarter are UNBALANCED by one cent. The ledger is
    still what the auditor reads; the reconciliation report must list them.
  - The PROCESSOR STATEMENT does not always agree with the transactions file: about
    one day in seventeen the processor restated the online channel (gross higher by
    250.00 USD and one more transaction than our file); one day in twenty-three the
    statement is missing; one day in thirty-one the fee does not follow the contract
    (1.5% of gross plus 0.10 USD per transaction). The reconciliation report lists
    every difference, to the cent, by day and channel.
  - PII: the files carry account ids, never names. Keep it that way downstream.

Deterministic per (seed, date).

  python eod_files.py --out data/eod/incoming --at 02:00 --backfill 7   (seven past days now, then nightly)
  python eod_files.py --now --date 2026-11-03                            (drop one day now)
  python eod_files.py --stats --date 2026-11-03                          (reference counts, no files)
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import random
import sys
import time
from datetime import date, datetime, timedelta, timezone

DEFAULT_SEED = 42
N_ACCOUNTS = 2044
HOUSE = {"deposit": "CASH", "withdrawal": "CASH", "transfer": "SETTLEMENT", "fee": "FEES", "interest": "INTEREST"}
MERCHANTS = ["grocery", "fuel", "restaurant", "travel", "online_retail", "utilities", "health", "entertainment", ""]
EPOCH = date(2026, 1, 1)


def _daynum(day: date) -> int:
    return (day - EPOCH).days


def is_late(day: date) -> bool:
    return _daynum(day) % 10 == 3


def is_corrupted(day: date) -> bool:
    return _daynum(day) % 50 == 7


def statement_restated(day: date) -> bool:
    return _daynum(day) % 17 == 5


def statement_missing(day: date) -> bool:
    return _daynum(day) % 23 == 9


def statement_bad_fee(day: date) -> bool:
    return _daynum(day) % 31 == 12


def processor_statement(day: date, txns: list[list]) -> list[list]:
    """The processor's view of the day's card withdrawals and transfers, per channel."""
    rows: list[list] = []
    for ch in ("mobile", "online"):
        card = [t for t in txns if t[5] == ch and t[3] in ("withdrawal", "transfer")]
        n, gross = len(card), round(sum(float(t[4]) for t in card), 2)
        if statement_restated(day) and ch == "online":
            n, gross = n + 1, round(gross + 250.00, 2)
        fee = round(gross * 0.015 + 0.10 * n, 2)
        if statement_bad_fee(day) and ch == "mobile":
            fee = round(fee + 9.99, 2)
        rows.append([day.isoformat(), ch, n, f"{gross:.2f}", f"{fee:.2f}", f"{gross - fee:.2f}", "USD", f"ST{day.strftime('%Y%m%d')}{ch[:1].upper()}"])
    return rows


STATEMENT_HEADER = ["statement_date", "channel", "txn_count", "gross_amount", "fee_amount", "net_amount", "currency", "statement_ref"]


def restates(day: date) -> date | None:
    """The previous day this day's drop restates, if any (every 20 days)."""
    return day - timedelta(days=3) if _daynum(day) % 20 == 11 else None


def day_rows(day: date, seed: int = DEFAULT_SEED) -> tuple[list[list], list[list], dict]:
    rng = random.Random(f"{seed}:eod:{day.isoformat()}")
    n = rng.randint(1500, 2100)
    txn_base = 10_000_000 + _daynum(day) * 10_000
    txns: list[list] = []
    ledger: list[list] = []
    entry_id = txn_base * 2
    unbalanced = set(rng.sample(range(n), 12 if day.day == 1 else 0))  # twelve a quarter, planted on the first of the month that opens it
    unbalanced = unbalanced if day.month in (1, 4, 7, 10) else set()
    for i in range(n):
        tid = txn_base + i
        ttype = rng.choices(["deposit", "withdrawal", "transfer", "fee", "interest"], [30, 35, 25, 6, 4])[0]
        amount = round({"deposit": rng.uniform(20, 3000), "withdrawal": rng.uniform(5, 900), "transfer": rng.uniform(10, 5000), "fee": rng.uniform(1, 25), "interest": rng.uniform(0.5, 60)}[ttype], 2)
        channel = rng.choices(["mobile", "online", "branch", "atm"], [45, 30, 10, 15])[0]
        ts = datetime(day.year, day.month, day.day, rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59))
        acc = rng.randint(1, N_ACCOUNTS)
        txns.append([tid, acc, ts.strftime("%Y-%m-%d %H:%M:%S"), ttype, f"{amount:.2f}", channel, rng.choice(MERCHANTS) if ttype in ("withdrawal", "transfer") else "", "USD"])
        cust_side, house_side = ("credit", "debit") if ttype in ("deposit", "interest") else ("debit", "credit")
        house_amt = round(amount + 0.01, 2) if i in unbalanced else amount
        ledger.append([entry_id, tid, acc, "", cust_side, f"{amount:.2f}", "USD"]); entry_id += 1
        ledger.append([entry_id, tid, "", HOUSE[ttype], house_side, f"{house_amt:.2f}", "USD"]); entry_id += 1
    control = {
        "business_date": day.isoformat(),
        "generated_at": f"{(day + timedelta(days=1)).isoformat()}T02:00:00Z",
        "files": {
            f"transactions_{day.strftime('%Y%m%d')}.csv": {"rows": len(txns), "sum_amount": round(sum(float(r[4]) for r in txns), 2)},
            f"ledger_{day.strftime('%Y%m%d')}.csv": {"rows": len(ledger), "sum_debit": round(sum(float(r[5]) for r in ledger if r[4] == "debit"), 2), "sum_credit": round(sum(float(r[5]) for r in ledger if r[4] == "credit"), 2)},
        },
        "unbalanced_entries_expected": len(unbalanced),
    }
    return txns, ledger, control


TXN_HEADER = ["id", "account_id", "txn_ts", "type", "amount", "channel", "merchant_category", "currency"]
LEDGER_HEADER = ["entry_id", "txn_id", "account_id", "house_account", "side", "amount", "currency"]


def _csv_bytes(header: list[str], rows: list[list]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue().encode()


def build_drop(day: date, seed: int = DEFAULT_SEED) -> dict[str, bytes]:
    """The files of one night's drop, as name -> bytes, planted failures applied."""
    txns, ledger, control = day_rows(day, seed)
    stamp = day.strftime("%Y%m%d")
    tx_bytes = _csv_bytes(TXN_HEADER, txns)
    if is_corrupted(day):
        # The transfer died mid-write: the last line is cut, the control stays complete.
        tx_bytes = tx_bytes[: len(tx_bytes) - 23]
    lg_bytes = _csv_bytes(LEDGER_HEADER, ledger)
    for name, data in ((f"transactions_{stamp}.csv", tx_bytes), (f"ledger_{stamp}.csv", lg_bytes)):
        control["files"][name]["sha256"] = hashlib.sha256(_csv_bytes(TXN_HEADER, txns) if name.startswith("transactions") else data).hexdigest()
    files = {f"transactions_{stamp}.csv": tx_bytes, f"ledger_{stamp}.csv": lg_bytes, f"control_{stamp}.json": json.dumps(control, indent=1).encode()}
    if not statement_missing(day):
        files[f"processor_{stamp}.csv"] = _csv_bytes(STATEMENT_HEADER, processor_statement(day, txns))
    prev = restates(day)
    if prev:
        ptx, _, pctl = day_rows(prev, seed)
        rng = random.Random(f"{seed}:restate:{prev.isoformat()}")
        for i in rng.sample(range(len(ptx)), 3):
            ptx[i][4] = f"{float(ptx[i][4]) + rng.choice([-50.0, 25.5, 100.0]):.2f}"
        pstamp = prev.strftime("%Y%m%d")
        files[f"transactions_{pstamp}_v2.csv"] = _csv_bytes(TXN_HEADER, ptx)
        files[f"control_{pstamp}_v2.json"] = json.dumps({"business_date": prev.isoformat(), "restates": f"transactions_{pstamp}.csv", "reason": "late postings corrected by the core team",
                                                         "files": {f"transactions_{pstamp}_v2.csv": {"rows": len(ptx), "sum_amount": round(sum(float(r[4]) for r in ptx), 2)}}}, indent=1).encode()
    return files


def stats(day: date | None = None, seed: int = DEFAULT_SEED) -> dict:
    day = day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    txns, ledger, control = day_rows(day, seed)
    prev = restates(day)
    return {
        "source": "eod_files", "business_date": day.isoformat(), "seed": seed,
        "transactions": len(txns), "ledger_entries": len(ledger), "sum_amount": control["files"][f"transactions_{day.strftime('%Y%m%d')}.csv"]["sum_amount"],
        "late": is_late(day), "corrupted": is_corrupted(day), "restates": prev.isoformat() if prev else None,
        "unbalanced_entries": control["unbalanced_entries_expected"],
        "by_type": {t: sum(1 for r in txns if r[3] == t) for t in HOUSE},
        "statement": {"missing": statement_missing(day), "restated_online": statement_restated(day), "bad_fee_mobile": statement_bad_fee(day),
                      "rows": [] if statement_missing(day) else processor_statement(day, txns)},
        "our_card_gross_by_channel": {ch: round(sum(float(t[4]) for t in txns if t[5] == ch and t[3] in ("withdrawal", "transfer")), 2) for ch in ("mobile", "online")},
    }


def drop(day: date, out: str, seed: int) -> None:
    os.makedirs(out, exist_ok=True)
    for name, data in build_drop(day, seed).items():
        tmp = os.path.join(out, name + ".part")
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, os.path.join(out, name))
    flags = [f for f, on in (("late", is_late(day)), ("corrupted", is_corrupted(day)), ("restates " + str(restates(day)), restates(day) is not None),
                             ("statement missing", statement_missing(day)), ("statement restated", statement_restated(day)), ("statement fee off", statement_bad_fee(day))) if on]
    print(f"eod_files: dropped {day}{' (' + ', '.join(flags) + ')' if flags else ''}", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="data/eod/incoming")
    ap.add_argument("--at", default="02:00", help="drop time, HH:MM, container clock (UTC)")
    ap.add_argument("--late-minutes", type=int, default=45)
    ap.add_argument("--backfill", type=int, default=0, help="drop this many past days immediately at start")
    ap.add_argument("--now", action="store_true", help="drop one day now and exit (yesterday, or --date)")
    ap.add_argument("--date", help="business date, YYYY-MM-DD")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--stats", action="store_true")
    a = ap.parse_args()
    today = datetime.now(timezone.utc).date()
    day = date.fromisoformat(a.date) if a.date else today - timedelta(days=1)
    if a.stats:
        print(json.dumps(stats(day, a.seed), indent=1))
        return
    if a.now:
        drop(day, a.out, a.seed)
        return
    for i in range(a.backfill, 0, -1):
        drop(today - timedelta(days=i), a.out, a.seed)
    hh, mm = (int(x) for x in a.at.split(":"))
    print(f"eod_files: nightly at {a.at} UTC into {a.out} (one day in ten {a.late_minutes} min late, one in fifty corrupted)", file=sys.stderr)
    while True:
        now = datetime.now(timezone.utc)
        nxt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if nxt <= now:
            nxt += timedelta(days=1)
        business = nxt.date() - timedelta(days=1)
        if is_late(business):
            nxt += timedelta(minutes=a.late_minutes)
        time.sleep(max(1.0, (nxt - now).total_seconds()))
        drop(business, a.out, a.seed)


if __name__ == "__main__":
    main()
