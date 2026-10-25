#!/usr/bin/env python3
"""24h allocation movers for the Aero dashboard, from archived feeds.

Reads the pools-feed snapshots archived on the data-history branch
(history/YYYY/MM/DD/pools-HHMM.json.gz, timestamps are the feeds'
generated_at in UTC), aligns the latest snapshot with the snapshot
closest to 24h before it (and at least 20h old), and writes
dist/data/movers.json:

{ready, generated_at, window_hours, gainers, losers}

Each mover row: {pair, chain, type, delta_abs, delta_pct, allocation,
stake_apr} — allocation / stake_apr are the latest snapshot's values,
deltas are new − old. Gainers / losers are the top 8 by |delta_abs|
among pools present in BOTH snapshots. Pools are aligned by their
"pool" address (feeds carry it since 2026-10-25); snapshots without
it fall back to chain|pair|type keys.

ready=false (with empty lists) when there are not enough snapshots or
the usable span is under 12h — the file is written either way, and the
script exits 0: movers are informational and must never block deploy.

Run: python movers.py --history-dir history-repo/history
     --out dist/data/movers.json
"""
import argparse
import glob
import gzip
import json
import os
import re
import sys
from datetime import datetime, timezone

MIN_AGE_H = 20.0
TARGET_H = 24.0
MIN_SPAN_H = 12.0
TOP_N = 8
NAME_RE = re.compile(r"pools-(\d{2})(\d{2})\.json\.gz$")
DATE_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})$")


def snapshot_ts(path):
    """UTC epoch from the history path layout YYYY/MM/DD/pools-HHMM."""
    m = NAME_RE.search(os.path.basename(path))
    d = DATE_RE.search(os.path.dirname(path).replace(os.sep, "/"))
    if not m or not d:
        return None
    try:
        dt = datetime(int(d.group(1)), int(d.group(2)), int(d.group(3)),
                      int(m.group(1)), int(m.group(2)),
                      tzinfo=timezone.utc)
    except ValueError:
        return None
    return dt.timestamp()


def row_key(row):
    pool = row.get("pool")
    if pool:
        return "addr:" + str(pool).lower()
    return "meta:{}|{}|{}".format(row.get("chain"), row.get("pair"),
                                  row.get("type"))


def collect(history_dir):
    """-> list of {ts, generated_at, rows} sorted by ts (loadable only)."""
    snaps = []
    if not history_dir or not os.path.isdir(history_dir):
        return snaps
    pattern = os.path.join(history_dir, "*", "*", "*", "pools-*.json.gz")
    for path in glob.glob(pattern):
        ts = snapshot_ts(path)
        if ts is None:
            continue
        try:
            with gzip.open(path, "rt") as f:
                doc = json.load(f)
            rows = doc["pools"]
            if not isinstance(rows, list):
                continue
        except Exception as e:
            print(f"movers: skipping {path}: {e}", file=sys.stderr)
            continue
        snaps.append({"ts": ts, "generated_at": doc.get("generated_at"),
                      "rows": rows})
    snaps.sort(key=lambda s: s["ts"])
    return snaps


def mover_row(row, delta_abs, delta_pct):
    alloc = row.get("allocation")
    return {
        "pair": row.get("pair"),
        "chain": row.get("chain"),
        "type": row.get("type"),
        "delta_abs": round(delta_abs, 2),
        "delta_pct": (round(delta_pct, 2) if delta_pct is not None
                      else None),
        "allocation": (round(alloc, 2) if isinstance(alloc, (int, float))
                       else None),
        "stake_apr": row.get("stake_apr"),
    }


def build(history_dir):
    snaps = collect(history_dir)
    out = {"ready": False, "generated_at": None, "window_hours": 0.0,
           "gainers": [], "losers": []}
    if not snaps:
        out["generated_at"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        return out
    latest = snaps[-1]
    out["generated_at"] = latest["generated_at"] or datetime.fromtimestamp(
        latest["ts"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if len(snaps) < 2:
        return out
    span_all = (latest["ts"] - snaps[0]["ts"]) / 3600.0
    out["window_hours"] = round(span_all, 2)
    cutoff = latest["ts"] - MIN_AGE_H * 3600
    target = latest["ts"] - TARGET_H * 3600
    olds = [s for s in snaps if s["ts"] <= cutoff]
    if not olds:
        return out
    old = min(olds, key=lambda s: abs(s["ts"] - target))
    window = (latest["ts"] - old["ts"]) / 3600.0
    out["window_hours"] = round(window, 2)
    if window < MIN_SPAN_H:
        return out

    new_map = {row_key(r): r for r in latest["rows"]}
    old_map = {row_key(r): r for r in old["rows"]}
    deltas = []
    for key, nrow in new_map.items():
        orow = old_map.get(key)
        if orow is None:
            continue
        na, oa = nrow.get("allocation"), orow.get("allocation")
        if not isinstance(na, (int, float)) or not isinstance(oa,
                                                              (int, float)):
            continue
        d = na - oa
        if d == 0:
            continue
        pct = (d / oa * 100.0) if oa else None
        deltas.append((d, mover_row(nrow, d, pct)))
    gainers = [r for d, r in sorted((x for x in deltas if x[0] > 0),
                                    key=lambda x: -x[0])[:TOP_N]]
    losers = [r for d, r in sorted((x for x in deltas if x[0] < 0),
                                   key=lambda x: x[0])[:TOP_N]]
    out.update({"ready": True, "gainers": gainers, "losers": losers})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history-dir", required=True,
                    help="history/ dir of the data-history checkout")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    try:
        result = build(args.history_dir)
    except Exception as e:
        print(f"movers: failed: {e}", file=sys.stderr)
        result = {"ready": False,
                  "generated_at": datetime.now(timezone.utc).strftime(
                      "%Y-%m-%dT%H:%M:%SZ"),
                  "window_hours": 0.0, "gainers": [], "losers": []}
    out_dir = os.path.dirname(os.path.abspath(args.out))
    os.makedirs(out_dir, exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(result, f, indent=1)
    print(f"movers: ready={result['ready']} "
          f"window={result['window_hours']}h "
          f"gainers={len(result['gainers'])} "
          f"losers={len(result['losers'])} -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
