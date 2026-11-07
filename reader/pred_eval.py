#!/usr/bin/env python3
"""Score archived pred_apr predictions against realised outcomes.

The pipeline archives every round's predictions (preds-*.json.gz on
the data-history branch, written by predict.py --preds-out). For a
prediction made at T, the realised APR over [T, T+48h] is built from
the pools snapshot closest to T+48h: its trailing fees48h IS the fee
income accrued over exactly that window, so

    realised = stake_apr(T) x (fees48h(T+48h) + incentives(T))
               / rewards48h(T) x alloc(T) / alloc(T+48h)

with incentives taken as scheduled at T (the forecast's own input).
This mirrors predict.py's ratio construction, so pred and realised
share units by construction.

Metrics: MAE (percentage points) of the prediction vs the naive
baseline (stake_apr itself as the forecast), skill = 1 - MAE ratio,
signed bias, directional accuracy for predictions that moved at
least 0.5pp from the current APR (the ones that show an arrow), a
per-origin Spearman rank correlation, and an allocation-size split.

Run: python pred_eval.py --history-dir <dir>   (the fetched tree,
i.e. the directory containing history/YYYY/MM/DD/*.json.gz)
"""
import argparse
import glob
import gzip
import json
import math
import os
import re
import sys
from datetime import datetime, timezone

_NAME_RE = re.compile(r"(pools|preds)-(\d{2})(\d{2})\.json\.gz$")
_DATE_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})$")
HORIZON_H = 48.0
TOL_H = 2.5


def snap_ts(path):
    m = _NAME_RE.search(os.path.basename(path))
    d = _DATE_RE.search(os.path.dirname(path).replace(os.sep, "/"))
    if not m or not d:
        return None
    try:
        return datetime(int(d.group(1)), int(d.group(2)), int(d.group(3)),
                        int(m.group(2)), int(m.group(3)),
                        tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


def load_kind(history_dir, kind):
    out = {}
    for path in glob.glob(os.path.join(
            history_dir, "*", "*", "*", f"{kind}-*.json.gz")):
        ts = snap_ts(path)
        if ts is None:
            continue
        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                out[ts] = json.load(f)
        except Exception:
            continue
    return out


def ranks(vals):
    order = sorted(range(len(vals)), key=lambda i: vals[i])
    out = [0.0] * len(vals)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and vals[order[j + 1]] == vals[order[i]]:
            j += 1
        for k in range(i, j + 1):
            out[order[k]] = (i + j) / 2.0
        i = j + 1
    return out


def pearson(a, b):
    n = len(a)
    ma, mb = sum(a) / n, sum(b) / n
    cov = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((y - mb) ** 2 for y in b)
    return cov / math.sqrt(va * vb) if va > 0 and vb > 0 else 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history-dir", required=True)
    args = ap.parse_args()
    preds_snaps = load_kind(args.history_dir, "preds")
    pools_snaps = load_kind(args.history_dir, "pools")
    pools_by_key = {}
    for ts, snap in pools_snaps.items():
        rows = {}
        for r in snap.get("pools", []):
            if r.get("pool") and r.get("chain"):
                rows[(r["chain"].lower(), r["pool"].lower())] = r
        pools_by_key[ts] = rows
    pool_times = sorted(pools_by_key)

    def closest(t):
        if not pool_times:
            return None
        best = min(pool_times, key=lambda x: abs(x - t))
        return best if abs(best - t) <= TOL_H * 3600 else None

    pairs = []  # (pred, naive, realised, alloc)
    spears = []
    used_origins = 0
    for t, psnap in sorted(preds_snaps.items()):
        t48 = closest(t + HORIZON_H * 3600)
        if t48 is None:
            continue
        fut = pools_by_key[t48]
        pv, rv = [], []
        for row in psnap.get("pools", []):
            f = fut.get((str(row.get("chain", "")).lower(),
                         str(row.get("pool", "")).lower()))
            if not f:
                continue
            apr, rew = row.get("stake_apr"), row.get("rewards48h")
            alloc, pred = row.get("allocation"), row.get("pred_apr")
            fees_f, alloc_f = f.get("fees48h"), f.get("allocation")
            if not all(isinstance(x, (int, float)) and x > 0
                       for x in (apr, rew, alloc, alloc_f)):
                continue
            if not isinstance(fees_f, (int, float)) \
                    or not isinstance(pred, (int, float)):
                continue
            inc = row.get("incentives48h") or 0.0
            realised = apr * ((fees_f + inc) / rew) * (alloc / alloc_f)
            pairs.append((pred, apr, realised, alloc))
            pv.append(pred)
            rv.append(realised)
        if len(pv) >= 30:
            spears.append(pearson(ranks(pv), ranks(rv)))
            used_origins += 1
    if not pairs:
        print("pred_eval: no scorable prediction pairs yet "
              f"({len(preds_snaps)} preds snapshots archived; each "
              "needs a pools snapshot 48h later)")
        return 0
    n = len(pairs)
    mae_p = sum(abs(p - r) for p, _, r, _ in pairs) / n
    mae_n = sum(abs(a - r) for _, a, r, _ in pairs) / n
    bias = sum(p - r for p, _, r, _ in pairs) / n
    dir_tot = sum(1 for p, a, _, _ in pairs if abs(p - a) >= 0.5)
    dir_ok = sum(1 for p, a, r, _ in pairs
                 if abs(p - a) >= 0.5 and (p - a) * (r - a) > 0)
    print(f"origins scored: {used_origins}   pairs: {n}")
    print(f"MAE  pred {mae_p:.2f}pp  vs naive {mae_n:.2f}pp  "
          f"-> skill {1 - mae_p / mae_n:+.1%}")
    print(f"bias {bias:+.2f}pp")
    print(f"directional (|delta| >= 0.5pp): {dir_ok}/{dir_tot} "
          f"= {dir_ok / max(dir_tot, 1):.1%}")
    if spears:
        print(f"Spearman rank corr (per-origin avg): "
              f"{sum(spears) / len(spears):.3f}")
    for name, grp in (("alloc >= 1M",
                       [x for x in pairs if x[3] >= 1_000_000]),
                      ("alloc < 1M",
                       [x for x in pairs if x[3] < 1_000_000])):
        if grp:
            mp = sum(abs(p - r) for p, _, r, _ in grp) / len(grp)
            mn = sum(abs(a - r) for _, a, r, _ in grp) / len(grp)
            print(f"{name}: n={len(grp)} MAE pred {mp:.2f}pp vs "
                  f"naive {mn:.2f}pp (skill {1 - mp / mn:+.1%})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
