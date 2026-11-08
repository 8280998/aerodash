#!/usr/bin/env python3
"""Predicted voting APR for the AERO dashboard wall feed.

The wall's Stake APR is backward-looking: its fee component is the
trailing 48h of realised fees, and its denominator is the CURRENT
allocation, while the rewards a voter actually receives accrue over
the coming period against the allocation then in place. This step
adds a `pred_apr` field to each row of the page pools feed:

    pred_apr = stake_apr x (pred_rewards / rewards48h) / (1 + adj)

- pred_rewards = pred_fees48h + incentives48h. The incentives part of
  the feed is already forward-looking (program rate x overlap with
  the next 48h), so it is carried over unchanged.
- pred_fees48h is an exponentially weighted average of the pool's
  fees48h series over the trailing WINDOW_H hours of hourly archive
  snapshots (half-life HALF_LIFE_H), which strips one-off volume
  spikes that a single 48h window cannot.
- adj is the allocation-trend correction: half of the pool's 24h
  allocation change, clamped to +/-25%. Inflows dilute the APR a
  voter will actually see; outflows raise it.

Pools with too little history (< MIN_SAMPLES samples or a span under
MIN_SPAN_H) get NO pred_apr key; the page then shows the plain APR.
Autopilot / whale reallocations are not predictable and are NOT
modelled beyond the 24h trend term.

The step also scores the forecast made one horizon ago: for each row
it looks up the preds-archive snapshot closest to 48h old and adds
`pred_dev` = (current stake_apr / that snapshot's pred_apr - 1) x 100
-- how far reality landed from the prediction, in percent (positive =
reality beat the forecast). Rows without a scorable past prediction
get NO pred_dev key.

Run: python predict.py --feed data/pools.json --history-dir history/
(the history dir uses the data-history layout history/YYYY/MM/DD/).
The feed file is rewritten in place.
"""
import argparse
import glob
import gzip
import json
import os
import re
import sys
from datetime import datetime, timezone

WINDOW_H = 192.0        # trailing window for the fee EWMA (8 days)
HALF_LIFE_H = 48.0      # EWMA half-life
MIN_SAMPLES = 12        # hourly samples required for a prediction
MIN_SPAN_H = 36.0       # minimum coverage span of those samples
TREND_MIN_AGE_H = 18.0  # 24h-trend reference sample age window
TREND_MAX_AGE_H = 36.0
TREND_FACTOR = 0.5      # extrapolate half the 24h allocation move
TREND_CAP = 0.25        # ... clamped to +/-25%
SHRINK_K_USD = 500.0    # fee-scale shrinkage: the prediction delta is
                        # weighted by pred_fees / (pred_fees + K), so
                        # dust-fee pools (whose EWMA is noise) barely
                        # move while real pools take the full signal

_NAME_RE = re.compile(r"(?:pools|preds)-(\d{2})(\d{2})\.json\.gz$")

DEV_TARGET_AGE_H = 48.0   # a prediction is scored against the feed
DEV_MIN_AGE_H = 36.0      # generated one forecast horizon (48h) later;
DEV_MAX_AGE_H = 60.0      # accept the preds snapshot closest to 48h old
_DATE_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})$")


def snapshot_ts(path):
    """UTC epoch from the history path layout YYYY/MM/DD/pools-HHMM."""
    m = _NAME_RE.search(os.path.basename(path))
    d = _DATE_RE.search(os.path.dirname(path).replace(os.sep, "/"))
    if not m or not d:
        return None
    try:
        dt = datetime(int(d.group(1)), int(d.group(2)), int(d.group(3)),
                      int(m.group(1)), int(m.group(2)),
                      tzinfo=timezone.utc)
    except ValueError:
        return None
    return dt.timestamp()


def parse_iso(ts):
    try:
        return datetime.strptime(
            ts, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc).timestamp()
    except (TypeError, ValueError):
        return None


def collect(history_dir, now, window_h):
    """{(chain_lower, pool_lower): [(ts, fees48h, allocation), ...]}
    from every snapshot inside the trailing window, oldest first."""
    series = {}
    if not history_dir or not os.path.isdir(history_dir):
        return series
    cutoff = now - window_h * 3600
    paths = []
    for path in glob.glob(os.path.join(
            history_dir, "*", "*", "*", "pools-*.json.gz")):
        ts = snapshot_ts(path)
        if ts is not None and cutoff <= ts <= now + 3600:
            paths.append((ts, path))
    for ts, path in sorted(paths):
        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                snap = json.load(f)
        except Exception:
            continue
        snap_ts = parse_iso(snap.get("generated_at")) or ts
        for row in snap.get("pools", []):
            pool = row.get("pool")
            chain = row.get("chain")
            fees = row.get("fees48h")
            alloc = row.get("allocation")
            if not pool or not chain:
                continue
            if not isinstance(fees, (int, float)) \
                    or not isinstance(alloc, (int, float)):
                continue
            key = (str(chain).lower(), str(pool).lower())
            series.setdefault(key, []).append((snap_ts, fees, alloc))
    return series


def collect_past_preds(history_dir, now):
    """{(chain_lower, pool_lower): pred_apr} from the preds-archive
    snapshot closest to DEV_TARGET_AGE_H old (within the accepted age
    band); empty when no snapshot qualifies."""
    if not history_dir or not os.path.isdir(history_dir):
        return {}
    best = None
    for path in glob.glob(os.path.join(
            history_dir, "*", "*", "*", "preds-*.json.gz")):
        ts = snapshot_ts(path)
        if ts is None:
            continue
        age_h = (now - ts) / 3600.0
        if DEV_MIN_AGE_H <= age_h <= DEV_MAX_AGE_H:
            if best is None or abs(age_h - DEV_TARGET_AGE_H) < abs(
                    (now - best[0]) / 3600.0 - DEV_TARGET_AGE_H):
                best = (ts, path)
    if best is None:
        return {}
    try:
        with gzip.open(best[1], "rt", encoding="utf-8") as f:
            snap = json.load(f)
    except Exception:
        return {}
    out = {}
    for row in snap.get("pools", []):
        pool, chain, pred = row.get("pool"), row.get("chain"), \
            row.get("pred_apr")
        if pool and chain and isinstance(pred, (int, float)) and pred > 0:
            out[(str(chain).lower(), str(pool).lower())] = pred
    return out


def predict_row(row, samples, now):
    """pred_apr for one feed row, or None when history is too thin."""
    apr = row.get("stake_apr")
    rewards = row.get("rewards48h")
    alloc_now = row.get("allocation")
    if not isinstance(apr, (int, float)) or apr <= 0:
        return None
    if not isinstance(rewards, (int, float)) or rewards <= 0:
        return None
    if not isinstance(alloc_now, (int, float)) or alloc_now <= 0:
        return None
    samples = samples + [(now, row.get("fees48h"), alloc_now)]
    samples = [s for s in samples
               if isinstance(s[1], (int, float))
               and now - s[0] <= WINDOW_H * 3600]
    if len(samples) < MIN_SAMPLES:
        return None
    span_h = (now - min(s[0] for s in samples)) / 3600.0
    if span_h < MIN_SPAN_H:
        return None
    wsum = 0.0
    fsum = 0.0
    for ts, fees, _alloc in samples:
        w = 0.5 ** (((now - ts) / 3600.0) / HALF_LIFE_H)
        wsum += w
        fsum += w * fees
    if wsum <= 0:
        return None
    pred_fees = fsum / wsum
    incentives = row.get("incentives48h")
    if not isinstance(incentives, (int, float)):
        incentives = 0.0
    pred_rewards = pred_fees + incentives
    # Allocation trend: reference sample closest to 24h old.
    ref = None
    for ts, _fees, alloc in samples:
        age_h = (now - ts) / 3600.0
        if TREND_MIN_AGE_H <= age_h <= TREND_MAX_AGE_H \
                and isinstance(alloc, (int, float)) and alloc > 0:
            if ref is None or abs(age_h - 24.0) < abs(
                    (now - ref[0]) / 3600.0 - 24.0):
                ref = (ts, alloc)
    adj = 0.0
    if ref is not None:
        pct24 = (alloc_now - ref[1]) / ref[1]
        adj = max(-TREND_CAP, min(TREND_CAP, TREND_FACTOR * pct24))
    raw = apr * (pred_rewards / rewards) / (1.0 + adj)
    w = pred_fees / (pred_fees + SHRINK_K_USD) if pred_fees > 0 else 0.0
    return round(apr + (raw - apr) * w, 2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--feed", required=True)
    ap.add_argument("--history-dir", required=True)
    ap.add_argument("--preds-out",
                    help="also write the predictions archive record "
                         "(this round's pred_apr values plus the "
                         "inputs needed to score them at T+48h)")
    args = ap.parse_args()
    with open(args.feed, encoding="utf-8") as f:
        feed = json.load(f)
    now = parse_iso(feed.get("generated_at"))
    if now is None:
        print("predict: feed has no parseable generated_at",
              file=sys.stderr)
        return 1
    series = collect(args.history_dir, now, WINDOW_H)
    past_preds = collect_past_preds(args.history_dir, now)
    n_pred = 0
    n_dev = 0
    preds = []
    for row in feed.get("pools", []):
        key = (str(row.get("chain", "")).lower(),
               str(row.get("pool", "")).lower())
        past = past_preds.get(key)
        apr_now = row.get("stake_apr")
        if past is not None and isinstance(apr_now, (int, float)):
            row["pred_dev"] = round((apr_now / past - 1.0) * 100.0, 1)
            n_dev += 1
        pred = predict_row(row, series.get(key, []), now)
        if pred is not None:
            row["pred_apr"] = pred
            n_pred += 1
            preds.append({
                "chain": row.get("chain"), "pool": row.get("pool"),
                "pair": row.get("pair"), "stake_apr": row.get("stake_apr"),
                "pred_apr": pred, "rewards48h": row.get("rewards48h"),
                "incentives48h": row.get("incentives48h"),
                "allocation": row.get("allocation")})
    with open(args.feed, "w", encoding="utf-8") as f:
        json.dump(feed, f, indent=1)
    if args.preds_out:
        with open(args.preds_out, "w", encoding="utf-8") as f:
            json.dump({"generated_at": feed.get("generated_at"),
                       "pools": preds}, f, indent=1)
    print(f"predict: {n_pred}/{len(feed.get('pools', []))} pools "
          f"have pred_apr ({len(series)} pools in history window); "
          f"{n_dev} rows scored with pred_dev")
    return 0


if __name__ == "__main__":
    sys.exit(main())
