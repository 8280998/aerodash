#!/usr/bin/env python3
"""Data quality gate for the hourly Aero dashboard pipeline.

Runs after the adapter and before deployment. Compares this run's new
feeds (dist/data/pools.json, dist/data/lp.json) against the previous
live feeds (fetched from aerodash.xyz at job start into prev/) and
against reader health metrics taken from the raw reader output.

Hard failures (any one => the run is DEGRADED: the previous feeds are
copied back over dist/data/ so the last good data stays live, the
deploy still happens, and status.json records deployed=false):
- a feed is missing, unparsable, or has an empty pools list;
- pools feed pool count < 70% of the previous feed's count (only
  enforced when the previous feed had >= 50 pools);
- the anchor pool AERO/USDC (Base, Basic Volatile) is missing from the
  new pools feed, or its stake_apr / allocation drifted > 50% versus
  the previous feed.

Soft warnings (deploy proceeds with the new data, deployed=true):
- gaugeStates read losses > 0 (counted from the raw records' errors:
  the reader appends "gaugeStates failed: ..." per affected pool);
- Coinbase priced-symbol coverage < 130 (raw coinbase_symbols_priced);
- LP feed pool count drifted > 30% versus the previous feed;
- any lines passed via --warnings-file (the workflow's archive step
  writes "history archive failed: ..." lines there).

Always writes status.json with the fixed page contract:
{generated_at, deployed, hard_failures, warnings, feed_pools,
 prev_feed_pools, lp_pools, gauge_losses, coinbase_symbols}
(missing values are null; field names/order are contractual).
feed_pools / lp_pools are THIS run's produced counts (before any
restore); prev_feed_pools is the live feed count fetched at job start.

Exit code is 0 on every handled path: the gate communicates through
status.json and the restored files, never by failing the job.

Run: python quality_gate.py --pools dist/data/pools.json
     --lp dist/data/lp.json --prev-dir prev --raw raw_pools.json
     --status-out dist/data/status.json [--warnings-file FILE]
"""
import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone

ANCHOR = {"pair": "AERO/USDC", "chain": "Base", "type": "Basic Volatile"}
SHRINK_RATIO = 0.70
SHRINK_MIN_PREV = 50
ANCHOR_DRIFT_MAX = 0.50
LP_DRIFT_MAX = 0.30
COINBASE_MIN = 130


def load_feed(path):
    """-> (doc, pools, error). pools is None when unparsable; an empty
    list is returned as-is (parsed but empty is a distinct failure)."""
    if not path or not os.path.exists(path):
        return None, None, "missing"
    try:
        with open(path) as f:
            doc = json.load(f)
    except Exception as e:
        return None, None, f"unparsable ({e.__class__.__name__})"
    if not isinstance(doc, dict) or not isinstance(doc.get("pools"), list):
        return None, None, "no pools list"
    return doc, doc["pools"], None


def find_anchor(pools):
    if not pools:
        return None
    for r in pools:
        if (r.get("pair") == ANCHOR["pair"]
                and r.get("chain") == ANCHOR["chain"]
                and r.get("type") == ANCHOR["type"]):
            return r
    return None


def rel_drift(new, old):
    """Relative change |new-old|/|old|; None when not computable."""
    if new is None or old is None or old == 0:
        return None
    return abs(new - old) / abs(old)


def raw_health(raw_path):
    """-> (gauge_losses, coinbase_symbols, error). Reads the raw reader
    output: gauge losses = records carrying a 'gaugeStates failed'
    error; coinbase_symbols = top-level coinbase_symbols_priced."""
    if not raw_path or not os.path.exists(raw_path):
        return None, None, "missing"
    try:
        with open(raw_path) as f:
            raw = json.load(f)
    except Exception as e:
        return None, None, f"unparsable ({e.__class__.__name__})"
    losses = 0
    for r in raw.get("pools", []):
        for e in r.get("errors") or []:
            if isinstance(e, str) and e.startswith("gaugeStates failed"):
                losses += 1
                break
    cs = raw.get("coinbase_symbols_priced")
    if isinstance(cs, list):
        cs = len(cs)
    if not isinstance(cs, int):
        cs = None
    return losses, cs, None


def restore(prev_path, dest_path, label, warnings):
    if prev_path and os.path.exists(prev_path):
        shutil.copyfile(prev_path, dest_path)
        warnings.append(f"restored previous {label} feed over the new one")
        return True
    warnings.append(f"no previous {label} feed available to restore")
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pools", required=True, help="new pools feed path")
    ap.add_argument("--lp", required=True, help="new lp feed path")
    ap.add_argument("--prev-dir", default="prev",
                    help="dir holding previous live pools.json / lp.json")
    ap.add_argument("--raw", default=None, help="raw reader output path")
    ap.add_argument("--status-out", required=True)
    ap.add_argument("--warnings-file", default=None,
                    help="optional file; each non-empty line is a warning")
    args = ap.parse_args()

    hard, warnings = [], []

    if args.warnings_file and os.path.exists(args.warnings_file):
        with open(args.warnings_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    warnings.append(line)

    new_doc, new_pools, perr = load_feed(args.pools)
    if perr:
        hard.append(f"pools feed {perr}")
    elif not new_pools:
        hard.append("pools feed empty")
    new_lp_doc, new_lp, lerr = load_feed(args.lp)
    if lerr:
        hard.append(f"lp feed {lerr}")
    elif not new_lp:
        hard.append("lp feed empty")

    prev_pools_path = os.path.join(args.prev_dir, "pools.json")
    prev_lp_path = os.path.join(args.prev_dir, "lp.json")
    _, prev_pools, _ = load_feed(prev_pools_path)
    _, prev_lp, _ = load_feed(prev_lp_path)

    feed_pools = len(new_pools) if new_pools is not None else None
    prev_feed_pools = len(prev_pools) if prev_pools is not None else None
    lp_pools = len(new_lp) if new_lp is not None else None
    prev_lp_pools = len(prev_lp) if prev_lp is not None else None

    if (feed_pools is not None and prev_feed_pools is not None
            and prev_feed_pools >= SHRINK_MIN_PREV
            and feed_pools < SHRINK_RATIO * prev_feed_pools):
        hard.append(f"pools feed shrank: {feed_pools} < 70% of "
                    f"previous {prev_feed_pools}")

    anchor_new = find_anchor(new_pools)
    if new_pools and anchor_new is None:
        hard.append("anchor pool AERO/USDC (Base, Basic Volatile) "
                    "missing from pools feed")
    anchor_prev = find_anchor(prev_pools)
    if anchor_new is not None and anchor_prev is not None:
        for field in ("stake_apr", "allocation"):
            d = rel_drift(anchor_new.get(field), anchor_prev.get(field))
            if d is not None and d > ANCHOR_DRIFT_MAX:
                hard.append(f"anchor {field} drifted {d:.0%} "
                            f"(> 50%) vs previous feed")

    gauge_losses, coinbase_symbols, rerr = raw_health(args.raw)
    if rerr:
        warnings.append(f"raw reader output {rerr}; gauge/coinbase "
                        f"checks skipped")
    if gauge_losses:
        warnings.append(f"gaugeStates read losses: {gauge_losses} pools")
    if coinbase_symbols is not None and coinbase_symbols < COINBASE_MIN:
        warnings.append(f"coinbase priced symbols low: "
                        f"{coinbase_symbols} (< {COINBASE_MIN})")
    if (lp_pools is not None and prev_lp_pools
            and abs(lp_pools - prev_lp_pools) / prev_lp_pools
            > LP_DRIFT_MAX):
        warnings.append(f"lp feed count drifted: {prev_lp_pools} -> "
                        f"{lp_pools} (> 30%)")

    deployed = not hard
    if hard:
        restore(prev_pools_path, args.pools, "pools", warnings)
        restore(prev_lp_path, args.lp, "lp", warnings)

    gen = None
    if isinstance(new_doc, dict):
        gen = new_doc.get("generated_at")
    if not gen:
        gen = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    status = {
        "generated_at": gen,
        "deployed": deployed,
        "hard_failures": hard,
        "warnings": warnings,
        "feed_pools": feed_pools,
        "prev_feed_pools": prev_feed_pools,
        "lp_pools": lp_pools,
        "gauge_losses": gauge_losses,
        "coinbase_symbols": coinbase_symbols,
    }
    out_dir = os.path.dirname(os.path.abspath(args.status_out))
    os.makedirs(out_dir, exist_ok=True)
    with open(args.status_out, "w") as f:
        json.dump(status, f, indent=1)
    print(json.dumps(status, indent=1))
    print(f"quality gate: deployed={deployed} "
          f"hard={len(hard)} warnings={len(warnings)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
