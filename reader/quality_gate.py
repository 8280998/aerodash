#!/usr/bin/env python3
"""Data quality gate v2 for the hourly Aero dashboard pipeline.

Runs after the adapter and before deployment. Compares this run's new
feeds (dist/data/pools.json, dist/data/lp.json) against the previous
live feeds (fetched from aerodash.xyz at job start into prev/) and
against reader health metrics taken from the raw reader output.

v2 changes (after production run #6, 2026-10-25, exposed two v1
defects: a legitimate feed shrink caused by an adapter caliber change
-- dust rule sunk into the feed, absurd-fee cap 10x -> 2x -- was
misjudged as a read failure and rolled back; and a prev "lp feed" that
was actually the site's HTML fallback page for a missing path was
restored over a healthy new lp.json):

- Baseline validity: a previous feed only counts as a baseline when it
  parses as JSON, holds a pools list, and carries a generated_at.
  Anything else (missing file, HTML fallback page, truncated write)
  means NO baseline: it never participates in comparisons and is NEVER
  used for restore. pools and lp baselines are judged and restored
  independently.
- Shrink judgement is caliber-agnostic: the primary universe metric is
  the RAW reader output (pool_count + gauged count from chain_stats /
  records), compared against the previous run's raw counts, which the
  gate fetches best-effort from the live status.json (its own previous
  output). Raw universe shrink > 30% vs that baseline is a hard
  failure. The feed-count shrink alone is no longer a hard failure:
  > 50% shrink is hard only when accompanied by another health anomaly
  (gauge losses, anchor anomaly, low Coinbase coverage, LP drift);
  an isolated shrink with everything else green is a warning that
  names a suspected caliber/filter change, and the new feed deploys.

Hard failures (the run is DEGRADED; each affected feed is restored
from its own valid baseline when one exists -- the raw-universe
failure incriminates both feeds -- the deploy still happens, and
status.json records deployed=false):
- the new pools or lp feed is missing, unparsable, or empty;
- the anchor pool AERO/USDC (Base, Basic Volatile) is missing from the
  new pools feed, or its stake_apr / allocation drifted > 50% versus
  the previous (valid) pools baseline;
- raw pool universe shrank > 30% versus the previous run's raw count;
- pools feed shrank > 50% AND at least one companion health anomaly.

Soft warnings (deploy proceeds with the new data, deployed=true):
- isolated pools feed shrink > 20% with all other checks green
  (suspected caliber/filter change);
- gaugeStates read losses > 0 (counted from the raw records' errors:
  the reader appends "gaugeStates failed: ..." per affected pool);
- Coinbase priced-symbol coverage < 130 (raw coinbase_symbols_priced);
- LP feed pool count drifted > 30% versus the previous valid lp feed;
- a previous feed file exists but is not a valid baseline;
- any lines passed via --warnings-file (the workflow's archive step
  writes "history archive failed: ..." lines there).

Always writes status.json with the page contract:
{generated_at, deployed, hard_failures, warnings, feed_pools,
 prev_feed_pools, lp_pools, gauge_losses, coinbase_symbols,
 raw_pools, raw_gauged, prev_raw_pools}
(v2 only appends fields; missing values are null; existing field
names/order are contractual). feed_pools / lp_pools are THIS run's
produced counts (before any restore); prev_feed_pools is the live
feed count fetched at job start (null when no valid baseline).

Exit code is 0 on every handled path: the gate communicates through
status.json and the restored files, never by failing the job.

Run: python quality_gate.py --pools dist/data/pools.json
     --lp dist/data/lp.json --prev-dir prev --raw raw_pools.json
     --status-out dist/data/status.json [--warnings-file FILE]
     [--prev-status-url URL] [--prev-status-file FILE]
"""
import argparse
import json
import os
import shutil
import sys
import urllib.request
from datetime import datetime, timezone

ANCHOR = {"pair": "AERO/USDC", "chain": "Base", "type": "Basic Volatile"}
ANCHOR_DRIFT_MAX = 0.50
LP_DRIFT_MAX = 0.30
COINBASE_MIN = 130
SHRINK_MIN_PREV = 50        # feed-shrink comparisons need a real baseline
SHRINK_WARN_RATIO = 0.80    # feed < 80% of prev -> shrink warning
SHRINK_HARD_RATIO = 0.50    # feed < 50% of prev + anomaly -> hard failure
RAW_SHRINK_MAX = 0.30       # raw universe shrink > 30% -> hard failure
DEFAULT_PREV_STATUS_URL = "https://aerodash.xyz/data/status.json"


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


def load_baseline(path):
    """Previous-feed baseline: stricter than load_feed. A file only
    counts as a baseline when it parses, holds a pools list, AND
    carries a non-empty generated_at. -> (pools, error); pools is None
    when the file is missing or not a valid baseline."""
    if not path or not os.path.exists(path):
        return None, "missing"
    doc, pools, err = load_feed(path)
    if err:
        return None, err
    gen = doc.get("generated_at")
    if not isinstance(gen, str) or not gen:
        return None, "no generated_at"
    return pools, None


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
    """-> (gauge_losses, coinbase_symbols, raw_pools, raw_gauged,
    error). Reads the raw reader output: gauge losses = records
    carrying a 'gaugeStates failed' error; coinbase_symbols =
    top-level coinbase_symbols_priced; raw_pools = top-level
    pool_count (fallback: record count / chain_stats pools_found sum);
    raw_gauged = records with a gauge, from chain_stats / records."""
    if not raw_path or not os.path.exists(raw_path):
        return None, None, None, None, "missing"
    try:
        with open(raw_path) as f:
            raw = json.load(f)
    except Exception as e:
        return None, None, None, None, f"unparsable ({e.__class__.__name__})"
    records = raw.get("pools")
    losses = 0
    gauged = 0
    if isinstance(records, list):
        for r in records:
            if r.get("gauge"):
                gauged += 1
            for e in r.get("errors") or []:
                if isinstance(e, str) and e.startswith("gaugeStates failed"):
                    losses += 1
                    break
    else:
        records = None
    cs = raw.get("coinbase_symbols_priced")
    if isinstance(cs, list):
        cs = len(cs)
    if not isinstance(cs, int):
        cs = None
    raw_pools = raw.get("pool_count")
    if not isinstance(raw_pools, int):
        if records is not None:
            raw_pools = len(records)
        else:
            stats = raw.get("chain_stats") or {}
            total = sum(v.get("pools_found", 0) for v in stats.values()
                        if isinstance(v, dict))
            raw_pools = total or None
    if records is None:
        gauged = None
    return losses, cs, raw_pools, gauged, None


def fetch_prev_raw_pools(url, local_file):
    """Previous run's raw pool count, best-effort: from a local status
    file when given (tests / debugging), else by fetching the live
    status.json the gate itself wrote last run. -> int or None."""
    doc = None
    if local_file:
        try:
            with open(local_file) as f:
                doc = json.load(f)
        except Exception:
            return None
    elif url:
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "aerodash-quality-gate/2"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                doc = json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None
    if isinstance(doc, dict):
        v = doc.get("raw_pools")
        if isinstance(v, int) and v > 0:
            return v
    return None


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
    ap.add_argument("--prev-status-url", default=DEFAULT_PREV_STATUS_URL,
                    help="live status.json URL for the raw baseline")
    ap.add_argument("--prev-status-file", default=None,
                    help="read the previous status from this local file "
                         "instead of the URL (tests / debugging)")
    args = ap.parse_args()

    hard, warnings = [], []
    # Per-feed failure attribution drives independent restore.
    pools_failed = False
    lp_failed = False

    if args.warnings_file and os.path.exists(args.warnings_file):
        with open(args.warnings_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    warnings.append(line)

    # --- new feeds: parse/empty stays a hard failure, per feed ---
    new_doc, new_pools, perr = load_feed(args.pools)
    if perr:
        hard.append(f"pools feed {perr}")
        pools_failed = True
    elif not new_pools:
        hard.append("pools feed empty")
        pools_failed = True
    new_lp_doc, new_lp, lerr = load_feed(args.lp)
    if lerr:
        hard.append(f"lp feed {lerr}")
        lp_failed = True
    elif not new_lp:
        hard.append("lp feed empty")
        lp_failed = True

    # --- previous feeds: valid-baseline gate (v2 core fix) ---
    prev_pools_path = os.path.join(args.prev_dir, "pools.json")
    prev_lp_path = os.path.join(args.prev_dir, "lp.json")
    prev_pools, prev_pools_err = load_baseline(prev_pools_path)
    prev_lp, prev_lp_err = load_baseline(prev_lp_path)
    if prev_pools is None and prev_pools_err != "missing":
        warnings.append(f"previous pools feed is not a valid baseline "
                        f"({prev_pools_err}); baseline checks and restore "
                        f"for pools disabled this run")
    if prev_lp is None and prev_lp_err != "missing":
        warnings.append(f"previous lp feed is not a valid baseline "
                        f"({prev_lp_err}); baseline checks and restore "
                        f"for lp disabled this run")

    feed_pools = len(new_pools) if new_pools is not None else None
    prev_feed_pools = len(prev_pools) if prev_pools is not None else None
    lp_pools = len(new_lp) if new_lp is not None else None
    prev_lp_pools = len(prev_lp) if prev_lp is not None else None

    # --- raw reader health + universe ---
    (gauge_losses, coinbase_symbols, raw_pools, raw_gauged,
     rerr) = raw_health(args.raw)
    if rerr:
        warnings.append(f"raw reader output {rerr}; gauge/coinbase/raw "
                        f"checks skipped")
    if gauge_losses:
        warnings.append(f"gaugeStates read losses: {gauge_losses} pools")
    coinbase_low = (coinbase_symbols is not None
                    and coinbase_symbols < COINBASE_MIN)
    if coinbase_low:
        warnings.append(f"coinbase priced symbols low: "
                        f"{coinbase_symbols} (< {COINBASE_MIN})")

    prev_raw_pools = fetch_prev_raw_pools(args.prev_status_url,
                                          args.prev_status_file)
    raw_failed = False
    if (raw_pools is not None and prev_raw_pools
            and raw_pools < (1 - RAW_SHRINK_MAX) * prev_raw_pools):
        hard.append(f"raw pool universe shrank: {raw_pools} < 70% of "
                    f"previous raw {prev_raw_pools} "
                    f"({(1 - raw_pools / prev_raw_pools):.0%} drop)")
        raw_failed = True

    # --- anchor: missing / drift stay hard failures (pools feed) ---
    anchor_new = find_anchor(new_pools)
    anchor_anomaly = False
    if new_pools and anchor_new is None:
        hard.append("anchor pool AERO/USDC (Base, Basic Volatile) "
                    "missing from pools feed")
        pools_failed = True
        anchor_anomaly = True
    anchor_prev = find_anchor(prev_pools)
    if anchor_new is not None and anchor_prev is not None:
        for field in ("stake_apr", "allocation"):
            d = rel_drift(anchor_new.get(field), anchor_prev.get(field))
            if d is not None and d > ANCHOR_DRIFT_MAX:
                hard.append(f"anchor {field} drifted {d:.0%} "
                            f"(> 50%) vs previous feed")
                pools_failed = True
                anchor_anomaly = True

    # --- LP drift vs its own valid baseline (warning + anomaly input) ---
    lp_drift_bad = False
    if (lp_pools is not None and prev_lp_pools
            and abs(lp_pools - prev_lp_pools) / prev_lp_pools
            > LP_DRIFT_MAX):
        lp_drift_bad = True
        warnings.append(f"lp feed count drifted: {prev_lp_pools} -> "
                        f"{lp_pools} (> 30%)")

    # --- feed shrink: caliber-agnostic judgement (v2 core fix) ---
    if (feed_pools is not None and prev_feed_pools is not None
            and prev_feed_pools >= SHRINK_MIN_PREV):
        ratio = feed_pools / prev_feed_pools
        if ratio < SHRINK_WARN_RATIO:
            anomalies = []
            if gauge_losses:
                anomalies.append(f"gauge losses {gauge_losses}")
            if anchor_anomaly:
                anomalies.append("anchor anomaly")
            if coinbase_low:
                anomalies.append(f"coinbase symbols "
                                 f"{coinbase_symbols} < {COINBASE_MIN}")
            if lp_drift_bad:
                anomalies.append("lp count drift > 30%")
            if ratio < SHRINK_HARD_RATIO and anomalies:
                hard.append(f"pools feed shrank: {feed_pools} < 50% of "
                            f"previous {prev_feed_pools}, with health "
                            f"anomalies: {'; '.join(anomalies)}")
                pools_failed = True
            else:
                warnings.append(
                    f"pools feed shrank: {prev_feed_pools} -> "
                    f"{feed_pools} ({(1 - ratio):.0%} drop) but raw "
                    f"universe and all other health checks are green; "
                    f"deploying the new feed -- suspected caliber/"
                    f"filter change, not a read failure")

    if raw_failed:
        # The collapsed read produced both feeds; both are suspect.
        pools_failed = True
        lp_failed = True

    # --- restore: per feed, only from a VALID baseline ---
    deployed = not hard
    if pools_failed:
        if prev_pools is not None:
            shutil.copyfile(prev_pools_path, args.pools)
            warnings.append("restored previous pools feed over the "
                            "new one")
        else:
            warnings.append("no valid previous pools feed available "
                            "to restore; new pools feed kept as-is")
    if lp_failed:
        if prev_lp is not None:
            shutil.copyfile(prev_lp_path, args.lp)
            warnings.append("restored previous lp feed over the new one")
        else:
            warnings.append("no valid previous lp feed available to "
                            "restore; new lp feed kept as-is")

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
        "raw_pools": raw_pools,
        "raw_gauged": raw_gauged,
        "prev_raw_pools": prev_raw_pools,
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
