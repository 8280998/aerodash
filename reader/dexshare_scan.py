#!/usr/bin/env python3
"""DEX market-share hourly snapshot (archival only; the page does not
consume this file).

Fetches DefiLlama's public DEX overview once, groups protocols by
project with the SAME family rules as the dashboard's live DEX-share
table, and writes a compact snapshot: all-DEX 24h/7d totals plus the
top families by 24h volume (Aerodrome always included). The hourly
runner archives each snapshot to the data-history branch so a
self-owned, fixed-caliber share series accumulates over time.

Failure policy: any fetch/parse error exits non-zero WITHOUT writing
output; the runner treats this step as non-blocking.

Run: python dexshare_scan.py --out data/dexshare.json
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import requests

URL = ("https://api.llama.fi/overview/dexs"
       "?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true")
TOP_N = 12


def family(name):
    n = str(name or "").lower()
    if "aero" in n:
        return "Aerodrome"
    if n.startswith("uniswap"):
        return "Uniswap"
    if n.startswith("pancakeswap"):
        return "PancakeSwap"
    if n.startswith("raydium"):
        return "Raydium"
    if n.startswith("meteora"):
        return "Meteora"
    if n.startswith("curve"):
        return "Curve"
    if n.startswith("orca"):
        return "Orca"
    return name


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    data = None
    last_exc = None
    for attempt in range(2):
        try:
            r = requests.get(URL, timeout=60)
            r.raise_for_status()
            data = r.json()
            break
        except Exception as exc:  # noqa: BLE001 - retried once, then fatal
            last_exc = exc
    if data is None:
        print(f"dexshare fetch failed: {last_exc}", file=sys.stderr)
        return 1

    tot24 = 0.0
    tot7 = 0.0
    groups = {}
    for p in data.get("protocols") or []:
        v = p.get("total24h")
        if not isinstance(v, (int, float)) or not v > 0:
            continue
        v7 = p.get("total7d")
        has7 = isinstance(v7, (int, float)) and v7 > 0
        tot24 += v
        if has7:
            tot7 += v7
        g = groups.setdefault(family(p.get("name")),
                              {"name": family(p.get("name")),
                               "vol24h": 0.0, "vol7d": 0.0})
        g["vol24h"] += v
        if has7:
            g["vol7d"] += v7
    if not tot24 > 0 or "Aerodrome" not in groups:
        print("dexshare: empty totals or Aerodrome missing",
              file=sys.stderr)
        return 1

    fams = sorted(groups.values(), key=lambda g: -g["vol24h"])
    for i, g in enumerate(fams):
        g["rank"] = i + 1
        g["share24"] = round(g["vol24h"] / tot24 * 100, 4)
        g["share7"] = round(g["vol7d"] / tot7 * 100, 4) if tot7 > 0 else 0.0
        g["vol24h"] = round(g["vol24h"], 2)
        g["vol7d"] = round(g["vol7d"], 2)
    keep = fams[:TOP_N]
    if all(g["name"] != "Aerodrome" for g in keep):
        keep.append(next(g for g in fams if g["name"] == "Aerodrome"))

    out = {
        "generated_at": datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"),
        "total24h": round(tot24, 2),
        "total7d": round(tot7, 2),
        "families": keep,
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(out, f, indent=1)
    aero = next(g for g in fams if g["name"] == "Aerodrome")
    print(f"dexshare: total24h ${tot24:,.0f}; Aerodrome rank "
          f"{aero['rank']} share {aero['share24']:.2f}%; "
          f"{len(keep)} families written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
