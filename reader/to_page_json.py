#!/usr/bin/env python3
"""Convert the reader's raw pools.json into the dashboard's page format.

Input : reader.py output {"generated_at": epoch, "aero_usd": float,
        "pools": [raw records]}.
Output: {"generated_at": ISO-8601 UTC string, "source": "onchain",
        "aero_price_usd": float, "pools": [page rows]} where each row is
        {"pair", "fee", "chain", "type", "fees48h", "incentives48h",
         "rewards48h", "stake_apr", "allocation"} — exactly the shape the
        dashboard's sanitizeLive() accepts (deploy/dist/index.html).

Field mapping:
- fees48h     = fees48h_usd + mev48h_usd. The official UI's Fees 48H
                includes MEV fees (probe: fees+MEV matched UI at 1.003,
                fees alone did not), and rewards48h = fees48h +
                incentives48h then matches the reader's APR numerator.
- incentives48h = incentives48h_usd (live-programs-only, rate * full
                48h window; see reader.py read_incentives).
- stake_apr   = stake_apr_pct; allocation = allocation (Allocation
                Power units, 1e18-scaled value as a plain number).
- chain       = display name (base -> Base, optimism -> OP Mainnet, ...).
- fee         = fmt_fee(fee_pct): the pool's live swap fee read
                on-chain by reader.py (CL: pool.fee() in pips; Basic:
                factory getBaseFee in 0.01% units) and stored in the
                raw record as fee_pct, a percent number. Formatted like
                the snapshot conventions ("0.03%", "0.3%", "1%",
                "<0.01%" when 0 < fee_pct < 0.01). Records from older
                reader runs have no fee_pct; they emit "" (the page
                tolerates an empty fee string).

Trust filter (a pool is emitted only if ALL hold):
- pair present; allocation > 0; stake_apr present (the reader only
  computes APR when fees AND incentives were fully priced);
- BOTH tokens priced via Coinbase: price_via.token0 == "coinbase" and
  price_via.token1 == "coinbase". Single-pool on-chain spot prices are
  off by orders of magnitude for illiquid/manipulated exotics, so any
  record touching an on-chain-propagated price is excluded from the
  wall feed (it stays in the raw pools.json for analysis).
Sanity drop: fees48h > 10 * allocation * aero_price_usd (a pool whose
48h fees exceed 10x the USD value of its total allocation is a pricing
artefact, not data).

Run: python to_page_json.py [--in pools.json] [--out page_pools.json]
Prints a summary: input count, output count, drops by reason.
"""
import argparse
import json
import os
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
CHAIN_NAMES = {
    "base": "Base",
    "optimism": "OP Mainnet",
    "arbitrum": "Arbitrum",
    "ethereum": "Ethereum",
    "ink": "Ink",
    "arc": "Arc",
    "robinhood": "Robinhood",
}


def fmt_fee(pct):
    """Percent number -> page fee string. None (unread/old data) -> "".
    0 < pct < 0.01 -> "<0.01%" (the UI's convention for sub-0.01% fees);
    otherwise up to 2 decimals with trailing zeros stripped."""
    if pct is None:
        return ""
    if pct == 0:
        return "0%"
    if pct < 0.01:
        return "<0.01%"
    return f"{pct:.2f}".rstrip("0").rstrip(".") + "%"


def convert(doc):
    aero = doc.get("aero_usd")
    drops = {}
    out = []

    def drop(reason):
        drops[reason] = drops.get(reason, 0) + 1

    for r in doc.get("pools", []):
        if not r.get("pair"):
            drop("no_pair")
            continue
        alloc = r.get("allocation")
        if not alloc or alloc <= 0:
            drop("no_allocation")
            continue
        if r.get("stake_apr_pct") is None:
            drop("no_apr")
            continue
        pv = r.get("price_via") or {}
        if pv.get("token0") != "coinbase" or pv.get("token1") != "coinbase":
            drop("untrusted_price")
            continue
        if r.get("fees48h_usd") is None:
            drop("no_fees")
            continue
        fees = r["fees48h_usd"] + (r.get("mev48h_usd") or 0.0)
        inc = r.get("incentives48h_usd")
        if inc is None:
            # Unreachable in practice (APR requires priced incentives),
            # but keep the page contract: null means "not included".
            rewards = fees
        else:
            rewards = fees + inc
        if aero and fees > 10 * alloc * aero:
            drop("absurd_fees")
            continue
        out.append({
            "pair": r["pair"],
            "fee": fmt_fee(r.get("fee_pct")),
            "chain": CHAIN_NAMES.get(r["chain"], r["chain"]),
            "type": r.get("type") or "",
            "fees48h": round(fees, 2),
            "incentives48h": (round(inc, 2) if inc is not None else None),
            "rewards48h": round(rewards, 2),
            "stake_apr": round(r["stake_apr_pct"], 2),
            "allocation": round(alloc, 2),
        })
    out.sort(key=lambda p: p["allocation"], reverse=True)
    ts = doc.get("generated_at")
    gen = (datetime.fromtimestamp(ts, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ") if ts else "")
    page = {"generated_at": gen, "source": "onchain",
            "aero_price_usd": aero, "pools": out}
    return page, drops


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp",
                    default=os.path.join(HERE, "pools.json"))
    ap.add_argument("--out", dest="outp",
                    default=os.path.join(HERE, "page_pools.json"))
    args = ap.parse_args()
    with open(args.inp) as f:
        doc = json.load(f)
    page, drops = convert(doc)
    with open(args.outp, "w") as f:
        json.dump(page, f, indent=1)
    print(f"input {len(doc.get('pools', []))} -> output "
          f"{len(page['pools'])} pools; drops: {drops}")
    print(f"wrote {args.outp}")


if __name__ == "__main__":
    main()
