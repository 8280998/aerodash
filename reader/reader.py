#!/usr/bin/env python3
"""Aero new-system on-chain data reader v1 (read-only, public RPC).

Enumerates gauged pools per chain from FactoryRegistry, reads 48h fees/MEV
from PoolTape/CLPoolTape, per-pool Allocation from LeafVoter gauge points,
and per-gauge incentive program streams from VotingRewardsManager.
Incentives口径: incentives48h counts ONLY programs still live at read
time (end > now), each at rate * full 48h window (not pro-rated by
overlap), matching the official UI (proven 2026-10-24, ratios
0.998-1.000 on 6 Base pools); the old gross trailing-48h stream is
kept separately as incentives48h_gross_usd.
Prices: Coinbase spot API for majors + on-chain spot propagation.
NO DefiLlama anywhere. eth_call only; no keys, no signing.

Run: ~/workspace/hbtc-miner/venv/bin/python reader.py [--chains base,...]
Output: pools.json (+ token_cache.json for metadata caching).
"""
import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error

from eth_abi import encode as abi_encode, decode as abi_decode
from web3 import Web3

HERE = os.path.dirname(os.path.abspath(__file__))
WINDOW = 172800  # 48h in seconds
ANNUALIZE = 365.25 * 24 * 3600 / WINDOW  # 182.625
MULTICALL3 = "0xcA11bde05977b3631167028862bF2a173976CA11"
POOL_TAPE = "0x34bdD64B19D8631F936c2D07F19AAa6B83515992"
CL_POOL_TAPE = "0x27D5EB73e7406675F6B0CbD037490ec3dba935a4"
LEAF_VOTER = "0x382409fad8503fbcdbb27a4211551fa03e884078"
STABLE_F = "0x78589E876edcEbdce72ee3436ab3c9AD3a7cEF2D"
VOLATILE_F = "0x8E6a9FCd1d638CC760Ffba397605Fe3355dE76D2"
ZERO = "0x" + "00" * 20
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
      "Content-Type": "application/json"}

CHAINS = {
    "base": {"chain_id": 8453, "registry": "0x7b2A2c12dC5Ea2a31455952a05031C3E4BB28F3E",
             "cl_factory": "0x9CB220581b91b0C83223012FE69688b92aD07a76", "chunk": 50,
             "rpcs": ["https://base-rpc.publicnode.com", "https://mainnet.base.org",
                      "https://base.drpc.org", "https://1rpc.io/base",
                      "https://base-mainnet.public.blastapi.io"]},
    "optimism": {"chain_id": 10, "registry": "0xb6F9705A631CC1C0E8570FFde48882C95E2Df408",
                 "cl_factory": "0xfeaB42D90055db3a0616249f76B6002D72266778",
                 "rpcs": ["https://mainnet.optimism.io", "https://optimism-rpc.publicnode.com",
                          "https://optimism.drpc.org", "https://1rpc.io/op"]},
    "arbitrum": {"chain_id": 42161, "registry": "0x9E635b2c689fC52201DF7eF1C9FA607c2F58f666",
                 "cl_factory": "0x88a60ee77392577c355C0acb2A8d476D5a4EC03a",
                 "rpcs": ["https://arb1.arbitrum.io/rpc", "https://arbitrum-one-rpc.publicnode.com",
                          "https://arbitrum.drpc.org", "https://1rpc.io/arb"]},
    "ethereum": {"chain_id": 1, "registry": "0xdd1705e23762F4D6e44D8C31BD8Ea80c54612239",
                 "cl_factory": "0xF4Fd2E6c5B60c5b365cd1B96DD6245E6D60414c2",
                 "rpcs": ["https://ethereum-rpc.publicnode.com", "https://eth.drpc.org",
                          "https://1rpc.io/eth"]},
    "ink": {"chain_id": 57073, "registry": "0xdd1705e23762F4D6e44D8C31BD8Ea80c54612239",
            "cl_factory": "0x7660BF2aDefC687C22906112ca323d60A00358cF",
            "rpcs": ["https://rpc-gel.inkonchain.com", "https://rpc-qnd.inkonchain.com",
                     "https://ink.drpc.org"]},
    "arc": {"chain_id": 5042, "registry": "0xCe44907DC45D3BFa3f5f6035B0e6E8B22D780A42",
            "cl_factory": "0x7F83235643181E440433400a027618Cd50879751",
            "rpcs": ["https://rpc.mainnet.arc.io"], "chunk": 25},
    "robinhood": {"chain_id": 4663, "registry": "0xdd1705e23762F4D6e44D8C31BD8Ea80c54612239",
                  "cl_factory": "0xc63d7338c178e805c544B8f897066bD3BBcBE136",
                  "rpcs": ["https://robinhood.drpc.org"], "chunk": 50},
}

# Coinbase spot aliases: on-chain symbol -> Coinbase asset code
CB_ALIAS = {"WETH": "ETH", "WBTC": "BTC", "cbBTC": "BTC", "BTC": "BTC",
            "WETH.e": "ETH", "ETH": "ETH"}
# Symbols treated as USD-pegged only if Coinbase confirms; no hardcoding here.


def sel(sig):
    return Web3.keccak(text=sig)[:4]


def enc_call(sig, types, args):
    return sel(sig) + (abi_encode(types, args) if types else b"")


def dec(types, data):
    return abi_decode(types, data)


class RPCError(Exception):
    pass


class RPC:
    """JSON-RPC client with endpoint rotation, retries, browser UA."""

    def __init__(self, endpoints, chain_id):
        self.endpoints = endpoints
        self.chain_id = chain_id
        self.idx = 0
        self.calls = 0
        self.errors = 0

    _session = None

    def _once(self, url, method, params):
        import requests
        if RPC._session is None:
            RPC._session = requests.Session()
            RPC._session.headers.update(UA)
        resp = RPC._session.post(
            url, json={"jsonrpc": "2.0", "id": 1, "method": method,
                       "params": params}, timeout=30)
        resp.raise_for_status()
        out = resp.json()
        if "error" in out:
            raise RPCError(str(out["error"])[:200])
        return out["result"]

    def call(self, method, params, attempts=6):
        last = None
        for i in range(attempts):
            url = self.endpoints[self.idx % len(self.endpoints)]
            try:
                res = self._once(url, method, params)
                self.calls += 1
                return res
            except Exception as e:  # rotate + backoff
                self.errors += 1
                last = e
                self.idx += 1
                time.sleep(min(1.5 * (i + 1), 6))
        raise RPCError(f"{method} failed on all endpoints: {last}")

    def eth_call(self, to, data_hex, block="latest"):
        return self.call("eth_call", [{"to": to, "data": data_hex}, block])

    def eth_call_once(self, to, data_hex):
        """Single attempt on the current endpoint, no retry sleeps."""
        url = self.endpoints[self.idx % len(self.endpoints)]
        return self._once(url, "eth_call",
                          [{"to": to, "data": data_hex}, "latest"])

    def batch_eth_call(self, items, attempts=5):
        """items: list of (to, data_hex). One HTTP POST with a JSON-RPC
        batch array. Returns list of (result_hex_or_None, error_or_None).
        Rotates endpoints on transport failure."""
        import requests
        if RPC._session is None:
            RPC._session = requests.Session()
            RPC._session.headers.update(UA)
        payload = [{"jsonrpc": "2.0", "id": i, "method": "eth_call",
                    "params": [{"to": to, "data": data}, "latest"]}
                   for i, (to, data) in enumerate(items)]
        last = None
        for i in range(attempts):
            url = self.endpoints[self.idx % len(self.endpoints)]
            try:
                resp = RPC._session.post(url, json=payload, timeout=60)
                resp.raise_for_status()
                out = resp.json()
                if isinstance(out, dict) and "error" in out:
                    raise RPCError(str(out["error"])[:200])
                by_id = {o["id"]: o for o in out}
                res = []
                for j in range(len(items)):
                    o = by_id.get(j, {})
                    if "error" in o:
                        res.append((None, str(o["error"])[:160]))
                    else:
                        res.append((o.get("result"), None))
                self.calls += len(items)
                return res
            except Exception as e:
                self.errors += 1
                last = e
                self.idx += 1
                time.sleep(min(1.5 * (i + 1), 6))
        raise RPCError(f"batch failed on all endpoints: {last}")

    def block_number(self):
        return int(self.call("eth_blockNumber", []), 16)

    def block_timestamp(self, n):
        b = self.call("eth_getBlockByNumber", [hex(n), False])
        return int(b["timestamp"], 16)

    def get_code(self, addr):
        return self.call("eth_getCode", [addr, "latest"])


class Call:
    """One logical call: target, signature, arg types/args, out types."""

    def __init__(self, to, sig, in_types, args, out_types):
        self.to = Web3.to_checksum_address(to)
        self.sig = sig
        self.data = enc_call(sig, in_types, args)
        self.out_types = out_types

    def decode(self, raw):
        return dec(self.out_types, raw)


TRANSIENT = ("rate limit", "limit exceeded", "timeout", "timed out",
             "-32005", "429", "too many", "-32603", "over capacity",
             "compute units", "capacity", "exceeded")


def multicall(rpc, calls, chunk=None):
    if chunk is None:
        chunk = getattr(rpc, "chunk", 100)
    """Run Calls batched via JSON-RPC batch requests (Multicall3 is not
    deployed at its canonical address on these chains in this environment,
    verified by eth_getCode on Base; JSON-RPC batching gives the same
    round-trip savings). Falls back to individual eth_call per item.
    Returns list of (ok, decoded_or_errorstr)."""
    results = [None] * len(calls)
    for start in range(0, len(calls), chunk):
        if start:
            time.sleep(0.25)  # pace batches to stay under free-tier limits
        batch = calls[start:start + chunk]
        try:
            raw = rpc.batch_eth_call([(c.to, "0x" + c.data.hex())
                                      for c in batch])
        except Exception:
            raw = None
        for j, c in enumerate(batch):
            i = start + j
            if raw is not None:
                result, err = raw[j]
                if err is None and result is not None:
                    try:
                        results[i] = (True, c.decode(bytes.fromhex(result[2:])))
                        continue
                    except Exception as e:
                        results[i] = (False, f"decode: {e}")
                        continue
                results[i] = (False, err or "empty result")
            else:  # batch transport failed: individual fallback
                results[i] = _single(rpc, calls[i])
    # retry rounds for transient per-item failures (rate limits): single
    # calls with growing pauses until a round recovers nothing (max 5).
    for rnd in range(5):
        bad = [i for i, r in enumerate(results)
               if r is not None and not r[0]
               and any(t in str(r[1]).lower() for t in TRANSIENT)]
        if not bad:
            break
        recovered = 0
        for i in bad:
            time.sleep(0.4 + 0.3 * rnd)
            results[i] = _single(rpc, calls[i])
            if results[i][0]:
                recovered += 1
        if not recovered:
            break
    return results


def _single(rpc, c):
    """One eth_call with at most one endpoint rotation, no long sleeps."""
    for _ in range(2):
        try:
            r1 = rpc.eth_call_once(c.to, "0x" + c.data.hex())
            return (True, c.decode(bytes.fromhex(r1[2:])))
        except Exception as e:
            rpc.idx += 1
            last = e
    return (False, str(last)[:120])


# ---- function signatures / output types -------------------------------------
BASIC_OBS = ("(uint128,uint128,uint128,uint128,uint128,uint128,"
             "uint128,uint128,uint48,uint48)")
CL_OBS = ("(uint160,uint40,uint32,int24,uint160,uint48,uint120,uint120,"
          "uint120,uint120,uint120,uint120,uint120,uint120)")
GAUGE_STATE_OUT = ["uint128", "uint128", "uint48", "bool", "bool", "uint128",
                   "uint256", "uint256",
                   "(int128,int128,uint48,uint128)"]
INCENTIVE_OUT = ["(address,uint256,uint256,uint48,uint48,address)[]"]


def addr(x):
    return Web3.to_checksum_address(x)


def enumerate_pools(rpc, cfg):
    """FactoryRegistry -> target factories -> targets (pools)."""
    reg = cfg["registry"]
    res = multicall(rpc, [Call(reg, "targetFactories()", [], [],
                               ["address[]"])])
    if not res[0][0]:
        raise RPCError(f"targetFactories failed: {res[0][1]}")
    factories = [addr(a) for a in res[0][1][0]]
    calls = [Call(reg, "factoryToTargets(address)", ["address"], [f],
                  ["address[]"]) for f in factories]
    res = multicall(rpc, calls)
    pools = {}  # pool -> factory
    for f, r in zip(factories, res):
        if not r[0]:
            continue
        for t in r[1][0]:
            pools.setdefault(addr(t), f)
    return factories, pools


def pool_type(factory, cfg):
    f = factory.lower()
    if f == cfg["cl_factory"].lower():
        return "cl"
    if f == STABLE_F.lower():
        return "basic_stable"
    if f == VOLATILE_F.lower():
        return "basic_volatile"
    return "unknown"


class TokenCache:
    def __init__(self, path):
        self.path = path
        self.data = {}
        if os.path.exists(path):
            try:
                self.data = json.load(open(path))
            except Exception:
                self.data = {}

    def save(self):
        with open(self.path, "w") as f:
            json.dump(self.data, f)

    def get(self, chain, token):
        return self.data.get(f"{chain}:{token.lower()}")

    def put(self, chain, token, symbol, decimals):
        self.data[f"{chain}:{token.lower()}"] = {"symbol": symbol,
                                                 "decimals": decimals}


def read_chain(chain, cfg, cache, max_pools=None, only=None):
    rpc = RPC(cfg["rpcs"], cfg["chain_id"])
    rpc.chunk = cfg.get("chunk", 100)
    stats = {"chain": chain, "chain_id": cfg["chain_id"], "rpc_used": cfg["rpcs"],
             "errors": []}
    got_id = int(rpc.call("eth_chainId", []), 16)
    if got_id != cfg["chain_id"]:
        raise RPCError(f"chainId mismatch: got {got_id}")
    block = rpc.block_number()
    now = rpc.block_timestamp(block)
    stats["block"] = block
    stats["block_timestamp"] = now
    code = rpc.get_code(MULTICALL3)
    stats["multicall3"] = code != "0x"
    if not stats["multicall3"]:
        stats["errors"].append(
            "Multicall3 not deployed at canonical address on this chain; "
            "JSON-RPC batch requests used for batching instead")

    t0 = time.time()
    factories, pools = enumerate_pools(rpc, cfg)
    print(f"    [{chain}] enumerated {len(pools)} pools in "
          f"{time.time()-t0:.0f}s", flush=True)
    stats["factories"] = len(factories)
    stats["pools_found"] = len(pools)
    items = sorted(pools.items())
    if only:
        items = [(p, f) for p, f in items if p.lower() in only]
        print(f"    [{chain}] --only filter: {len(items)} target pools",
              flush=True)
    if max_pools:
        items = items[:max_pools]
    stats["pools_attempted"] = len(items)

    # ---- batch A0: tokens + tickSpacing (classifies CL vs Basic) ----------
    calls0, meta0 = [], []
    for pool, factory in items:
        base = len(calls0)
        calls0.append(Call(pool, "token0()", [], [], ["address"]))
        calls0.append(Call(pool, "token1()", [], [], ["address"]))
        calls0.append(Call(pool, "tickSpacing()", [], [], ["int24"]))
        meta0.append((pool, factory, base))
    res0 = multicall(rpc, calls0) if calls0 else []
    print(f"    [{chain}] batch A0 done {time.time()-t0:.0f}s", flush=True)

    # ---- batch A: spot/reserves, tape observe (correct tape), gauge -------
    calls, meta = [], []
    for (pool, factory, b0) in meta0:
        hint = pool_type(factory, cfg)
        rts = res0[b0 + 2]
        if rts[0]:
            kind, tick_spacing = "cl", rts[1][0]
        elif hint == "cl":  # factory says CL but tickSpacing failed: try CL
            kind, tick_spacing = "cl", None
        else:
            kind, tick_spacing = hint, None
        base = len(calls)
        if kind == "cl":
            calls.append(Call(pool, "slot0()", [], [],
                              ["uint160", "int24", "uint16", "uint16",
                               "uint16", "bool"]))
            calls.append(Call(CL_POOL_TAPE, "observe(address,uint48[])",
                              ["address", "uint48[]"], [pool, [WINDOW, 0]],
                              [CL_OBS + "[]"]))
        else:
            calls.append(Call(pool, "getReserves()", [], [],
                              ["uint256", "uint256", "uint256"]))
            calls.append(Call(POOL_TAPE, "observe(address,uint48[])",
                              ["address", "uint48[]"], [pool, [WINDOW, 0]],
                              [BASIC_OBS + "[]"]))
        calls.append(Call(cfg["registry"], "targetToGauge(address)",
                          ["address"], [pool], ["address"]))
        meta.append((pool, factory, kind, tick_spacing, base, b0))
    resA = multicall(rpc, calls) if calls else []
    print(f"    [{chain}] batch A done {time.time()-t0:.0f}s", flush=True)

    # ---- batch A1: pool swap fee ------------------------------------------
    # CL: pool.fee() -> uint24 in pips (1e6 = 100%), live value from the
    # pool's swap hook, so it can drift between reads. Basic: the pool
    # factory's getBaseFee(pool, caller, amt0, amt1, reserves) -> uint256
    # in hundredths of a percent (100 = 1%); this is the base swap fee
    # WITHOUT the MEV tax that factory.getFee would add on top. Amounts
    # are 0 and reserves are the pool's real ones from batch A; the fee
    # modules in the official repos ignore the amounts. Verified
    # 2026-10-24 vs the 18:45 HKT UI snapshot: CL50 300 -> 0.03%,
    # CL200 10000 -> 1%, CL1 85/60 -> <0.01%, Basic Volatile 30 -> 0.3%.
    fcalls, fmeta = [], []
    for (pool, factory, kind, tick_spacing, base, b0) in meta:
        if kind in ("cl", "unknown"):
            fcalls.append(Call(pool, "fee()", [], [], ["uint24"]))
        else:
            rsp = resA[base]
            r0v, r1v = (rsp[1][0], rsp[1][1]) if rsp[0] else (0, 0)
            fcalls.append(Call(
                factory,
                "getBaseFee(address,address,uint256,uint256,uint256,"
                "uint256)",
                ["address", "address", "uint256", "uint256", "uint256",
                 "uint256"], [pool, ZERO, 0, 0, r0v, r1v], ["uint256"]))
        fmeta.append((pool, kind))
    resF = multicall(rpc, fcalls) if fcalls else []
    fee_by_pool, fee_err = {}, {}
    for (pool, kind), r in zip(fmeta, resF):
        if r[0]:
            raw = r[1][0]
            fee_by_pool[pool] = (raw / 10000.0 if kind in ("cl", "unknown")
                                 else raw / 100.0)
        else:
            fee_err[pool] = r[1]
    print(f"    [{chain}] batch A1 (fees) done {time.time()-t0:.0f}s",
          flush=True)

    # ---- token metadata ----------------------------------------------------
    tokens = set()
    for pool, factory, kind, tick_spacing, base, b0 in meta:
        for j in (0, 1):
            r = res0[b0 + j]
            if r[0]:
                tokens.add(addr(r[1][0]))
    need = [t for t in sorted(tokens) if not cache.get(chain, t)]
    mcalls = []
    for t in need:
        mcalls.append(Call(t, "symbol()", [], [], ["string"]))
        mcalls.append(Call(t, "decimals()", [], [], ["uint8"]))
    mres = multicall(rpc, mcalls) if mcalls else []
    for i, t in enumerate(need):
        s, d = mres[2 * i], mres[2 * i + 1]
        if s[0] and d[0]:
            cache.put(chain, t, s[1][0], d[1][0])
    cache.save()

    def tok_meta(t):
        m = cache.get(chain, t)
        if m:
            return m["symbol"], m["decimals"]
        return None, None

    # ---- assemble raw pool records ----------------------------------------
    records = []
    for pool, factory, kind, tick_spacing, base, b0 in meta:
        rec = {"chain": chain, "pool": pool, "factory": factory, "kind": kind,
               "tick_spacing": tick_spacing, "errors": []}
        if pool in fee_by_pool:
            rec["fee_pct"] = round(fee_by_pool[pool], 6)
        else:
            rec["fee_pct"] = None
            rec["errors"].append(f"fee read failed: "
                                 f"{fee_err.get(pool)}")
        r0, r1 = res0[b0], res0[b0 + 1]
        if r0[0] and r1[0]:
            rec["token0"], rec["token1"] = addr(r0[1][0]), addr(r1[1][0])
            rec["symbol0"], rec["dec0"] = tok_meta(rec["token0"])
            rec["symbol1"], rec["dec1"] = tok_meta(rec["token1"])
        else:
            rec["errors"].append("token0/token1 read failed")
        rsp = resA[base]
        if kind == "cl":
            if rsp[0]:
                rec["sqrtPriceX96"] = rsp[1][0]
        else:
            if rsp[0]:
                rec["reserve0"], rec["reserve1"] = rsp[1][0], rsp[1][1]
        robs = resA[base + 1]
        if robs[0]:
            obs = robs[1][0]
            if len(obs) == 2:
                old, new = obs[0], obs[1]
                if kind == "cl":
                    rec["fee_raw"] = (new[8] - old[8], new[9] - old[9])
                    rec["mev_raw"] = (new[12] - old[12], new[13] - old[13])
                    rec["window_seconds"] = new[1] - old[1]
                else:
                    rec["fee_raw"] = (new[0] - old[0], new[1] - old[1])
                    rec["mev_raw"] = (new[6] - old[6], new[7] - old[7])
                    rec["window_seconds"] = new[8] - old[8]
            else:
                rec["errors"].append(f"observe returned {len(obs)} obs")
        else:
            rec["errors"].append(f"tape observe failed: {robs[1]}")
        rg = resA[base + 2]
        rec["gauge"] = addr(rg[1][0]) if rg[0] and int(rg[1][0], 16) else None
        if not rg[0]:
            rec["errors"].append(f"targetToGauge failed: {rg[1]}")
        records.append(rec)

    # ---- batch B: gauge states, rewards manager, incentive counts ---------
    gauged = [r for r in records if r["gauge"]]
    bcalls, bmeta = [], []
    for rec in gauged:
        g = rec["gauge"]
        bcalls.append(Call(LEAF_VOTER, "gaugeStates(address)", ["address"],
                           [g], GAUGE_STATE_OUT))
        bcalls.append(Call(cfg["registry"], "gaugeToRewards(address)",
                           ["address"], [g], ["address"]))
        bmeta.append(rec)
    resB = multicall(rpc, bcalls) if bcalls else []
    for i, rec in enumerate(bmeta):
        rs, rr = resB[2 * i], resB[2 * i + 1]
        if rs[0]:
            ceiling, claimed, lsettle, isreg, isact, surplus, li, lti, pt = rs[1]
            bias, slope, ts, perm = pt
            weight = max(bias - slope * max(now - ts, 0), 0) + perm
            rec["allocation_raw"] = weight
            rec["allocation"] = weight / 1e18
        else:
            rec["errors"].append(f"gaugeStates failed: {rs[1]}")
        if rr[0] and int(rr[1][0], 16):
            rec["rewards_manager"] = addr(rr[1][0])
    # incentive counts
    mgrs = [r for r in records if r.get("rewards_manager")]
    icalls = [Call(r["rewards_manager"], "incentiveCount()", [], [],
                   ["uint256"]) for r in mgrs]
    ires = multicall(rpc, icalls) if icalls else []
    for rec, r in zip(mgrs, ires):
        rec["incentive_count"] = r[1][0] if r[0] else None
        if not r[0]:
            rec["errors"].append(f"incentiveCount failed: {r[1]}")
    print(f"    [{chain}] gauge/incentive batches done "
          f"{time.time()-t0:.0f}s", flush=True)
    stats["rpc_calls"] = rpc.calls
    stats["rpc_errors"] = rpc.errors
    stats["read_seconds"] = round(time.time() - t0, 1)
    return records, stats, now, block


def read_incentives(records_by_chain, rpcs_by_chain, now_by_chain):
    """For each rewards manager with programs, read all programs and build
    two raw token-amount lists per record (rate is token/sec * 1e18):
    - incentive_raw_gross: every program's trailing-48h streamed amount,
      rate * overlap([start,end], [now-48h, now]) — the v1 definition,
      kept for reference only.
    - incentive_raw (UI parity, proven 2026-10-24): ONLY programs still
      live at read time (end > now), each contributing rate * WINDOW —
      the FULL 48h window, not pro-rated by overlap. Ended programs
      contribute 0. This reproduced the official UI's Incentives 48H
      at ratios 0.998-1.000 on 6 Base pools (targeted read 19:39 HKT
      vs UI capture 19:38 HKT)."""
    for chain, records in records_by_chain.items():
        rpc = rpcs_by_chain[chain]
        now = now_by_chain[chain]
        targets = [r for r in records if r.get("incentive_count")]
        if not targets:
            for r in records:
                if r.get("incentive_count") == 0:
                    r["incentives48h_usd"] = 0.0
            continue
        calls, meta = [], []
        for rec in targets:
            n = min(rec["incentive_count"], 200)
            calls.append(Call(rec["rewards_manager"],
                              "incentives(uint256[])", ["uint256[]"],
                              [list(range(1, n + 1))], INCENTIVE_OUT))
            meta.append(rec)
        res = multicall(rpc, calls)
        for rec, r in zip(meta, res):
            if not r[0]:
                rec["incentives48h_usd"] = None
                rec["errors"].append(f"incentives read failed: {r[1]}")
                continue
            rec["incentives48h_usd"] = 0.0  # base; raw amounts priced later
            for (token, amount, rate, start, end, creator) in r[1][0]:
                if rate == 0:
                    continue
                rec.setdefault("incentive_tokens", set()).add(addr(token))
                # Gross (reference only): trailing-48h streamed amount,
                # rate * overlap of [start, end] with [now-48h, now],
                # counting live and ended programs alike (v1 definition).
                overlap = max(0, min(end, now) - max(start, now - WINDOW))
                if overlap > 0:
                    rec.setdefault("incentive_raw_gross", []).append(
                        (addr(token), rate * overlap / 1e18))
                # Live-only (UI parity): a program contributes to
                # incentives48h only if it is still live at read time
                # (end > now), and then at rate * WINDOW — the full 48h
                # window, NOT pro-rated by overlap. Ended programs
                # contribute 0 here and survive only in the gross list.
                if end > now:
                    rec.setdefault("incentive_raw", []).append(
                        (addr(token), rate * WINDOW / 1e18))
            rec["_incentive_overlap_done"] = True
        for rec in records:
            if rec.get("incentive_count") == 0:
                rec["incentives48h_usd"] = 0.0


def coinbase_prices(symbols):
    """symbol -> USD via Coinbase spot (parallel). Missing on failure."""
    from concurrent.futures import ThreadPoolExecutor

    def one(sym):
        asset = CB_ALIAS.get(sym, sym)
        try:
            req = urllib.request.Request(
                f"https://api.coinbase.com/v2/prices/{asset}-USD/spot",
                headers={"User-Agent": UA["User-Agent"]})
            with urllib.request.urlopen(req, timeout=8) as r:
                return sym, float(json.load(r)["data"]["amount"])
        except Exception:
            return sym, None

    todo = [s for s in sorted(symbols) if s]
    out = {}
    with ThreadPoolExecutor(max_workers=12) as ex:
        for sym, px in ex.map(one, todo):
            if px:
                out[sym] = px
    return out


def price_tokens(records, cb_prices):
    """Per-chain token price map (chain, token) -> usd; seeded from Coinbase
    (matched by symbol), propagated through pool spot prices."""
    prices = {}  # (chain, token) -> (usd, via)
    by_chain = {}
    for rec in records:
        by_chain.setdefault(rec["chain"], []).append(rec)
    for chain, recs in by_chain.items():
        for rec in recs:
            for t, s in ((rec.get("token0"), rec.get("symbol0")),
                         (rec.get("token1"), rec.get("symbol1"))):
                if t and s and s in cb_prices:
                    prices[(chain, t)] = (cb_prices[s], "coinbase")
        # propagation rounds
        for _ in range(12):
            changed = False
            for rec in recs:
                t0, t1 = rec.get("token0"), rec.get("token1")
                d0, d1 = rec.get("dec0"), rec.get("dec1")
                if not t0 or not t1 or d0 is None or d1 is None:
                    continue
                spot = None  # price of token1 in token0 units (raw-adj)
                if rec["kind"] == "cl" and rec.get("sqrtPriceX96"):
                    sp = rec["sqrtPriceX96"] / 2 ** 96
                    spot = sp * sp * 10 ** (d0 - d1)  # token1 per token0 (human)
                elif rec.get("reserve0") and rec.get("reserve1"):
                    spot = (rec["reserve1"] / 10 ** d1) / (rec["reserve0"] / 10 ** d0)
                if not spot:
                    continue
                p0 = prices.get((chain, t0))
                p1 = prices.get((chain, t1))
                if p0 and not p1:
                    prices[(chain, t1)] = (p0[0] * spot, "onchain")
                    changed = True
                elif p1 and not p0:
                    prices[(chain, t0)] = (p1[0] / spot, "onchain")
                    changed = True
            if not changed:
                break
    return prices


def finalize(records, prices, cb_prices, aero_usd, now_by_chain, block_by_chain):
    out = []
    for rec in records:
        chain = rec["chain"]
        r = {"chain": chain, "pool": rec["pool"], "gauge": rec.get("gauge"),
             "token0": rec.get("token0"), "token1": rec.get("token1"),
             "symbol0": rec.get("symbol0"), "symbol1": rec.get("symbol1"),
             "pair": (f"{rec['symbol0']}/{rec['symbol1']}"
                      if rec.get("symbol0") and rec.get("symbol1") else None),
             "type": {"cl": f"CL{rec.get('tick_spacing')}" if rec.get("tick_spacing") else "CL",
                      "basic_volatile": "Basic Volatile",
                      "basic_stable": "Basic Stable"}.get(rec["kind"], rec["kind"]),
             "fee_pct": rec.get("fee_pct"),
             "factory": rec["factory"],
             "block": block_by_chain[chain],
             "read_timestamp": now_by_chain[chain],
             "window_seconds": rec.get("window_seconds"),
             "allocation": rec.get("allocation"),
             "errors": rec.get("errors", [])}
        p0 = prices.get((chain, rec.get("token0") or ""))
        p1 = prices.get((chain, rec.get("token1") or ""))
        r["priced"] = bool(p0 and p1)
        r["price_via"] = {"token0": p0[1] if p0 else None,
                          "token1": p1[1] if p1 else None}
        fees = mev = None
        if r["priced"] and rec.get("fee_raw") and rec.get("dec0") is not None:
            f0 = rec["fee_raw"][0] / 10 ** rec["dec0"]
            f1 = rec["fee_raw"][1] / 10 ** rec["dec1"]
            m0 = rec["mev_raw"][0] / 10 ** rec["dec0"]
            m1 = rec["mev_raw"][1] / 10 ** rec["dec1"]
            fees = f0 * p0[0] + f1 * p1[0]
            mev = m0 * p0[0] + m1 * p1[0]
        r["fees48h_usd"] = fees
        r["mev48h_usd"] = mev
        def price_raw(raw_list):
            total = 0.0
            for token, raw_amt in raw_list:
                meta = rec.get("incentive_meta", {}).get(token)
                pr = prices.get((chain, token))
                if pr is None and meta and meta[0] in cb_prices:
                    pr = (cb_prices[meta[0]], "coinbase")
                if pr is None or meta is None:
                    return None
                total += raw_amt / 10 ** meta[1] * pr[0]
            return total

        if rec.get("incentive_raw") or rec.get("incentive_raw_gross"):
            # incentives48h_usd = live-programs-only (UI parity);
            # incentives48h_gross_usd = v1 gross trailing-48h stream.
            r["incentives48h_usd"] = price_raw(rec.get("incentive_raw") or [])
            r["incentives48h_gross_usd"] = price_raw(
                rec.get("incentive_raw_gross") or [])
            if r["incentives48h_usd"] is None or \
                    r["incentives48h_gross_usd"] is None:
                r["errors"] = r["errors"] + ["incentive token unpriced"]
        elif "incentives48h_usd" in rec:
            r["incentives48h_usd"] = rec["incentives48h_usd"]
            r["incentives48h_gross_usd"] = rec["incentives48h_usd"]
        else:
            r["incentives48h_usd"] = None
            r["incentives48h_gross_usd"] = None
        apr = None
        if (fees is not None and r["incentives48h_usd"] is not None
                and rec.get("allocation") and aero_usd):
            rewards = fees + (mev or 0.0) + r["incentives48h_usd"]
            denom = rec["allocation"] * aero_usd
            if denom > 0:
                apr = rewards * ANNUALIZE / denom * 100
        r["stake_apr_pct"] = apr
        r["rewards_manager"] = rec.get("rewards_manager")
        r["incentive_program_count"] = rec.get("incentive_count")
        out.append(r)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chains", default=",".join(CHAINS))
    ap.add_argument("--max-pools-per-chain", type=int, default=None)
    ap.add_argument("--only", default=None,
                    help="comma-separated pool addresses; read only these "
                         "(targeted validation runs)")
    ap.add_argument("--out", default=os.path.join(HERE, "pools.json"))
    args = ap.parse_args()

    cache = TokenCache(os.path.join(HERE, "token_cache.json"))
    records_by_chain, stats, now_by_chain, block_by_chain = {}, {}, {}, {}
    rpcs = {}
    for chain in args.chains.split(","):
        chain = chain.strip()
        cfg = CHAINS[chain]
        print(f"== {chain} ==", flush=True)
        try:
            rpc = RPC(cfg["rpcs"], cfg["chain_id"])
            rpc.chunk = cfg.get("chunk", 100)
            only = ({a.strip().lower() for a in args.only.split(",")}
                    if args.only else None)
            recs, st, now, block = read_chain(chain, cfg, cache,
                                              args.max_pools_per_chain, only)
            records_by_chain[chain] = recs
            stats[chain] = st
            now_by_chain[chain] = now
            block_by_chain[chain] = block
            rpcs[chain] = rpc
            print(f"  pools found {st['pools_found']}, attempted "
                  f"{st['pools_attempted']}, block {block}", flush=True)
        except Exception as e:
            import traceback
            traceback.print_exc()
            stats[chain] = {"chain": chain, "fatal": str(e)[:300]}
            print(f"  FATAL: {e}", flush=True)

    all_records = [r for rs in records_by_chain.values() for r in rs]
    read_incentives(records_by_chain, rpcs, now_by_chain)

    # metadata for incentive tokens not already cached
    for chain, recs in records_by_chain.items():
        missing = set()
        for rec in recs:
            for token, _ in (rec.get("incentive_raw_gross")
                             or rec.get("incentive_raw") or []):
                if not cache.get(chain, token):
                    missing.add(token)
        if missing:
            calls = []
            for t in sorted(missing):
                calls.append(Call(t, "symbol()", [], [], ["string"]))
                calls.append(Call(t, "decimals()", [], [], ["uint8"]))
            res = multicall(rpcs[chain], calls)
            for i, t in enumerate(sorted(missing)):
                s, d = res[2 * i], res[2 * i + 1]
                if s[0] and d[0]:
                    cache.put(chain, t, s[1][0], d[1][0])
            cache.save()
        for rec in recs:
            meta = {}
            for token, _ in (rec.get("incentive_raw_gross")
                             or rec.get("incentive_raw") or []):
                m = cache.get(chain, token)
                if m:
                    meta[token] = (m["symbol"], m["decimals"])
            rec["incentive_meta"] = meta

    symbols = set()
    for rec in all_records:
        for s in (rec.get("symbol0"), rec.get("symbol1")):
            if s:
                symbols.add(s)
        for token, _ in (rec.get("incentive_raw_gross")
                         or rec.get("incentive_raw") or []):
            m = cache.get(rec["chain"], token)
            if m:
                symbols.add(m["symbol"])
    symbols.add("AERO")
    cb = coinbase_prices(symbols)
    aero_usd = cb.get("AERO")
    print("coinbase priced symbols:", len(cb), "AERO:", aero_usd, flush=True)

    prices = price_tokens(all_records, cb)
    final = finalize(all_records, prices, cb, aero_usd, now_by_chain,
                     block_by_chain)
    doc = {"generated_at": int(time.time()), "aero_usd": aero_usd,
           "coinbase_symbols_priced": len(cb), "chain_stats": stats,
           "pool_count": len(final), "pools": final}
    with open(args.out, "w") as f:
        json.dump(doc, f, indent=1, default=str)
    print(f"wrote {args.out}: {len(final)} pools")
    for chain, st in stats.items():
        print(chain, {k: st.get(k) for k in ("pools_found", "pools_attempted",
                                             "fatal")})


if __name__ == "__main__":
    main()
