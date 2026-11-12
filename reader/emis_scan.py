#!/usr/bin/env python3
"""48 小时净排放采样（2026-11-12 加入）：给 data/status.json 末尾追加

  emis48h_aero  本轮 generated_at 往前约 48 小时的 AERO 总供应变化，
                按实际间隔线性折算到整 48 小时（净排放：窗口内铸造
                减去销毁；ve_scan 读到的 ve_total_supply_aero 之差）
  emis_span_h   两次供应读取的实际间隔（小时，1 位小数）

基线供应取 data-history 存档里的 status 快照（--history-dir，布局
history/YYYY/MM/DD/status-HHMM.json.gz）；存档不足 48 小时深时，
回退到主分支 dist/data/status.json 的逐小时提交记录（GitHub API）。

失败口径与 dexshare_scan 一致：任何失败都以退出码 1 结束且不写
status 文件，本步永不阻断管线；ve_stale=true（本轮供应是沿用值）
时同样不写，避免算出假排放。
"""
import argparse
import gzip
import json
import os
import sys
from datetime import datetime, timezone

import requests

TARGET_H = 48.0
MIN_SPAN_H = 40.0
MAX_SPAN_H = 56.0
GH_COMMITS = ("https://api.github.com/repos/8280998/aerodash/commits"
              "?path=dist/data/status.json&per_page=100")
GH_RAW = "https://raw.githubusercontent.com/8280998/aerodash/{sha}/dist/data/status.json"


def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def supply_pair(doc):
    """从一份 status 文档取 (generated_at, 总供应, 是否沿用值)。"""
    if not isinstance(doc, dict):
        return None
    gen = doc.get("generated_at")
    sup = doc.get("ve_total_supply_aero")
    if not isinstance(gen, str) or not isinstance(sup, (int, float)):
        return None
    if not (sup > 0):
        return None
    return gen, float(sup), bool(doc.get("ve_stale"))


def pick_candidate(cands, now_ts):
    """cands: [(gen, supply, stale)]；挑间隔最接近 48h 的，
    同等接近时优先非沿用值。返回 (span_h, supply) 或 None。"""
    best = None
    for gen, sup, stale in cands:
        try:
            span = (now_ts - parse_ts(gen)) / 3600.0
        except Exception:
            continue
        if span < MIN_SPAN_H or span > MAX_SPAN_H:
            continue
        key = (stale, abs(span - TARGET_H))
        if best is None or key < best[0]:
            best = (key, span, sup)
    if best is None:
        return None
    return best[1], best[2]


def from_history(hist_dir, now_ts):
    cands = []
    if hist_dir and os.path.isdir(hist_dir):
        for root, _dirs, files in os.walk(hist_dir):
            for name in files:
                if not (name.startswith("status-")
                        and name.endswith(".json.gz")):
                    continue
                try:
                    with gzip.open(os.path.join(root, name), "rt") as f:
                        pair = supply_pair(json.load(f))
                except Exception:
                    continue
                if pair:
                    cands.append(pair)
    return pick_candidate(cands, now_ts)


def get_json(url, attempts=2):
    last = None
    for i in range(attempts):
        try:
            r = requests.get(url, timeout=30,
                             headers={"User-Agent": "aerodash-emis/1.0"})
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last = exc
    raise last


def from_commits(now_ts):
    commits = get_json(GH_COMMITS)
    cands = []
    metas = []
    for c in commits:
        try:
            cdate = c["commit"]["committer"]["date"]
            span = (now_ts - parse_ts(cdate)) / 3600.0
        except Exception:
            continue
        if MIN_SPAN_H <= span <= MAX_SPAN_H:
            metas.append((abs(span - TARGET_H), c["sha"]))
    metas.sort()
    for _diff, sha in metas[:3]:
        try:
            pair = supply_pair(get_json(GH_RAW.format(sha=sha)))
        except Exception:
            continue
        if pair:
            cands.append(pair)
            picked = pick_candidate(cands, now_ts)
            if picked:
                return picked
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", required=True)
    ap.add_argument("--history-dir")
    args = ap.parse_args()

    with open(args.status) as f:
        status = json.load(f)
    cur = supply_pair(status)
    if cur is None:
        print("emis: status 缺 generated_at / ve_total_supply_aero",
              file=sys.stderr)
        return 1
    if cur[2]:
        print("emis: 本轮 ve_stale=true，供应为沿用值，跳过",
              file=sys.stderr)
        return 1
    now_ts = parse_ts(cur[0])

    picked = from_history(args.history_dir, now_ts)
    src = "history"
    if picked is None:
        picked = from_commits(now_ts)
        src = "commits"
    if picked is None:
        print("emis: 40–56h 窗口内无可用基线快照", file=sys.stderr)
        return 1
    span_h, old_supply = picked
    emis = (cur[1] - old_supply) * TARGET_H / span_h
    status["emis48h_aero"] = round(emis, 2)
    status["emis_span_h"] = round(span_h, 1)
    with open(args.status, "w") as f:
        json.dump(status, f, indent=1)
    print(f"emis: span {span_h:.1f}h ({src}) net48h {emis:,.0f} AERO")
    return 0


if __name__ == "__main__":
    sys.exit(main())
