# AERO 分析看板 · AERO Analytics Dashboard

**线上站点：[aerodash.xyz](https://aerodash.xyz/)**

新 AERO（Aerodrome 新系统）的链上数据看板：投票收益、LP 收益、sAERO 仓位查询与系统总览。全部数据由链上合约直读，**每小时更新**，页面打开后每 10 分钟自动刷新。

覆盖 7 条链：Base、Ethereum、OP Mainnet、Arbitrum、Ink、Arc、Robinhood Chain。

## 功能

### 投票收益
- 全部达标池按 Allocation（分配量）分 6 档，档内按 Stake APR 降序取前 10 名
- 顶部有 24 小时 Allocation 异动榜（涨幅 / 跌幅）
- 手机端有档位筛选，一次只看一档

**口径**：池子手续费按「手续费生成份额」计入分配者收益——CL 池用 LpSugar 的质押流动性份额（与官方投票页同一口径），Basic 池用质押价值份额；未质押部分产生的手续费归 LP 本人，不计入。份额为读取时点快照，套用在 48 小时窗口上，与官方个别数字可能有小时点差异。

**入墙门槛**：价格可信（Coinbase 报价，或链上共识价——同一代币在 ≥2 个以主流报价代币为对手方的池中互为印证且通过价格一致性校验）、Rewards 48H ≥ $10、异常费率池剔除。

### 我的 sAERO
- 输入地址（可加 tokenId）查询仓位：当前投票分配、待领估算、各链冷却倒计时（分配冷却 47 小时 / 169,200 秒）
- 投票建议引擎：按当前收益格局给出建议分配（单池稀释上限 25%，最多 8 个池），并与现分配并排对比
- 批量雷达：一次最多 50 个地址
- 纯浏览器端只读查询（公共 RPC），**不签名、不发交易、不接触私钥**

### LP 收益
- TVL ≥ $100K 的池子，按链切换查看
- AERO 排放 APR 与手续费 APR **分开列示、从不相加**（一个池的仓位只能赚取其中一种口径的收益）

### 数据总览
- 全系统与分链统计：达标池数、TVL、48 小时手续费
- **累计销毁**：逐小时增量扫描 AERO 合约上 VotingEscrow → 零地址的销毁转账（Momentum Fund 回购销毁），自动累计并显示最近一笔

## 数据管线

```
reader（公共 RPC 直读） → adapter（口径换算 + 入墙过滤） → 质量门 → GitHub 原子提交 → Cloudflare Pages
```

- Reader 直读 PoolTape / CLPoolTape 的 48 小时观测窗口、LeafVoter 的分配状态、LpSugar 的质押流动性；激励按在运行的 program 费率 × 完整 48 小时计
- 价格：Coinbase 主流币报价为锚，链上现货多池共识校验；偏离过大的代币价格被修正或标记未验证，其池子不进任何榜单
- 质量门：锚点池漂移、原始池数骤降等硬指标不过门则不发布，线上保留上一轮数据
- 每小时快照（pools / lp）gzip 存档于 `data-history` 分支，供异动榜与回溯分析使用

## 仓库结构

- `dist/` — 网站本体：单文件静态页 `index.html` + `data/` 下的四份数据饲料（pools / lp / movers / status）
- `reader/` — 数据管线代码（reader、adapter、movers、质量门、销毁扫描）
- `.github/workflows/` — 仅负责 push 后部署到 Cloudflare Pages（数据由外部管线按小时提交）

## 说明

- 本站为独立开发的数据工具，与 Aerodrome / Dromos 官方无关；数据来自公开链上读取与公开接口，存在读取时点差异，关键决策请以官方应用与链上原始数据为准
- 不构成投资建议
- 本站由 Meta Muse 开发与维护

---

## English summary

An on-chain analytics dashboard for the new AERO system (Aerodrome's post-migration tokenomics), live at [aerodash.xyz](https://aerodash.xyz/), refreshed hourly across 7 chains (Base, Ethereum, OP Mainnet, Arbitrum, Ink, Arc, Robinhood Chain).

- **Vote rewards** — pools grouped into six Allocation tiers, top 10 per tier by Stake APR, plus a 24h allocation-movers board. Fees are attributed to allocators by fee-generation share (LpSugar staked-liquidity share for CL pools, staked value share for Basic pools — the same caliber as the official vote page). Admission requires a trusted price (Coinbase-quoted, or on-chain consensus-corroborated across ≥2 anchored pools and gate-verified), Rewards 48H ≥ $10, and sanity filters.
- **My sAERO** — browser-side read-only position lookup via public RPCs: current allocation, claimable estimates, per-chain 47h cooldown countdowns, and a suggestion engine (25% per-pool dilution cap). No signing, no transactions.
- **LP yields** — pools with TVL ≥ $100K by chain; AERO emissions APR and fees APR are always shown separately, never summed.
- **Overview** — system and per-chain aggregates, plus a cumulative burn counter that incrementally tracks AERO transfers from the VotingEscrow to the zero address (Momentum Fund buyback-and-burn).

Data pipeline: public-RPC reader (PoolTape/CLPoolTape 48h observations, LeafVoter allocations, LpSugar staked liquidity) → adapter → quality gate → atomic GitHub commits → Cloudflare Pages. Hourly snapshots are archived (gzipped) on the `data-history` branch. Independent tool, not affiliated with Aerodrome/Dromos; not financial advice. Built and maintained by Meta Muse.
