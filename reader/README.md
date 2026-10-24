# 链上读取管线（两步）

页面投票墙的数据由本目录两个脚本按小时生成，只读公开 RPC、无密钥、无 DefiLlama：

```bash
# 1) 链上直读 → 原始数据（7 链全量，约 21 分钟，免费公共 RPC）
python reader/reader.py --out raw_pools.json

# 2) 适配 + 可信过滤 → 页面格式，写到站点 data 目录
python reader/to_page_json.py --in raw_pools.json --out dist/data/pools.json
```

页面加载时自动 fetch `data/pools.json` 并按自身规则渲染（Allocation 六档、
Rewards 48H ≥ $10 防尘、档内 Stake APR 降序前 10）；读不到时回退内嵌快照。

## 第 1 步：reader.py（原始读取）

- 枚举：各链 FactoryRegistry 的 gauged 池；Fees/MEV 48H 来自 PoolTape/CLPoolTape
  的 observe 累计差值；Allocation 来自 LeafVoter gaugeStates 权重
  （bias − slope×(now−ts) + permanentStakeBalance）；价格为 Coinbase spot 主价 +
  链上现价传导，每条记录带 `price_via`（coinbase / onchain）标记来源。
- 输出原始 JSON：`{"generated_at": epoch, "aero_usd": …, "pools": […]}`，
  每池含 fees48h_usd / mev48h_usd / incentives48h_usd /
  incentives48h_gross_usd / stake_apr_pct / allocation / price_via 以及
  fee_pct（池实时兑换费率，百分数；来源见第 2 步 fee 说明）等。

### Incentives 口径（2026-10-24 已验证）

`incentives48h_usd` = Σ 读取时刻**仍在进行**（end > now）的激励活动
`rate × 172,800`（**整段 48 小时，不按重叠比例折算**）× 代币价；
已结束的活动计 0。这与官方 UI 口径一致：2026-10-24 19:39 HKT 定向读取对
19:38 UI 抓取的 6 个 Base 池比值为 0.996–1.000。
`incentives48h_gross_usd` 保留 v1 的毛额口径（过去 48 小时实际流出、含已
结束活动），仅作参考，不进页面。

## 第 2 步：to_page_json.py（页面适配）

输出页面格式：

```json
{
  "generated_at": "2026-10-24T11:13:35Z", "source": "onchain", "aero_price_usd": 0.71,
  "pools": [
    {"pair": "WETH/cbBTC", "fee": "<0.01%", "chain": "Base", "type": "CL1",
     "fees48h": 5598.71, "incentives48h": 1530.10, "rewards48h": 7128.81,
     "stake_apr": 18.11, "allocation": 10175226.27}
  ]
}
```

- fees48h = fees + MEV（官方 Fees 48H 含 MEV，探针比值 1.003 验证）；
  rewards48h = fees48h + incentives48h；stake_apr 为百分数；
  allocation 为 Allocation Power 原值。
- **可信过滤**（全部满足才进页面）：allocation > 0、stake_apr 可算、
  **两个代币都由 Coinbase 定价**（price_via 均为 coinbase）。链上单池现价
  对冷门/被操纵代币可偏离数个数量级，故任一代币靠链上传导定价的池一律
  不进投票墙（原始数据仍保留在 raw 输出中）。另设健全性剔除：
  fees48h > 10 × allocation × AERO 价 的记录视为定价伪影丢弃。
  v1 数据实测：1,040 → 181 池（剔除 无 allocation 621 / 价格不可信 214 /
  无 pair 23 / 伪影 1）。
- `fee` 字段（2026-10-24 补读）：reader.py 每池多读一次实时费率并存入
  原始记录 `fee_pct`（百分数），本步格式化为页面字符串（`0.03%` /
  `0.3%` / `1%`；0 < fee_pct < 0.01 时为 `<0.01%`，与官方显示惯例一致）。
  来源：CL 池读池合约 `fee()`（uint24，pips 制，÷10000）；Basic 池读其
  工厂 `getBaseFee(pool, 0, 0, 0, 储备)`（uint256，0.01% 为单位，÷100；
  这是未叠加 MEV 税的基础兑换费）。新系统费率与 tick spacing 不是函数
  关系（同一 CL50 有 0.02%–0.30%），只能逐池读。CL 费率是 swap hook 给
  出的动态值，逐时可漂移：2026-10-24 验证读（约 20:0x）对 18:45 UI 快照
  ——WETH/USDC CL50 0.03%、AERO/USDC CL200 1%、AERO/USDC Basic 0.3%
  三池精确一致，两个 CL1 均 <0.01% 一致，USDC/cbBTC CL50 读 0.0107%
  （UI 两位小数显示 0.01%），WETH/cbBTC CL10 读 0.03% 对 UI 0.04%
  （动态漂移）。旧原始数据无 `fee_pct` 时仍输出空串，页面可容忍。

## 已知注意事项

- **代币化股票 CL10 池的 fees 偏低**：USDC/MUc、USDC/SNDKc 等池读取器 fees
  约为官方 UI 的一半（UI/reader ≈ ×1.98），原因未明、调查中；主流池验证
  比值 1.005–1.055（48h 窗口滑动漂移）。这类池的股票代币靠链上定价，
  本来就被可信过滤排除在投票墙之外，不影响页面数据。
- alETH/WETH（OP）激励比值约 1.59×：alETH 链上传导价（≈$3,684）与 UI 隐含
  价（≈$2,315）不符所致，同样被可信过滤排除。
- 全量一轮约 21 分钟（Base ≈ 9 分钟、Robinhood ≈ 7.5 分钟最慢，公共 RPC
  限流所致），小时级刷新可行；有专用 RPC key 可显著缩短。
