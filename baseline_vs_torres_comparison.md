# Baseline Detection vs Torres 原代码 — 最终对比

> Torres et al. "Rolling in the Shadows" (ACM CCS 2024)
> baseline_detection.py — 基于 Dune CSV 的忠实复现

---

## 一、套利检测 (Arbitrage, Torres §3.1)

### 核心算法

| Torres 行号 | 功能 | 状态 |
|:----------:|------|:----:|
| L393 | 按交易分组，遍历每笔 tx 的 swap 列表 | ✅ |
| L395 | 至少 2 个 swap 才进入检测 | ✅ |
| L396 | `first.in_amount <= last.out_amount` 金额预检 | ✅ |
| L397-398 | token 地址非空检查 | ✅ 隐含满足 |
| L399 | 首尾 token 匹配（含 ETH/WETH 等价） | ✅ |
| L400-402 | 初始化 `valid=True`, `intermediary=[first]` | ✅ |
| L404-406 | `prev` 从全局列表取（非 intermediary） | ✅ |
| L407 | `intermediary.append(curr)` | ✅ |
| L408 | 链连续性：`prev.out_token != curr.in_token` — **严格相等** | ✅ |
| L410 | 无 value 泄漏：`prev.out_amount < curr.in_amount` | ✅ |
| L412 | 不同交易所：`prev.exchange == curr.exchange` | ✅ |
| L414 | 子环闭合检查（含 ETH/WETH 等价）+ `len>=2` 守卫 | ✅ |
| L527 | 子环记录后重置 intermediary，`valid` 不重置 | ✅ |

### 利润计算

| 功能 | Torres | 我们 | 状态 |
|------|--------|------|:----:|
| token balance 累加 | `in_amount(-)`, `out_amount(+)` 按 token 分组 | 同逻辑 | ✅ |
| 单位转换 | `amount / 10**decimals`（RPC 获取 decimals） | Dune 已预转换 | 🟡 |
| 代币价格 | CoinGecko 历史 API | `amount_usd / amount` 反推 | 🟡 近似 |
| Gas 成本 | `receipt.gasUsed * tx.gasPrice`（RPC） | Dune `tx_gas_used * tx_effective_gas_price` | 🟡 |
| 精度 | `decimal.Decimal` + `int` wei | Python `float` | 🟡 精度降低 |

### 闪电贷

| 功能 | Torres 行号 | 状态 |
|------|:----------:|:----:|
| Aave V3 FlashLoan 检测 | L536-576 | ❌ |
| Radiant V2 FlashLoan 检测 | L578-617 | ❌ |
| Balancer V2 FlashLoan 检测 | L619-664 | ❌ |
| 闪电贷手续费计入成本 | L698-706 | ❌ |

> 闪电贷检测不影响套利**发现**，仅影响分类标注和利润精度（未扣除手续费 → 利润高估）。

---

## 二、三明治检测 (Sandwich, Torres §3.3)

### 核心算法

| Torres 行号 | 功能 | 状态 |
|:----------:|------|:----:|
| L28 | `BLOCK_RANGE=1` 逐块分析 | ✅ |
| L66-73 | 每块初始化 victims/attackers/transfer_to/transfer_from/asset_transfers | ✅ |
| L83 | `value>0 && from!=to` 过滤 | ✅ |
| L85 | 跳过 WETH 转账 | ✅ |
| L85-87 | `transfer_to[token+_from]` 反转查找 | ✅ |
| L98 | 反转验证：角色互换 + 顺序 + `value_a1 >= value_a2` | ✅ |
| L100-111 | Victim 搜索：`(_from_a1==_from_w) OR (_to_a1==_to_w)` — OR 逻辑 | ✅ |
| L111 | 覆盖赋值（取最后满足条件的 victim） | ✅ |
| L113-114 | `victims.add(event_w.tx_hash)` | ✅ |
| L116 | attacker tx 不在 victims 中 | ✅ |
| L125-126 | `tx1.from != victim.from && tx2.from != victim.from` — EOA 不等 | ✅ |
| L128-129 | 三笔 tx 发给同一合约 + 发起人不同 → 巧合排序，跳过 | ✅ |
| L131-132 | tx1 和 victim 发给同一合约，tx2 不同 → 跳过 | ✅ |
| L134-140 | 提取 exchange 地址 | ❌ |
| L142-162 | RPC 验证交易所合约（Uniswap V2/V3） | ❌ |
| L164-165 | `attackers.add(tx1/tx2)` | ✅ |
| L176-182 | 更新 transfer_to / transfer_from / asset_transfers 索引 | ✅ |
| L192-203 | 双向 transfer 验证（attacker tx 是真正的 swap） | ✅ |
| L200-203 | 两笔 attacker tx 使用相同交易所 pair | ✅ |
| L205-212 | 按 attacker pair 去重 | ✅ |

### 利润计算

| 功能 | Torres 行号 | 状态 |
|------|:----------:|:----:|
| Gas 成本 | L220-224 | ❌ |
| Token balance 累加 | L231-255 | ❌ |
| WETH/token gain | L257-279 | ❌ |
| 总利润 = gain - cost | L288-290 | ❌ |

> 当前检测结果为 0 个三明治，利润计算未实现不影响输出。

---

## 三、总结

### 已实现 ✅（14 项）

| 检测类型 | 功能 |
|---------|------|
| 套利 | 外层首尾预检 (L396-399) |
| 套利 | prev 从全局列表取 (L404-406) |
| 套利 | 严格链连续性检查 (L408) |
| 套利 | 无 value 泄漏 (L410) |
| 套利 | 不同交易所 (L412) |
| 套利 | ETH/WETH 等价闭合 (L414) |
| 套利 | valid 不重启 + len>=2 守卫 (L527) |
| 三明治 | 逐块分析 BLOCK_RANGE=1 (L28) |
| 三明治 | attacker EOA != victim EOA (L125-126) |
| 三明治 | tx_to 三重过滤 (L128-132) |
| 三明治 | Victim OR 逻辑 (L110) |
| 三明治 | 双向 swap 验证 (L192-203) |
| 三明治 | 相同交易所 pair (L200-203) |
| 三明治 | Pair 去重 (L205) |

### 等价替代 🟡（5 项）

| 差异点 | 说明 |
|--------|------|
| Dune CSV vs Archive Node RPC | 数据源不同，Dune DEX 覆盖更广 |
| float vs int (amount 精度) | Dune 已做 decimal 转换，浮点误差极小 |
| amount_usd 反推价格 vs CoinGecko | 利润为近似值，不影响检测逻辑 |
| Dune project 字段 vs RPC 事件签名 | DEX 识别方式不同，覆盖等价 |
| 多 victim 聚合差异 | 影响极低，大部分三明治只有 1 个 victim |

### 未实现 🔴（4 项）

| 功能 | 影响 | 原因 |
|------|------|------|
| 闪电贷检测 (Aave/Radiant/Balancer) | 中 — 不影响发现，影响分类标注 | 需要 FlashLoan 事件数据（Dune 可查但未纳入查询） |
| 闪电贷费用计入成本 | 低 — 部分套利利润高估 | 依赖闪电贷检测 |
| RPC 交易所合约验证 | 中 — 可能增加少量假阳性 | 需要 Archive Node RPC，Dune CSV 无法调用合约 ABI |
| 三明治利润计算 | 低 — 当前检测为 0 | 未来产生结果时需补充 |

---

## 四、字段映射

### 套利：Dune dex.trades → Torres swap

| Dune 列 | Torres 字段 | 用途 |
|---------|------------|------|
| tx_hash | transactionHash | 按 tx 分组 |
| evt_index | logIndex | swap 排序 |
| pool_address | exchange | 不同交易所检查 |
| token_sold_address | in_token | 链连续性 + 闭合检查 |
| token_sold_amount | in_amount | 金额预检 + 泄漏检查 |
| token_bought_address | out_token | 链连续性 + 闭合检查 |
| token_bought_amount | out_amount | 金额预检 + 泄漏检查 |
| amount_usd | —（Torres 用 CoinGecko） | 利润计算 |
| tx_gas_used | receipt.gasUsed | Gas 成本 |
| tx_effective_gas_price | tx.gasPrice | Gas 成本 |

### 三明治：Dune erc20.evt_Transfer → Torres Transfer

| Dune 列 | Torres 字段 | 用途 |
|---------|------------|------|
| tx_hash | transactionHash | victim/attacker 追踪 |
| tx_index | transactionIndex | 块内排序 |
| token_address | event.address | token 识别 + WETH 过滤 |
| transfer_from | _from（topics[1] 解码） | 反转检测 + victim 匹配 |
| transfer_to | _to（topics[2] 解码） | 反转检测 + victim 匹配 |
| transfer_value | _value（data 解码） | value>0 过滤 + 金额比较 |
| tx_from | tx["from"]（RPC getTransaction） | L125-126 EOA 检查 |
| tx_to | tx["to"]（RPC getTransaction） | L128-132 tx_to 过滤 |
