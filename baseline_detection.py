#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Baseline MEV Detection — Faithful reproduction of Torres et al. (Rolling in
the Shadows, CCS 2024) heuristics applied to Arbitrum Dune Analytics CSV data.

Implements:
  1. Arbitrage detection  (Section 3.1) — from dex.trades (q1_swaps_*.csv)
  2. Sandwich detection   (Section 3.3) — from ERC-20 Transfers (q2_transfers_*.csv)

Input files (Dune exports):
  - q1_swaps_pre_eip4844.csv      (block 176351748–176379410, Feb 2024)
  - q1_swaps_post_eip4844.csv     (block 201152729–201167067, Apr 2024)
  - q2_transfers_pre_eip4844.csv  (same block range as swaps pre)
  - q2_transfers_post_eip4844.csv (same block range as swaps post)
"""

import os
import csv
import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Set


# ─────────────────────────────────────────────────────────────────────────────
# Constants — Torres arbitrage.py lines 30-44
# ─────────────────────────────────────────────────────────────────────────────
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
ETH  = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"


def tokens_equivalent(a: str, b: str) -> bool:
    """Torres arbitrage.py line 399/414: in_token == out_token OR both ETH/WETH."""
    a_low, b_low = a.lower(), b.lower()
    if a_low == b_low:
        return True
    if {a_low, b_low} <= {WETH, ETH}:
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Swap:
    """One DEX swap from Dune dex.trades."""
    tx_hash: str
    tx_index: int
    block_number: int
    block_time: str
    evt_index: int
    pool: str            # pool_address
    taker: str           # taker (≈ Torres "sender")
    in_token: str        # token_sold_address
    in_symbol: str       # token_sold_symbol
    in_amount: float     # token_sold_amount
    out_token: str       # token_bought_address
    out_symbol: str      # token_bought_symbol
    out_amount: float    # token_bought_amount
    amount_usd: float
    project: str         # DEX name (uniswap, sushiswap, etc.)
    tx_from: str         # tx originator EOA
    gas_used: int
    gas_price: float     # tx_effective_gas_price


@dataclass
class Transfer:
    """One ERC-20 Transfer event from Dune."""
    tx_hash: str
    tx_index: int
    block_number: int
    block_time: str
    evt_index: int
    token_address: str
    from_address: str    # transfer_from
    to_address: str      # transfer_to
    value: float         # transfer_value (raw uint256)
    tx_from: str         # tx originator EOA
    tx_to: str           # tx recipient contract (Issue #8: needed for Torres L128-132)


@dataclass
class ArbitrageResult:
    tx_hash: str
    block_number: int
    block_time: str
    num_swaps: int
    swap_path: str
    pools: str
    projects: str
    profit_usd: float
    token_balances: Dict
    tx_from: str
    gas_cost_usd: float
    swaps: List[Swap] = field(default_factory=list)


@dataclass
class SandwichResult:
    block_number: int
    block_time: str
    attacker_tx1_hash: str
    attacker_tx1_index: int
    victim_tx_hashes: List[str]
    victim_tx_indices: List[int]
    attacker_tx2_hash: str
    attacker_tx2_index: int
    pool: str
    token_address: str
    attacker_address: str


# ─────────────────────────────────────────────────────────────────────────────
# CSV Loading — adapted to Dune column names
# ─────────────────────────────────────────────────────────────────────────────

def safe_float(val, default=0.0):
    try:
        if val is None or val == '' or val == 'None':
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    try:
        if val is None or val == '' or val == 'None':
            return default
        return int(float(val))
    except (ValueError, TypeError):
        return default


def load_swaps(csv_path: str) -> List[Swap]:
    """Load Dune dex.trades CSV (q1_swaps_*.csv)."""
    swaps = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                swaps.append(Swap(
                    tx_hash=row["tx_hash"].strip().lower(),
                    tx_index=safe_int(row["tx_index"]),
                    block_number=safe_int(row["block_number"]),
                    block_time=row["block_time"].strip(),
                    evt_index=safe_int(row["evt_index"]),
                    pool=row["pool_address"].strip().lower(),
                    taker=row["taker"].strip().lower(),
                    in_token=row["token_sold_address"].strip().lower(),
                    in_symbol=row["token_sold_symbol"].strip(),
                    in_amount=safe_float(row["token_sold_amount"]),
                    out_token=row["token_bought_address"].strip().lower(),
                    out_symbol=row["token_bought_symbol"].strip(),
                    out_amount=safe_float(row["token_bought_amount"]),
                    amount_usd=safe_float(row["amount_usd"]),
                    project=row.get("project", "").strip(),
                    tx_from=row.get("tx_from", "").strip().lower(),
                    gas_used=safe_int(row.get("tx_gas_used", 0)),
                    gas_price=safe_float(row.get("tx_effective_gas_price", 0)),
                ))
            except (ValueError, KeyError) as e:
                continue
    return swaps


def load_transfers(csv_path: str) -> List[Transfer]:
    """Load Dune ERC-20 Transfer CSV (q2_transfers_*.csv)."""
    transfers = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                transfers.append(Transfer(
                    tx_hash=row["tx_hash"].strip().lower(),
                    tx_index=safe_int(row["tx_index"]),
                    block_number=safe_int(row["block_number"]),
                    block_time=row["block_time"].strip(),
                    evt_index=safe_int(row["evt_index"]),
                    token_address=row["token_address"].strip().lower(),
                    from_address=row["transfer_from"].strip().lower(),
                    to_address=row["transfer_to"].strip().lower(),
                    value=safe_float(row["transfer_value"]),
                    tx_from=row.get("tx_from", "").strip().lower(),
                    tx_to=row.get("tx_to", "").strip().lower(),
                ))
            except (ValueError, KeyError):
                continue
    return transfers


# ═════════════════════════════════════════════════════════════════════════════
# 1. ARBITRAGE DETECTION — Torres Section 3.1
# ═════════════════════════════════════════════════════════════════════════════
#
# Torres arbitrage.py L392-527 (Arbitrum version):
#
#   for tx_index in swaps:
#     if len(swaps[tx_index]) > 1:
#       # L396-399: OUTER pre-check — first/last token match AND amount check
#       if first.in_token == last.out_token and first.in_amount <= last.out_amount:
#         valid = True
#         intermediary_swaps = [first]
#         for i in 1..len:
#           prev = swaps[tx_index][i-1]    ← always from global list, NOT intermediary
#           curr = swaps[tx_index][i]
#           intermediary_swaps.append(curr)
#           L408: if prev.out_token != curr.in_token → valid = False
#           L410: if prev.out_amount < curr.in_amount → valid = False
#           L412: if prev.exchange == curr.exchange → valid = False
#           L414: if valid AND first.in_token == curr.out_token → record, reset intermediary
#         # valid never resets to True after going False — no mid-chain restart
#
# NOTE: Torres uses raw int amounts (wei). We use Dune float amounts which
#       introduces floating-point imprecision. This is a known data-source
#       limitation documented in the paper.
# ═════════════════════════════════════════════════════════════════════════════

def detect_arbitrages(swaps: List[Swap]) -> List[ArbitrageResult]:
    """Detect cyclic arbitrages. Faithful to Torres arbitrage.py L392-527."""
    tx_swaps: Dict[str, List[Swap]] = defaultdict(list)
    for s in swaps:
        tx_swaps[s.tx_hash].append(s)

    results = []

    for tx_hash, swap_list in tx_swaps.items():
        if len(swap_list) < 2:
            continue

        # Torres line 158: sort by logIndex
        swap_list.sort(key=lambda s: s.evt_index)

        # Issue #1/#3: Torres L396-399 — OUTER pre-check before entering loop
        # Check that the entire chain's first.in_token matches last.out_token
        # AND first.in_amount <= last.out_amount
        first_swap = swap_list[0]
        last_swap = swap_list[-1]
        if not tokens_equivalent(first_swap.in_token, last_swap.out_token):
            continue
        if first_swap.in_amount > last_swap.out_amount:
            continue

        valid = True
        intermediary: List[Swap] = [swap_list[0]]

        for i in range(1, len(swap_list)):
            # Issue #1: Torres L405-406 uses swaps[tx_index][i-1] from global
            # list, NOT from intermediary_swaps
            prev = swap_list[i - 1]
            curr = swap_list[i]
            intermediary.append(curr)

            # Torres L408: chain continuity (strict equality, no ETH/WETH fallback)
            if prev.out_token != curr.in_token:
                valid = False

            # Torres L410: no value leak
            if prev.out_amount < curr.in_amount:
                valid = False

            # Torres L412: different exchanges
            if prev.pool == curr.pool:
                valid = False

            # Torres L414: sub-cycle closure — checks against swaps[tx_index][0]
            # (first swap of the entire tx, NOT intermediary[0])
            # len >= 2: a cyclic arbitrage requires at least 2 swaps across
            # different DEXs. After sub-cycle reset intermediary=[], a single
            # append yields len==1 which cannot form a valid arbitrage.
            if valid and len(intermediary) >= 2 and tokens_equivalent(first_swap.in_token, curr.out_token):
                arb = _build_arb_result(tx_hash, intermediary)
                results.append(arb)
                # Torres L527: reset intermediary but continue loop
                # Issue #2: valid is NOT reset — stays True, no mid-chain restart
                intermediary = []

        # Issue #1: Torres does NOT restart from current swap when valid=False.
        # Once valid is False, no more sub-cycles can be found in this tx.

    return results


def _build_arb_result(tx_hash: str, chain: List[Swap]) -> ArbitrageResult:
    """Build ArbitrageResult with Torres-style token balance calculation."""
    token_balances: Dict[str, Dict] = {}

    for swap in chain:
        key_in = swap.in_token
        if key_in not in token_balances:
            price = (swap.amount_usd / swap.in_amount) if swap.in_amount > 0 else 0
            token_balances[key_in] = {"symbol": swap.in_symbol, "amount": 0.0, "price_usd": price}
        token_balances[key_in]["amount"] -= swap.in_amount

        key_out = swap.out_token
        if key_out not in token_balances:
            price = (swap.amount_usd / swap.out_amount) if swap.out_amount > 0 else 0
            token_balances[key_out] = {"symbol": swap.out_symbol, "amount": 0.0, "price_usd": price}
        token_balances[key_out]["amount"] += swap.out_amount

    cost_usd, gain_usd = 0.0, 0.0
    for info in token_balances.values():
        val = info["amount"] * info["price_usd"]
        if info["amount"] < 0:
            cost_usd += abs(val)
        elif info["amount"] > 0:
            gain_usd += val

    profit_usd = gain_usd - cost_usd

    # Gas cost estimate: gas_used * gas_price (in wei) → ETH → USD
    eth_price = chain[0].amount_usd / chain[0].in_amount if chain[0].in_amount > 0 else 0
    # Approximate ETH price from first swap if it involves WETH
    for s in chain:
        if s.in_token == WETH and s.in_amount > 0:
            eth_price = s.amount_usd / s.in_amount
            break
        if s.out_token == WETH and s.out_amount > 0:
            eth_price = s.amount_usd / s.out_amount
            break

    gas_cost_eth = (chain[0].gas_used * chain[0].gas_price) / 1e18 if chain[0].gas_price > 0 else 0
    gas_cost_usd = gas_cost_eth * eth_price

    path_tokens = [chain[0].in_symbol]
    for s in chain:
        path_tokens.append(s.out_symbol)
    swap_path = " → ".join(path_tokens)
    pools = " | ".join(f"{s.project}:{s.pool[:10]}" for s in chain)
    projects = ", ".join(s.project for s in chain)

    return ArbitrageResult(
        tx_hash=tx_hash,
        block_number=chain[0].block_number,
        block_time=chain[0].block_time,
        num_swaps=len(chain),
        swap_path=swap_path,
        pools=pools,
        projects=projects,
        profit_usd=profit_usd,
        token_balances={k: {"symbol": v["symbol"], "amount": v["amount"],
                            "usd": v["amount"] * v["price_usd"]}
                        for k, v in token_balances.items()},
        tx_from=chain[0].tx_from,
        gas_cost_usd=gas_cost_usd,
        swaps=list(chain),
    )


# ═════════════════════════════════════════════════════════════════════════════
# 2. SANDWICH DETECTION — Torres Section 3.3
# ═════════════════════════════════════════════════════════════════════════════
#
# Torres sandwiching.py (Arbitrum) operates on ERC-20 Transfer events.
# Key design: BLOCK_RANGE = 1 — each block is analyzed independently.
#
# Algorithm per block:
#   1. Index: transfer_to[token+recipient], transfer_from[from+to+txIndex]
#   2. Detect reversal pair: token sent FROM addr A who previously RECEIVED it
#   3. Validate: roles swap (from_a1==to_a2, from_a2==to_a1), ordering, value
#   4. Find victim tx between attacker tx1 & tx2 with same token flow (OR logic)
#   5. Torres L125-126: attacker EOA (tx_from) != victim EOA (tx_from)
#   6. Torres L128-132: tx_to filters (eliminate coincidental ordering)
#   7. Torres L192-203: validate attacker txs are actual swaps (bidirectional)
#
# NOTE: Torres L142-162 (RPC exchange verification via contract ABI calls)
#       cannot be reproduced with Dune CSV data. This is documented as a
#       known limitation — may produce slightly more false positives.
# ═════════════════════════════════════════════════════════════════════════════

def detect_sandwiches(transfers: List[Transfer]) -> List[SandwichResult]:
    """Detect sandwich attacks from ERC-20 Transfer events.
    Faithful to Torres sandwiching.py — per-block analysis (BLOCK_RANGE=1).
    """
    if not transfers:
        return []

    # Group by block (Torres: BLOCK_RANGE = 1, per-block analysis)
    block_transfers: Dict[int, List[Transfer]] = defaultdict(list)
    for t in transfers:
        block_transfers[t.block_number].append(t)

    results = []
    seen_pairs: Set[Tuple[str, str]] = set()

    for block_number in sorted(block_transfers.keys()):
        events = block_transfers[block_number]
        if len(events) < 3:
            continue

        # Sort by tx_index then evt_index within block
        events.sort(key=lambda t: (t.tx_index, t.evt_index))

        # Torres per-block data structures (reset each block)
        transfer_to: Dict[str, Transfer] = {}
        transfer_from: Dict[str, Tuple[str, int]] = {}
        asset_transfers: Dict[str, List[Transfer]] = defaultdict(list)
        victims: Set[str] = set()
        attackers: Set[str] = set()
        sandwiches_in_block = []

        for event in events:
            if event.value <= 0 or event.from_address == event.to_address:
                continue

            token = event.token_address
            _from = event.from_address
            _to = event.to_address

            # Torres L84-87: check for reversal (skip WETH)
            event_a1, event_a2 = None, None
            key_to = token + _from
            if token != WETH and key_to in transfer_to:
                event_a1 = transfer_to[key_to]
                event_a2 = event

            if event_a1 is not None and event_a2 is not None:
                _from_a1 = event_a1.from_address
                _to_a1   = event_a1.to_address
                _val_a1  = event_a1.value
                _from_a2 = event_a2.from_address
                _to_a2   = event_a2.to_address
                _val_a2  = event_a2.value

                # Torres L98: reversal validation
                if (_from_a1 == _to_a2 and _from_a2 == _to_a1 and
                        event_a1.tx_index < event_a2.tx_index and
                        _val_a1 >= _val_a2):

                    # Torres L100-111: search for victim
                    # Issue #9: OR inclusion — (_from_a1 == _from_w) OR (_to_a1 == _to_w)
                    event_w = None
                    if token in asset_transfers:
                        for at in asset_transfers[token]:
                            if (event_a1.tx_index < at.tx_index < event_a2.tx_index and
                                    at.tx_hash not in attackers):
                                if at.value > 0 and (_from_a1 == at.from_address or _to_a1 == at.to_address):
                                    event_w = at

                    if event_w is not None:
                        victims.add(event_w.tx_hash)

                        if (event_a1.tx_hash not in victims and
                                event_a2.tx_hash not in victims):

                            # Issue #7: Torres L125-126 — attacker EOA != victim EOA
                            # Uses tx_from (transaction originator), NOT transfer from/to
                            if (event_a1.tx_from != event_w.tx_from and
                                    event_a2.tx_from != event_w.tx_from):

                                # Issue #8: Torres L128-132 — tx_to filters
                                # L128: all three txs go to same contract but from
                                #        different EOAs → coincidental, not sandwich
                                skip = False
                                if (event_a1.tx_to and event_w.tx_to and event_a2.tx_to):
                                    if (event_a1.tx_to == event_w.tx_to == event_a2.tx_to and
                                            event_a1.tx_from != event_a2.tx_from):
                                        skip = True
                                    # L131: tx1 and victim go to same contract but
                                    #        tx2 goes elsewhere → not a real sandwich
                                    elif (event_a1.tx_to == event_w.tx_to and
                                            event_w.tx_to != event_a2.tx_to):
                                        skip = True

                                if not skip:
                                    # Collect for post-loop swap validation
                                    sandwiches_in_block.append((
                                        event_a1, event_a2, event_w,
                                        _from_a1, _to_a1, _from_a2, _to_a2,
                                        token
                                    ))

            # Update indexes (Torres L176-182)
            key_to_new = token + _to
            if key_to_new not in transfer_to:
                transfer_to[key_to_new] = event

            key_from = _from + _to + str(event.tx_index)
            if key_from not in transfer_from:
                transfer_from[key_from] = (token, event.evt_index)

            asset_transfers[token].append(event)

        # Torres L184-212: post-loop swap validation & deduplication
        for (event_a1, event_a2, event_w,
             _from_a1, _to_a1, _from_a2, _to_a2, token) in sandwiches_in_block:

            # Torres L192-203: check attacker txs are actual swaps
            # (bidirectional transfers in the same tx → real swap, not just transfer)
            key_fwd_a1 = _from_a1 + _to_a1 + str(event_a1.tx_index)
            key_rev_a1 = _to_a1 + _from_a1 + str(event_a1.tx_index)
            key_fwd_a2 = _from_a2 + _to_a2 + str(event_a2.tx_index)
            key_rev_a2 = _to_a2 + _from_a2 + str(event_a2.tx_index)

            if not (key_fwd_a1 in transfer_from and key_rev_a1 in transfer_from and
                    key_fwd_a2 in transfer_from and key_rev_a2 in transfer_from):
                continue

            # L194: same token both directions → not a swap
            if transfer_from[key_fwd_a1][0] == transfer_from[key_rev_a1][0]:
                continue
            if transfer_from[key_fwd_a2][0] == transfer_from[key_rev_a2][0]:
                continue
            # L200-203: attacker must use same exchange pair in both txs
            if transfer_from[key_fwd_a1][0] != transfer_from[key_fwd_a2][0]:
                continue
            if transfer_from[key_rev_a1][0] != transfer_from[key_rev_a2][0]:
                continue

            # Torres L205: deduplicate by attacker tx pair
            pair_key = (event_a1.tx_hash, event_a2.tx_hash)
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            attackers.add(event_a1.tx_hash)
            attackers.add(event_a2.tx_hash)

            results.append(SandwichResult(
                block_number=event_a1.block_number,
                block_time=event_a1.block_time,
                attacker_tx1_hash=event_a1.tx_hash,
                attacker_tx1_index=event_a1.tx_index,
                victim_tx_hashes=[event_w.tx_hash],
                victim_tx_indices=[event_w.tx_index],
                attacker_tx2_hash=event_a2.tx_hash,
                attacker_tx2_index=event_a2.tx_index,
                pool=_from_a1,
                token_address=token,
                attacker_address=event_a1.tx_from,
            ))

    return results


# ═════════════════════════════════════════════════════════════════════════════
# Statistics & Output
# ═════════════════════════════════════════════════════════════════════════════

def print_arb_stats(label: str, swaps: List[Swap], arbs: List[ArbitrageResult]):
    total_txs = len(set(s.tx_hash for s in swaps))
    total_blocks = len(set(s.block_number for s in swaps))
    block_range = (min(s.block_number for s in swaps), max(s.block_number for s in swaps))

    print(f"\n{'='*72}")
    print(f"  ARBITRAGE DETECTION: {label}")
    print(f"{'='*72}")
    print(f"  Block range:     {block_range[0]:,} – {block_range[1]:,} ({block_range[1]-block_range[0]:,} blocks)")
    print(f"  Time range:      {swaps[0].block_time[:19]} – {swaps[-1].block_time[:19]}")
    print(f"  Total swaps:     {len(swaps):,}")
    print(f"  Unique txs:      {total_txs:,}")
    print(f"  Unique blocks:   {total_blocks:,}")
    print(f"  Arbitrages:      {len(arbs):,}")

    if arbs:
        profits = [a.profit_usd for a in arbs]
        positive = [p for p in profits if p > 0]
        negative = [p for p in profits if p <= 0]
        print(f"    Profitable:    {len(positive):,}")
        print(f"    Unprofitable:  {len(negative):,}")
        if positive:
            print(f"    Total profit:  ${sum(positive):,.2f}")
            print(f"    Mean profit:   ${sum(positive)/len(positive):,.2f}")
            print(f"    Max profit:    ${max(positive):,.2f}")
            print(f"    Median profit: ${sorted(positive)[len(positive)//2]:,.2f}")

        # Gas cost
        total_gas = sum(a.gas_cost_usd for a in arbs)
        print(f"    Total gas cost:${total_gas:,.2f}")

        # Swap count distribution
        swap_counts = defaultdict(int)
        for a in arbs:
            swap_counts[a.num_swaps] += 1
        print(f"    By # swaps:    {dict(sorted(swap_counts.items()))}")

        # Top senders (MEV bots)
        bot_counts = defaultdict(int)
        for a in arbs:
            bot_counts[a.tx_from] += 1
        top_bots = sorted(bot_counts.items(), key=lambda x: -x[1])[:5]
        print(f"    Unique bots:   {len(bot_counts)}")
        print(f"    Top 5 bots:")
        for addr, cnt in top_bots:
            print(f"      {addr}: {cnt} arbs")

        # DEX distribution
        dex_counts = defaultdict(int)
        for a in arbs:
            for s in a.swaps:
                dex_counts[s.project] += 1
        print(f"    DEX usage:     {dict(sorted(dex_counts.items(), key=lambda x: -x[1]))}")

    print(f"{'='*72}\n")


def print_sandwich_stats(label: str, transfers: List[Transfer], sandwiches: List[SandwichResult]):
    total_txs = len(set(t.tx_hash for t in transfers))
    total_blocks = len(set(t.block_number for t in transfers))
    block_range = (min(t.block_number for t in transfers), max(t.block_number for t in transfers))

    print(f"\n{'='*72}")
    print(f"  SANDWICH DETECTION: {label}")
    print(f"{'='*72}")
    print(f"  Block range:     {block_range[0]:,} – {block_range[1]:,}")
    print(f"  Total transfers: {len(transfers):,}")
    print(f"  Unique txs:      {total_txs:,}")
    print(f"  Unique blocks:   {total_blocks:,}")
    print(f"  Sandwiches:      {len(sandwiches):,}")

    if sandwiches:
        unique_victims = set()
        for sw in sandwiches:
            unique_victims.update(sw.victim_tx_hashes)
        print(f"    Unique victims:{len(unique_victims):,}")

        attacker_counts = defaultdict(int)
        for sw in sandwiches:
            attacker_counts[sw.attacker_address] += 1
        top_attackers = sorted(attacker_counts.items(), key=lambda x: -x[1])[:5]
        print(f"    Unique attackers: {len(attacker_counts)}")
        print(f"    Top 5 attackers:")
        for addr, cnt in top_attackers:
            print(f"      {addr}: {cnt} sandwiches")

        # Token distribution
        token_counts = defaultdict(int)
        for sw in sandwiches:
            token_counts[sw.token_address] += 1
        top_tokens = sorted(token_counts.items(), key=lambda x: -x[1])[:5]
        print(f"    Top 5 tokens attacked:")
        for addr, cnt in top_tokens:
            print(f"      {addr[:20]}...: {cnt}")

    print(f"{'='*72}\n")


def save_arb_csv(output_dir: str, label: str, arbs: List[ArbitrageResult]):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"arbitrages_{label}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tx_hash", "block_number", "block_time", "tx_from",
                     "num_swaps", "swap_path", "pools", "projects",
                     "profit_usd", "gas_cost_usd", "net_profit_usd",
                     "token_balances"])
        for a in arbs:
            bal_str = "; ".join(f"{v['symbol']}:{v['amount']:+.8f}(${v['usd']:+.4f})"
                                for k, v in a.token_balances.items())
            w.writerow([a.tx_hash, a.block_number, a.block_time, a.tx_from,
                        a.num_swaps, a.swap_path, a.pools, a.projects,
                        f"{a.profit_usd:.6f}", f"{a.gas_cost_usd:.6f}",
                        f"{a.profit_usd - a.gas_cost_usd:.6f}", bal_str])
    print(f"  → Saved {len(arbs)} arbitrages to {path}")


def save_sandwich_csv(output_dir: str, label: str, sandwiches: List[SandwichResult]):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"sandwiches_{label}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["block_number", "block_time", "attacker_address",
                     "attacker_tx1_hash", "attacker_tx1_index",
                     "victim_tx_hashes", "victim_tx_indices",
                     "attacker_tx2_hash", "attacker_tx2_index",
                     "pool", "token_address"])
        for sw in sandwiches:
            w.writerow([sw.block_number, sw.block_time, sw.attacker_address,
                        sw.attacker_tx1_hash, sw.attacker_tx1_index,
                        "|".join(sw.victim_tx_hashes),
                        "|".join(str(i) for i in sw.victim_tx_indices),
                        sw.attacker_tx2_hash, sw.attacker_tx2_index,
                        sw.pool, sw.token_address])
    print(f"  → Saved {len(sandwiches)} sandwiches to {path}")


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "results")

    periods = [
        {
            "label": "pre_EIP4844",
            "swap_csv": os.path.join(base_dir, "q1_swaps_pre_eip4844.csv"),
            "transfer_csv": os.path.join(base_dir, "q2_transfers_pre_eip4844.csv"),
            "description": "Pre-EIP-4844 (Feb 2024, blocks 176351748–176379410)",
        },
        {
            "label": "post_EIP4844",
            "swap_csv": os.path.join(base_dir, "q1_swaps_post_eip4844.csv"),
            "transfer_csv": os.path.join(base_dir, "q2_transfers_post_eip4844.csv"),
            "description": "Post-EIP-4844 (Apr 2024, blocks 201152729–201167067)",
        },
    ]

    all_stats = []

    for period in periods:
        label = period["label"]
        print(f"\n{'#'*72}")
        print(f"  PERIOD: {period['description']}")
        print(f"{'#'*72}")

        # ── Arbitrage detection ──
        swap_path = period["swap_csv"]
        if swap_path and os.path.exists(swap_path):
            print(f"\n  Loading swaps from {os.path.basename(swap_path)}...")
            swaps = load_swaps(swap_path)
            print(f"  Loaded {len(swaps):,} swaps")

            if swaps:
                print(f"  Running arbitrage detection (Torres §3.1)...")
                arbs = detect_arbitrages(swaps)
                print_arb_stats(label, swaps, arbs)
                save_arb_csv(output_dir, label, arbs)
            else:
                print(f"  WARNING: No valid swaps loaded from {os.path.basename(swap_path)}")
                arbs = []
        else:
            print(f"  WARNING: No swap CSV found for {label}")
            swaps, arbs = [], []

        # ── Sandwich detection ──
        transfer_path = period["transfer_csv"]
        if transfer_path and os.path.exists(transfer_path):
            print(f"\n  Loading transfers from {os.path.basename(transfer_path)}...")
            transfers = load_transfers(transfer_path)
            print(f"  Loaded {len(transfers):,} transfers")

            if transfers:
                print(f"  Running sandwich detection (Torres §3.3, per-block)...")
                sandwiches = detect_sandwiches(transfers)
                print_sandwich_stats(label, transfers, sandwiches)
                save_sandwich_csv(output_dir, label, sandwiches)
            else:
                print(f"  WARNING: No valid transfers loaded")
                sandwiches = []
        else:
            print(f"  WARNING: No transfer CSV found for {label}")
            transfers, sandwiches = [], []

        # Collect stats for comparison
        stats = {
            "label": label,
            "description": period["description"],
            "total_swaps": len(swaps),
            "total_transfers": len(transfers),
            "arbitrages_found": len(arbs),
            "arb_profitable": len([a for a in arbs if a.profit_usd > 0]),
            "arb_total_profit_usd": sum(a.profit_usd for a in arbs if a.profit_usd > 0),
            "arb_total_gas_usd": sum(a.gas_cost_usd for a in arbs),
            "sandwiches_found": len(sandwiches),
        }
        all_stats.append(stats)

    # ── Cross-period comparison ──
    if len(all_stats) == 2:
        pre, post = all_stats[0], all_stats[1]

        print(f"\n{'='*72}")
        print(f"  CROSS-PERIOD COMPARISON: Pre vs Post EIP-4844")
        print(f"{'='*72}")
        print(f"{'Metric':<30} {'Pre-4844':>20} {'Post-4844':>20}")
        print(f"{'-'*70}")

        metrics = [
            ("Total swaps",        "total_swaps",          "d"),
            ("Total transfers",    "total_transfers",      "d"),
            ("Arbitrages found",   "arbitrages_found",     "d"),
            ("Profitable arbs",    "arb_profitable",       "d"),
            ("Total arb profit",   "arb_total_profit_usd", ".2f"),
            ("Total gas cost",     "arb_total_gas_usd",    ".2f"),
            ("Sandwiches found",   "sandwiches_found",     "d"),
        ]

        for name, key, fmt in metrics:
            v_pre = pre[key]
            v_post = post[key]
            if fmt == "d":
                print(f"{name:<30} {v_pre:>20,} {v_post:>20,}")
            else:
                print(f"{name:<30} {'$'+format(v_pre, fmt):>20} {'$'+format(v_post, fmt):>20}")

        print(f"{'='*72}")

        # Change analysis
        if pre["arbitrages_found"] > 0:
            arb_change = ((post["arbitrages_found"] - pre["arbitrages_found"])
                          / pre["arbitrages_found"] * 100)
            print(f"\n  Arbitrage count change: {arb_change:+.1f}%")
        if pre["sandwiches_found"] > 0:
            sw_change = ((post["sandwiches_found"] - pre["sandwiches_found"])
                         / pre["sandwiches_found"] * 100)
            print(f"  Sandwich count change:  {sw_change:+.1f}%")

    # Save summary
    summary_path = os.path.join(output_dir, "baseline_summary.json")
    os.makedirs(output_dir, exist_ok=True)
    with open(summary_path, "w") as f:
        json.dump(all_stats, f, indent=2)
    print(f"\n  Summary saved to {summary_path}")


if __name__ == "__main__":
    main()
