# l2-mev-evaluation

Faithful reproduction of Torres et al.'s MEV detection heuristics from [Rolling in the Shadows](https://arxiv.org/abs/2405.00138) (ACM CCS 2024), adapted from archive-node infrastructure to Dune Analytics CSV exports, applied to Arbitrum pre/post EIP-4844.

## Results

| Metric | Pre-EIP-4844 (Feb 2024) | Post-EIP-4844 (Apr 2024) |
|--------|:-----------------------:|:------------------------:|
| Swaps analyzed | 10,843 | 39,754 |
| Transfers analyzed | 83,686 | 150,000 |
| **Arbitrages detected** | **136** | **658** (+383%) |
| **Sandwiches detected** | **0** | **0** |
| Mean arb profit (token-level) | $6.81 | $0.97 |
| Median arb profit | $1.17 | $0.03 |
| Mean gas cost / arb | $0.17 | $0.012 (-93%) |

**Sandwich = 0** is consistent with Torres et al.'s finding of zero sandwiches across all rollups over 32 months (Table 8 in the paper). Arbitrum's private sequencer with FCFS ordering prevents traditional sandwich attacks.

## What's Implemented

The baseline reproduces **14 algorithmic checks** from Torres's original code, verified line-by-line against the source. See [`baseline_vs_torres_comparison.md`](Data/baseline_vs_torres_comparison.md) for the full audit.

### Arbitrage Detection (Torres §3.1, `arbitrage.py` L392-527)

- Outer pre-check: first/last token match (ETH/WETH equivalence) + amount constraint
- Sequential chain validation: strict token continuity, no value leak, different exchanges
- Sub-cycle detection with intermediary reset, `valid` never resets, `len>=2` guard

### Sandwich Detection (Torres §3.3, `sandwiching.py` L36-212)

- Per-block analysis (`BLOCK_RANGE = 1`)
- Reversal pair detection + victim search (OR-inclusion logic)
- `tx.from` EOA inequality check (L125-126)
- `tx.to` triple filter (L128-132)
- Bidirectional swap validation (L192-203)

### Not Implemented

| Feature | Reason |
|---------|--------|
| Flash loan detection (Aave/Radiant/Balancer) | Requires FlashLoan event data (not in current Dune queries) |
| RPC exchange contract verification | Requires archive node; Dune CSV cannot call contract ABIs |
| Sandwich profit calculation | 0 sandwiches detected; no output impact |

## Repository Structure

```
├── Data/
│   ├── baseline_detection.py            # Core detection script
│   ├── baseline_vs_torres_comparison.md # Line-by-line comparison with Torres
│   ├── fatch_data.py                    # Dune API data fetcher
│   ├── q1_swaps_pre_eip4844.csv         # Dune dex.trades exports
│   ├── q1_swaps_post_eip4844.csv
│   ├── q2_transfers_pre_eip4844.csv     # Dune erc20 transfer exports
│   ├── q2_transfers_post_eip4844.csv
│   └── results/                         # Detection outputs
├── Rolling-in-the-Shadows/              # Torres et al. original source (reference)
└── 2405.00138v3.pdf                     # Torres et al. paper
```

## Usage

```bash
# 1. Fetch data (requires DUNE_API_KEY env var)
cd Data
python fatch_data.py

# 2. Run detection
python baseline_detection.py
```

## Data Source

All data from [Dune Analytics](https://dune.com/) (`dex.trades` + `erc20.evt_Transfer` on Arbitrum).

| Period | Blocks | Time Window |
|--------|--------|-------------|
| Pre-EIP-4844 | 176,351,748 – 176,379,410 | Feb 1, 2024 10:00–12:00 UTC |
| Post-EIP-4844 | 201,408,918 – 201,437,404 | Apr 15, 2024 22:00–00:00 UTC |

## References

- Torres, C.F. et al. (2024). *Rolling in the Shadows: Analyzing the Extraction of MEV Across Layer-2 Rollups.* ACM CCS 2024. [arXiv:2405.00138](https://arxiv.org/abs/2405.00138)
- [EIP-4844: Shard Blob Transactions](https://eips.ethereum.org/EIPS/eip-4844)
- [Torres et al. source code](https://github.com/AnomalousIdentity/Rolling-in-the-Shadows)
