#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dune Analytics CSV downloader.
Paginated fetch + incremental flush + resume + error recovery.
"""

import os
import time
import requests
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("DUNE_API_KEY", "uW88iU8NkdSZ00ZpiAEcNBTD1uh17ANz")
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

BATCH = 10_000
SAVE_EVERY = 5          # flush every N batches (50K rows)
RATE_LIMIT_WAIT = 30
REQUEST_INTERVAL = 1

STEPS = [
    {
        "name": "q2_transfers_pre_eip4844",
        "query_id": 6797914,
        "description": "STEP 1: Pre-EIP-4844 Transfer (block 176351748-176379410)",
    },
    {
        "name": "q1_swaps_pre_eip4844",
        "query_id": 6797946,
        "description": "STEP 2: Pre-EIP-4844 Swap (same block range)",
    },
    {
        "name": "q2_transfers_post_eip4844",
        "query_id": 6797983,
        "description": "STEP 3: Post-EIP-4844 Transfer (block 201152729-201167067)",
    },
    {
        "name": "q1_swaps_post_eip4844",
        "query_id": 6797981,
        "description": "STEP 4: Post-EIP-4844 Swap (same block range)",
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def flush_to_disk(rows, csv_path, append):
    """Write buffer to CSV, return row count."""
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    df.to_csv(csv_path, mode="a" if append else "w",
              header=not append, index=False)
    return len(df)


def count_existing_rows(csv_path):
    """Count rows in existing CSV (excluding header) for resume offset."""
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) < 10:
        return 0
    with open(csv_path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f) - 1


# ── Core download ─────────────────────────────────────────────────────────────

def download_step(query_id, csv_path):
    """Paginated fetch with incremental flush. Returns total row count."""
    headers = {"X-Dune-API-Key": API_KEY}

    existing = count_existing_rows(csv_path)
    offset = existing
    append = existing > 0
    if existing > 0:
        print(f"    Resuming: {existing:,} rows exist, offset={offset}")

    buffer = []
    total_saved = existing
    batches_since_save = 0

    try:
        while True:
            url = (f"https://api.dune.com/api/v1/query/{query_id}/results"
                   f"?limit={BATCH}&offset={offset}")
            resp = requests.get(url, headers=headers)

            if resp.status_code == 402:
                print(f"    Credits exhausted, flushing buffer ({len(buffer)} rows)...")
                break

            if resp.status_code == 429:
                print(f"    Rate limited, waiting {RATE_LIMIT_WAIT}s...")
                time.sleep(RATE_LIMIT_WAIT)
                continue

            resp.raise_for_status()
            batch = resp.json()["result"]["rows"]

            if not batch:
                break

            buffer.extend(batch)
            offset += BATCH
            batches_since_save += 1
            print(f"    Fetched: {total_saved + len(buffer):,} rows (buffer: {len(buffer):,})")

            if batches_since_save >= SAVE_EVERY:
                n = flush_to_disk(buffer, csv_path, append)
                total_saved += n
                append = True
                buffer.clear()
                batches_since_save = 0
                print(f"    -> flushed {n:,} rows (total: {total_saved:,})")

            time.sleep(REQUEST_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n    Interrupted! Flushing buffer ({len(buffer)} rows)...")
    except Exception as e:
        print(f"    Error: {e}, flushing buffer ({len(buffer)} rows)...")

    if buffer:
        n = flush_to_disk(buffer, csv_path, append)
        total_saved += n

    return total_saved


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Dune CSV Downloader")
    print("=" * 60)

    results = {}
    for i, step in enumerate(STEPS):
        name = step["name"]
        query_id = step["query_id"]
        csv_path = os.path.join(OUTPUT_DIR, f"{name}.csv")

        print(f"\n[STEP {i+1}/{len(STEPS)}] {step['description']}")

        if query_id is None:
            print("  [SKIP] query_id not set")
            results[name] = 0
            continue

        total = download_step(query_id, csv_path)
        results[name] = total
        if total > 0:
            print(f"  Done: {name}.csv ({total:,} rows)")

    # Summary
    print("\n" + "=" * 60)
    print("  Summary")
    print("=" * 60)
    for name, count in results.items():
        csv_path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        if count > 0 and os.path.exists(csv_path):
            size = os.path.getsize(csv_path)
            print(f"  {name:40s} {count:>8,} rows  ({size:,} bytes)")
        else:
            print(f"  {name:40s} -")

    skipped = [n for n, c in results.items() if c == 0]
    if skipped:
        print(f"\nIncomplete: {', '.join(skipped)}")
        print("  Set query_id and re-run. Completed steps will resume automatically.")


if __name__ == "__main__":
    main()
