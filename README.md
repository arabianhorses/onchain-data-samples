# Onchain data samples

Selected BigQuery SQL and Python from **LP Scanner**, a pipeline I built to measure what liquidity providers earn on concentrated-liquidity DEXs (Uniswap v3/v4 and their forks, on Ethereum and Avalanche). It runs on raw event logs from Google's public BigQuery datasets, decodes them, and turns them into per-pool, per-tick volume, fee-yield and position data.

These are working excerpts from a larger private project, shared to show how I write data pipelines. They reference my own BigQuery dataset (`lpscanner.*`) and a few helper functions in it (e.g. `hex_to_int_str` for two's-complement int256 decoding), so they won't run as-is.

Related public work: [Paribu's users, seen from the chain](https://decentralizedanalysis.com/dashboards/paribu/article/), an analysis of an exchange's users built from public Ethereum data ([dashboard](https://decentralizedanalysis.com/dashboards/paribu/)).

## BigQuery: incremental Uniswap v3 pipeline (`bigquery/uniswap_v3/`)

Scheduled queries that keep the tables up to date at low cost. Each run either resumes from the newest row it already has, or re-scans only a short recent window, so no run re-reads history.

| File | What it does |
|---|---|
| `01_target_logs_incremental.sql` | Pulls raw event logs for the protocol contracts and every known pool from `goog_blockchain_ethereum_mainnet_us.logs` into one narrow table. It uses a monotonic `block_number * 1e6 + log_index` row id as the resume cursor, and a two-step look-back (48 h, then the month) to keep partition scans small. |
| `02_known_pool_addresses_incremental.sql` | Discovers new pools from factory `PoolCreated` events for Uniswap v3, SushiSwap v3 and PancakeSwap v3, and feeds them back into step 01. |
| `03_pool_creations_incremental.sql` | Decodes pool metadata (tokens, fee tier, tick spacing) from `PoolCreated` topics and data. |
| `04_swaps_and_liquidity_movements_incremental.sql` | Decodes `Swap`, `Mint` and `Burn` events directly from 32-byte ABI words: signed int256 amounts, `sqrtPriceX96`, ticks and liquidity deltas. Burns are stored as negative deltas, so liquidity can be summed per position. |
| `05_lp_nft_transfers_incremental.sql` | Tracks the current owner of every LP position NFT from ERC-721 `Transfer` events, across the position managers of four protocols (Uniswap v3 and v4, Pangolin v3, Blackhole). |
| `06_hourly_price_feed_incremental.sql` | Builds hourly USD prices for every token from onchain swaps alone, across Ethereum and Avalanche and four protocols (Uniswap v3 and v4, Pangolin, Blackhole). It works in two hops: tokens traded against a dollar-pegged token are priced first, then other tokens are priced through those. Each run scans 6 hours with a 2-hour overlap, so it's safe to repeat. |
| `07_daily_swap_volume_incremental.sql` | Daily USD volume and fees per pool across both chains, including dynamic-fee pools: fee-change events become validity intervals (`LEAD` over a monotonic event id), and each swap is matched to the fee in force at that moment. |

## Python: Uniswap v3 pool processor (`python/uniswapv3_processor.py`)

Replays each pool's full swap and liquidity history to rebuild its state, and outputs per-pool JSON for the LP Scanner frontend:

- **Exact tick math.** It uses Q64.96 `sqrtPrice` math with 50-digit `Decimal` precision to convert liquidity between token amounts and USD within any tick range.
- **Per-tick volume and fees.** Each swap's volume and fees are spread across the tick ranges it crosses, giving 1-day, 7-day and 30-day volume per tick.
- **Hourly fee yield per tick.** Fee earned per dollar of active liquidity, hour by hour, for estimating what a hypothetical LP position would have earned.
- **Position reconstruction.** It rebuilds open positions from mints and burns, checks the replayed liquidity against the onchain value after every swap, and flags mismatches.
- **Fork-agnostic.** The same code handles Uniswap v3, SushiSwap v3 and PancakeSwap v3 on any EVM chain.

Inputs are the parquet exports of tables 03 and 04, plus prices from 06. Outputs are optionally uploaded to Cloudflare R2; credentials are read from environment variables.

## Stack

Google BigQuery (scheduled scripting, `MERGE`, partition pruning) · Python (pandas, numpy, `decimal`) · parquet · Cloudflare R2
