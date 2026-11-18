"""
Uniswap V3 Pool Data Processor
Reads from parquet files (swaps, liq movements, pool creations)
Produces per-pool JSON with: time_frame_volume (1d/7d/30d USD per tick), hourly_fee_yield, positions
Stateless — builds everything from scratch each run, no incremental resume
Protocol-agnostic on chain — works for any chain with V3 data
Also handles identical V3 forks (SushiSwap V3, PancakeSwap V3)
"""

import pandas as pd
import numpy as np
import json
import os
import time
import glob
from datetime import datetime, timezone, timedelta
from decimal import Decimal, getcontext
getcontext().prec = 50

Q96 = 2 ** 96
WINDOW_DAYS = 30


## ═══════════════════════════════════════════════════════════════
## TICK MATH
## ═══════════════════════════════════════════════════════════════

def tick_to_sqrtp_x96(tick):
    return int(Decimal('1.0001') ** (Decimal(tick) / Decimal(2)) * Decimal(2 ** 96))


def align_tick(tick, tick_spacing):
    return (tick // tick_spacing) * tick_spacing


def calc_amount0(liquidity, sqrtp_a, sqrtp_b):
    """token0 delta = L * Q96 * |sqrtPb - sqrtPa| / (sqrtPa * sqrtPb)"""
    if sqrtp_a > sqrtp_b:
        sqrtp_a, sqrtp_b = sqrtp_b, sqrtp_a
    if sqrtp_a == 0 or sqrtp_b == 0 or liquidity == 0:
        return 0
    return (liquidity * Q96 * (sqrtp_b - sqrtp_a)) // (sqrtp_a * sqrtp_b)


def calc_amount1(liquidity, sqrtp_a, sqrtp_b):
    """token1 delta = L * |sqrtPb - sqrtPa| / Q96"""
    if sqrtp_a > sqrtp_b:
        sqrtp_a, sqrtp_b = sqrtp_b, sqrtp_a
    if liquidity == 0:
        return 0
    return liquidity * (sqrtp_b - sqrtp_a) // Q96


def liq_to_usd_at_tick(liquidity, tick_lower, tick_upper, at_tick,
                        token0_decimals, token1_decimals, token0_price, token1_price):
    """Convert raw liquidity L to USD value evaluated at a specific tick."""
    sqrtp_at = tick_to_sqrtp_x96(at_tick)
    sqrtp_lower = tick_to_sqrtp_x96(tick_lower)
    sqrtp_upper = tick_to_sqrtp_x96(tick_upper)
    if at_tick < tick_lower:
        amount0 = calc_amount0(liquidity, sqrtp_lower, sqrtp_upper)
        amount1 = 0
    elif at_tick >= tick_upper:
        amount0 = 0
        amount1 = calc_amount1(liquidity, sqrtp_lower, sqrtp_upper)
    else:
        amount0 = calc_amount0(liquidity, sqrtp_at, sqrtp_upper)
        amount1 = calc_amount1(liquidity, sqrtp_lower, sqrtp_at)
    return (amount0 / 10**token0_decimals) * token0_price + \
           (amount1 / 10**token1_decimals) * token1_price


## ═══════════════════════════════════════════════════════════════
## V3 INPUT TOKEN DETECTION
## ═══════════════════════════════════════════════════════════════

def determine_input_token_v3(amount0, amount1):
    """V3: amount0 > 0 = token0 entering pool (input).
    Returns: (input_token_index, input_amount, output_amount)"""
    if amount0 > 0:
        return 0, abs(amount0), abs(amount1)
    else:
        return 1, abs(amount1), abs(amount0)


## ═══════════════════════════════════════════════════════════════
## PARQUET HELPERS
## ═══════════════════════════════════════════════════════════════

def find_latest_parquet(output_dir, chain_protocol, event_type):
    pattern = os.path.join(output_dir, f'{chain_protocol}_{event_type}_*.parquet')
    files = sorted(glob.glob(pattern))
    if not files:
        return None
    return files[-1]


## ═══════════════════════════════════════════════════════════════
## POOL PROCESSOR
## ═══════════════════════════════════════════════════════════════

def process_pool(pool_address, swaps_df, liq_movements_df, tick_spacing, fee_tier,
                 token0_decimals, token1_decimals, token0_price, token1_price,
                 chain, platform, token0_address, token1_address,
                 token0_symbol, token1_symbol,
                 token0_anchor=None, token1_anchor=None):
    """
    Process a single V3 pool from parquet data.
    Returns dict with all pool data ready for JSON serialization.
    """
    chain = chain.replace('_c', '')  ## avalanche_c → avalanche
    t_start = time.time()

    pool_swaps = swaps_df[swaps_df['pool_address'] == pool_address].copy()
    pool_swaps['tick'] = pool_swaps['tick'].apply(int)
    pool_swaps = pool_swaps.sort_values('generatedIndex')

    pool_liq = liq_movements_df[liq_movements_df['pool_address'] == pool_address].copy()
    pool_liq['tickLower'] = pool_liq['tickLower'].apply(int)
    pool_liq['tickUpper'] = pool_liq['tickUpper'].apply(int)

    if len(pool_swaps) < 2:
        print(f"    {pool_address[:16]}... — not enough swaps ({len(pool_swaps)}), skip")
        return None

    ## ── Fee rate from pool creation (fixed per pool in V3) ──
    fee_rate = fee_tier / 1_000_000

    ## ── Tick range from swaps ──
    min_tick_aligned = align_tick(pool_swaps['tick'].min(), tick_spacing)
    max_tick_aligned = align_tick(pool_swaps['tick'].max(), tick_spacing) + tick_spacing
    min_gi_from_swaps = pool_swaps['generatedIndex'].min()

    ## ── Build initial tick_liquidity from pre-window liq mods ──
    initial_liq_mods = pool_liq[
        (pool_liq['generatedIndex'] < min_gi_from_swaps) &
        (pool_liq['tickLower'] <= max_tick_aligned) &
        (pool_liq['tickUpper'] > min_tick_aligned)
    ][['tickLower', 'tickUpper', 'liquidityDelta']]

    aligned_ticks = range(min_tick_aligned, max_tick_aligned + tick_spacing, tick_spacing)
    tick_liquidity = {}
    for tick in aligned_ticks:
        mask = (initial_liq_mods['tickLower'] <= tick) & (initial_liq_mods['tickUpper'] > tick)
        tick_liquidity[tick] = sum(int(ld) for ld in initial_liq_mods.loc[mask, 'liquidityDelta'])

    ## ── Build events to process (swaps + liq mods interleaved by GI) ──
    swaps_to_process = pool_swaps.copy()
    swaps_to_process['event_type'] = 'swap'
    swaps_to_process['startTick'] = swaps_to_process['tick'].shift(1)
    swaps_to_process['startSqrtPriceX96'] = swaps_to_process['sqrtPriceX96'].shift(1)
    swaps_to_process = swaps_to_process.iloc[1:]
    swaps_to_process = swaps_to_process.rename(columns={'tick': 'endTick', 'sqrtPriceX96': 'endSqrtPriceX96'})

    liq_to_process = pool_liq[pool_liq['generatedIndex'] > min_gi_from_swaps][
        ['tickLower', 'tickUpper', 'liquidityDelta', 'generatedIndex']
    ].copy()
    liq_to_process['event_type'] = 'liq_mod'

    events = pd.concat([swaps_to_process, liq_to_process], ignore_index=True)
    events = events.sort_values('generatedIndex').reset_index(drop=True)

    print(f"    {pool_address}... swaps={len(swaps_to_process)} liq_mods={len(liq_to_process)} initial_liq={len(initial_liq_mods)}")

    ## ── Replay events ──
    hourly_volume = {}
    hourly_fee_yield = {}

    liq_matches = 0
    liq_mismatches = 0
    output_flags = 0
    single_tick_count = 0
    multi_tick_count = 0
    liq_mod_count = 0

    ## Price tracking for metadata
    now_utc = datetime.now(timezone.utc)
    ago_targets = {
        '1d': now_utc - timedelta(days=1),
        '7d': now_utc - timedelta(days=7),
        '30d': now_utc - timedelta(days=30),
    }
    ago_swaps = {}
    ago_swap_deltas = {'1d': None, '7d': None, '30d': None}
    latest_swap = None
    daily_tick_buckets = {}

    for _, event in events.iterrows():
        if event['event_type'] == 'liq_mod':
            liq_mod_count += 1
            tickLower = int(event['tickLower'])
            tickUpper = int(event['tickUpper'])
            delta = int(event['liquidityDelta'])
            for tick in range(tickLower, tickUpper, tick_spacing):
                if tick in tick_liquidity:
                    tick_liquidity[tick] += delta

        elif event['event_type'] == 'swap':
            start_tick = int(event['startTick'])
            end_tick = int(event['endTick'])
            start_sqrtP = int(event['startSqrtPriceX96'])
            end_sqrtP = int(event['endSqrtPriceX96'])
            onchain_liq = int(event['liquidity'])
            amount0 = int(event['amount0'])
            amount1 = int(event['amount1'])
            gi = int(event['generatedIndex'])

            swap_time = pd.to_datetime(event.get('block_timestamp'), utc=True)
            hour_key = swap_time.strftime('%Y-%m-%d %H')

            start_aligned = align_tick(start_tick, tick_spacing)
            end_aligned = align_tick(end_tick, tick_spacing)

            input_token, input_amount, output_amount = determine_input_token_v3(amount0, amount1)

            ## ── Swap detail for metadata tracking ──
            swap_detail = {
                'tx': str(event.get('transaction_hash', '')),
                'tick': end_tick,
                'timestamp': swap_time.isoformat(),
                'amount0': str(amount0),
                'amount1': str(amount1),
                'sqrtPriceX96': str(end_sqrtP),
                'liquidity': str(onchain_liq),
                'log_index': int(event['log_index']),
            }
            latest_swap = swap_detail

            ## Ago swaps — closest swap to each target timestamp
            for tf, target in ago_targets.items():
                delta = abs((swap_time - target).total_seconds())
                if ago_swap_deltas[tf] is None or delta < ago_swap_deltas[tf]:
                    ago_swap_deltas[tf] = delta
                    ago_swaps[tf] = swap_detail

            ## Daily tick buckets — track min/max tick per day
            date_key = swap_time.strftime('%Y-%m-%d')
            if date_key not in daily_tick_buckets:
                daily_tick_buckets[date_key] = {
                    'tick_min': end_tick, 'tick_max': end_tick,
                    'min_swap': swap_detail, 'max_swap': swap_detail,
                    'swap_count': 0,
                }
            bucket = daily_tick_buckets[date_key]
            bucket['swap_count'] += 1
            if end_tick < bucket['tick_min']:
                bucket['tick_min'] = end_tick
                bucket['min_swap'] = swap_detail
            if end_tick > bucket['tick_max']:
                bucket['tick_max'] = end_tick
                bucket['max_swap'] = swap_detail

            ## ── Single-tick swap ──
            if start_aligned == end_aligned:
                single_tick_count += 1
                tracked_liq = tick_liquidity.get(start_aligned, 0)

                if tracked_liq == onchain_liq:
                    liq_matches += 1
                else:
                    liq_mismatches += 1

                ## Hourly volume + fees
                tick_key = str(start_aligned)
                fee_amount = int(input_amount * fee_rate)
                if hour_key not in hourly_volume:
                    hourly_volume[hour_key] = {}
                if tick_key not in hourly_volume[hour_key]:
                    hourly_volume[hour_key][tick_key] = {'input_vol0': 0, 'input_vol1': 0, 'fees_token0': 0, 'fees_token1': 0}
                if input_token == 0:
                    hourly_volume[hour_key][tick_key]['input_vol0'] += input_amount
                    hourly_volume[hour_key][tick_key]['fees_token0'] += fee_amount
                else:
                    hourly_volume[hour_key][tick_key]['input_vol1'] += input_amount
                    hourly_volume[hour_key][tick_key]['fees_token1'] += fee_amount

                ## Output deviation check (single-tick)
                onchain_input_after_fee = int(input_amount * (1_000_000 - fee_tier) / 1_000_000)
                if end_sqrtP > start_sqrtP:
                    calc_input_after_fee = abs(tracked_liq * (end_sqrtP - start_sqrtP) // Q96)
                else:
                    calc_input_after_fee = abs(tracked_liq * Q96 * (start_sqrtP - end_sqrtP) // (start_sqrtP * end_sqrtP))
                diff = abs(calc_input_after_fee - onchain_input_after_fee)
                if onchain_input_after_fee > 0 and diff > 9999 and diff / onchain_input_after_fee > 0.00001:
                    output_flags += 1

                ## Fee yield (single-tick)
                if fee_rate > 0 and tracked_liq > 0:
                    if input_token == 0:
                        fee_usd = (input_amount * fee_rate / 10**token0_decimals) * token0_price
                    else:
                        fee_usd = (input_amount * fee_rate / 10**token1_decimals) * token1_price

                    sqrt_lower = 1.0001 ** (start_aligned / 2.0)
                    sqrt_upper = 1.0001 ** ((start_aligned + tick_spacing) / 2.0)
                    sqrt_mid = (sqrt_lower + sqrt_upper) / 2.0
                    amt0_per_L = (1.0 / sqrt_mid) - (1.0 / sqrt_upper)
                    amt1_per_L = sqrt_mid - sqrt_lower
                    usd_per_L = (amt0_per_L / 10**token0_decimals) * token0_price + \
                                (amt1_per_L / 10**token1_decimals) * token1_price
                    total_liq_usd = tracked_liq * usd_per_L

                    if total_liq_usd > 0:
                        tick_fy_key = str(start_aligned)
                        if hour_key not in hourly_fee_yield:
                            hourly_fee_yield[hour_key] = {}
                        if tick_fy_key not in hourly_fee_yield[hour_key]:
                            hourly_fee_yield[hour_key][tick_fy_key] = Decimal('0')
                        hourly_fee_yield[hour_key][tick_fy_key] += Decimal(str(fee_usd)) / Decimal(str(total_liq_usd))

            ## ── Multi-tick swap ──
            else:
                multi_tick_count += 1
                end_tracked_liq = tick_liquidity.get(end_aligned, 0)
                if end_tracked_liq == onchain_liq:
                    liq_matches += 1
                else:
                    liq_mismatches += 1

                price_increasing = end_sqrtP > start_sqrtP

                ## Walk tick ranges and compute per-range volumes
                range_results = []
                total_calc_input = 0

                if price_increasing:
                    walk_tick = start_aligned
                    seg_start = start_sqrtP
                    while walk_tick <= end_aligned:
                        L = tick_liquidity.get(walk_tick, 0)
                        seg_end = end_sqrtP if walk_tick == end_aligned else tick_to_sqrtp_x96(walk_tick + tick_spacing)
                        if L > 0 and seg_end > seg_start:
                            amt0 = calc_amount0(L, seg_start, seg_end)
                            amt1 = calc_amount1(L, seg_start, seg_end)
                            calc_input = amt1 if input_token == 1 else amt0
                            range_results.append({'tick': walk_tick, 'calc_input': calc_input, 'liq': L, 'seg_start': seg_start, 'seg_end': seg_end})
                            total_calc_input += calc_input
                        walk_tick += tick_spacing
                        seg_start = seg_end
                else:
                    walk_tick = start_aligned
                    seg_start = start_sqrtP
                    while walk_tick >= end_aligned:
                        L = tick_liquidity.get(walk_tick, 0)
                        seg_end = end_sqrtP if walk_tick == end_aligned else tick_to_sqrtp_x96(walk_tick)
                        if L > 0 and seg_start > seg_end:
                            amt0 = calc_amount0(L, seg_end, seg_start)
                            amt1 = calc_amount1(L, seg_end, seg_start)
                            calc_input = amt0 if input_token == 0 else amt1
                            range_results.append({'tick': walk_tick, 'calc_input': calc_input, 'liq': L, 'seg_start': seg_start, 'seg_end': seg_end})
                            total_calc_input += calc_input
                        walk_tick -= tick_spacing
                        seg_start = seg_end

                ## Output deviation check (multi-tick)
                onchain_input_after_fee = int(input_amount * (1_000_000 - fee_tier) / 1_000_000)
                if onchain_input_after_fee > 0:
                    diff = abs(total_calc_input - onchain_input_after_fee)
                    if diff > 9999 and diff / onchain_input_after_fee > 0.00001:
                        output_flags += 1

                ## Compute fee_usd for the whole swap
                if fee_rate > 0:
                    if input_token == 0:
                        fee_usd = (input_amount * fee_rate / 10**token0_decimals) * token0_price
                    else:
                        fee_usd = (input_amount * fee_rate / 10**token1_decimals) * token1_price
                else:
                    fee_usd = 0

                ## Distribute volume + fee yield per range
                for r in range_results:
                    if total_calc_input > 0:
                        range_share = r['calc_input'] / total_calc_input
                    else:
                        range_share = 1.0 / len(range_results) if range_results else 0

                    range_tick = r['tick']
                    tick_key = str(range_tick)

                    ## Hourly volume + fees
                    range_input = int(input_amount * range_share)
                    range_fee = int(input_amount * range_share * fee_rate)
                    if hour_key not in hourly_volume:
                        hourly_volume[hour_key] = {}
                    if tick_key not in hourly_volume[hour_key]:
                        hourly_volume[hour_key][tick_key] = {'input_vol0': 0, 'input_vol1': 0, 'fees_token0': 0, 'fees_token1': 0}
                    if input_token == 0:
                        hourly_volume[hour_key][tick_key]['input_vol0'] += range_input
                        hourly_volume[hour_key][tick_key]['fees_token0'] += range_fee
                    else:
                        hourly_volume[hour_key][tick_key]['input_vol1'] += range_input
                        hourly_volume[hour_key][tick_key]['fees_token1'] += range_fee

                    ## Fee yield (multi-tick)
                    if fee_usd > 0 and r['liq'] > 0:
                        range_fee_usd = fee_usd * range_share
                        range_lower_sqrt = tick_to_sqrtp_x96(r['tick']) / (2**96)
                        range_upper_sqrt = tick_to_sqrtp_x96(r['tick'] + tick_spacing) / (2**96)
                        mid_sqrt = (range_lower_sqrt + range_upper_sqrt) / 2.0
                        amt0_per_L = (1.0 / mid_sqrt) - (1.0 / range_upper_sqrt)
                        amt1_per_L = mid_sqrt - range_lower_sqrt
                        usd_per_L = (amt0_per_L / 10**token0_decimals) * token0_price + \
                                    (amt1_per_L / 10**token1_decimals) * token1_price
                        range_liq_usd = r['liq'] * usd_per_L

                        if range_liq_usd > 0:
                            tick_fy_key = str(r['tick'])
                            if hour_key not in hourly_fee_yield:
                                hourly_fee_yield[hour_key] = {}
                            if tick_fy_key not in hourly_fee_yield[hour_key]:
                                hourly_fee_yield[hour_key][tick_fy_key] = Decimal('0')
                            hourly_fee_yield[hour_key][tick_fy_key] += Decimal(str(range_fee_usd)) / Decimal(str(range_liq_usd))


    ## ── Compress old hourly buckets into daily windows ──
    ## Last 24h stays hourly, older data gets aggregated into 24h windows
    ## anchored to now_utc. Daily bucket keys use the window start hour
    ## (e.g., '2026-03-25 15') so they look like hourly keys to the frontend.
    cutoff_24h = (now_utc - timedelta(hours=24)).strftime('%Y-%m-%d %H')
    old_hours_vol = sorted(k for k in hourly_volume if k < cutoff_24h)
    old_hours_fy = sorted(k for k in hourly_fee_yield if k < cutoff_24h)

    if old_hours_vol:
        ## Group old hours into 24h windows anchored to now_utc
        ## Window N covers: (now - (N+1)*24h) to (now - N*24h)
        daily_vol = {}
        for hour_key in old_hours_vol:
            hour_dt = datetime.strptime(hour_key, '%Y-%m-%d %H').replace(tzinfo=timezone.utc)
            hours_ago = (now_utc - hour_dt).total_seconds() / 3600
            window_idx = int(hours_ago // 24)  ## 1 = 24-48h ago, 2 = 48-72h ago, etc.
            window_start = now_utc - timedelta(hours=(window_idx + 1) * 24)
            window_key = window_start.strftime('%Y-%m-%d %H')

            if window_key not in daily_vol:
                daily_vol[window_key] = {}
            for tick_key, td in hourly_volume[hour_key].items():
                if tick_key not in daily_vol[window_key]:
                    daily_vol[window_key][tick_key] = {'input_vol0': 0, 'input_vol1': 0, 'fees_token0': 0, 'fees_token1': 0}
                daily_vol[window_key][tick_key]['input_vol0'] += td['input_vol0']
                daily_vol[window_key][tick_key]['input_vol1'] += td['input_vol1']
                daily_vol[window_key][tick_key]['fees_token0'] += td['fees_token0']
                daily_vol[window_key][tick_key]['fees_token1'] += td['fees_token1']
            del hourly_volume[hour_key]
        hourly_volume.update(daily_vol)

    if old_hours_fy:
        daily_fy = {}
        for hour_key in old_hours_fy:
            hour_dt = datetime.strptime(hour_key, '%Y-%m-%d %H').replace(tzinfo=timezone.utc)
            hours_ago = (now_utc - hour_dt).total_seconds() / 3600
            window_idx = int(hours_ago // 24)
            window_start = now_utc - timedelta(hours=(window_idx + 1) * 24)
            window_key = window_start.strftime('%Y-%m-%d %H')

            if window_key not in daily_fy:
                daily_fy[window_key] = {}
            for tick_key, val in hourly_fee_yield[hour_key].items():
                if tick_key not in daily_fy[window_key]:
                    daily_fy[window_key][tick_key] = Decimal('0')
                daily_fy[window_key][tick_key] += val
            del hourly_fee_yield[hour_key]
        hourly_fee_yield.update(daily_fy)

    ## ── Build timeframes from daily tick buckets ──
    timeframes = {}
    for tf, days in [('1d', 1), ('7d', 7), ('30d', 30)]:
        cutoff = (now_utc - timedelta(days=days)).strftime('%Y-%m-%d')
        tf_min = tf_max = None
        min_swap = max_swap = None
        swap_count = 0
        for date_key, bkt in daily_tick_buckets.items():
            if date_key >= cutoff:
                swap_count += bkt['swap_count']
                if tf_min is None or bkt['tick_min'] < tf_min:
                    tf_min = bkt['tick_min']
                    min_swap = bkt['min_swap']
                if tf_max is None or bkt['tick_max'] > tf_max:
                    tf_max = bkt['tick_max']
                    max_swap = bkt['max_swap']
        if tf_min is not None:
            timeframes[tf] = {
                'tick_min': tf_min, 'tick_max': tf_max,
                'min_swap': min_swap, 'max_swap': max_swap,
                'swap_count': swap_count,
            }

    ## ── Build positions from ALL liq mods ──
    positions = {}
    for _, row in pool_liq.iterrows():
        salt = row.get('salt', '')
        tL = int(row['tickLower'])
        tU = int(row['tickUpper'])
        delta = int(row['liquidityDelta'])
        key = (str(salt), tL, tU)
        positions[key] = positions.get(key, 0) + delta

    ## Filter to active positions (liquidity > 0)
    positions_list = []
    for (salt, tL, tU), liq in sorted(positions.items(), key=lambda x: -x[1]):
        if liq > 0:
            positions_list.append({
                's': salt,   ## V3 salt = owner address — keep as string, do not convert
                'tl': tL,
                'tu': tU,
                'l': str(liq),
            })

    ## ── Aggregate hourly_volume into time_frame_volume (USD per tick) ──
    ## Replaces raw hourly_volume in JSON output — frontend gets pre-computed USD values
    time_frame_volume = {}
    sorted_hours = sorted(hourly_volume.keys())

    for tf, days in [('1d', 1), ('7d', 7), ('30d', 30)]:
        if sorted_hours:
            anchor = datetime.strptime(sorted_hours[-1], '%Y-%m-%d %H').replace(tzinfo=timezone.utc)
            cutoff = (anchor - timedelta(days=days)).strftime('%Y-%m-%d %H')
        else:
            cutoff = ''

        tf_ticks = {}
        for hour_key, ticks in hourly_volume.items():
            if cutoff and hour_key < cutoff:
                continue
            for tick_key, d in ticks.items():
                if tick_key not in tf_ticks:
                    tf_ticks[tick_key] = {'volume_usd': 0.0, 'fees_usd': 0.0}
                vol0_usd = (d['input_vol0'] / 10**token0_decimals) * token0_price
                vol1_usd = (d['input_vol1'] / 10**token1_decimals) * token1_price
                fee0_usd = (d['fees_token0'] / 10**token0_decimals) * token0_price
                fee1_usd = (d['fees_token1'] / 10**token1_decimals) * token1_price
                tf_ticks[tick_key]['volume_usd'] += vol0_usd + vol1_usd
                tf_ticks[tick_key]['fees_usd'] += fee0_usd + fee1_usd

        for tick_key in tf_ticks:
            tf_ticks[tick_key]['volume_usd'] = round(tf_ticks[tick_key]['volume_usd'], 2)
            tf_ticks[tick_key]['fees_usd'] = round(tf_ticks[tick_key]['fees_usd'], 2)
        time_frame_volume[tf] = tf_ticks

    ## ── Compute l_usd per position at current tick (end_tick) ──
    for pos in positions_list:
        usd = liq_to_usd_at_tick(
            int(pos['l']), pos['tl'], pos['tu'], end_tick,
            token0_decimals, token1_decimals, token0_price, token1_price
        )
        pos['l_usd'] = round(usd, 2)

    ## Sort positions by l_usd descending
    positions_list.sort(key=lambda p: p['l_usd'], reverse=True)

    ## total_liquidity_usd and HHI
    total_liquidity_usd = sum(p['l_usd'] for p in positions_list)
    if total_liquidity_usd > 0:
        hhi = round(sum((p['l_usd'] / total_liquidity_usd) ** 2 for p in positions_list) * 10000, 1)
    else:
        hhi = 0.0

    ## ── Per-position fee yields ──
    fy_cutoffs = {
        '1d': (now_utc - timedelta(hours=24)).strftime('%Y-%m-%d %H'),
        '7d': (now_utc - timedelta(hours=168)).strftime('%Y-%m-%d %H'),
        '30d': (now_utc - timedelta(hours=720)).strftime('%Y-%m-%d %H'),
    }
    for pos in positions_list:
        tl, tu = pos['tl'], pos['tu']
        aligned_lower = (tl // tick_spacing) * tick_spacing
        aligned_upper = (tu // tick_spacing) * tick_spacing
        n = (aligned_upper - aligned_lower) // tick_spacing
        if n <= 0:
            pos['fy_1d'] = 0.0
            pos['fy_7d'] = 0.0
            pos['fy_30d'] = 0.0
            continue
        fy = {'1d': 0.0, '7d': 0.0, '30d': 0.0}
        for hour_key, tick_data in hourly_fee_yield.items():
            for wname, cutoff in fy_cutoffs.items():
                if hour_key >= cutoff:
                    for tick_str, val in tick_data.items():
                        aligned = (int(tick_str) // tick_spacing) * tick_spacing
                        if aligned_lower <= aligned < aligned_upper:
                            fy[wname] += float(val)
        pos['fy_1d'] = round(fy['1d'] / n, 8)
        pos['fy_7d'] = round(fy['7d'] / n, 8)
        pos['fy_30d'] = round(fy['30d'] / n, 8)

    ## ── Enrich timeframes with derived metrics ──
    for tf, days in [('1d', 1), ('7d', 7), ('30d', 30)]:
        if tf not in timeframes:
            continue
        tf_min = timeframes[tf]['tick_min']
        tf_max = timeframes[tf]['tick_max']

        tf_vol = time_frame_volume.get(tf, {})
        volume_usd = round(sum(v['volume_usd'] for v in tf_vol.values()), 2)
        fees_usd   = round(sum(v['fees_usd']   for v in tf_vol.values()), 2)

        active_liq = 0.0
        for pos in positions_list:
            tl, tu = pos['tl'], pos['tu']
            if tl < tf_max and tu > tf_min:
                overlap_low  = max(tl, tf_min)
                overlap_high = min(tu, tf_max)
                mid_tick = (overlap_low + overlap_high) // 2
                active_liq += liq_to_usd_at_tick(
                    int(pos['l']), tl, tu, mid_tick,
                    token0_decimals, token1_decimals, token0_price, token1_price
                )
        active_liq = round(active_liq, 2)

        cutoff_fy = (now_utc - timedelta(days=days)).strftime('%Y-%m-%d %H')
        tick_yield_sums = {}
        for hour_key, tick_data in hourly_fee_yield.items():
            if hour_key < cutoff_fy:
                continue
            for tick_str, val in tick_data.items():
                v = float(val)
                tick_yield_sums[tick_str] = tick_yield_sums.get(tick_str, 0.0) + v
        active_yields = [v for v in tick_yield_sums.values() if v > 0]
        fee_yield_pct = round((sum(active_yields) / len(active_yields)) * 100, 6) if active_yields else 0.0

        turnover       = round(volume_usd / active_liq, 4)           if active_liq > 0 else None
        efficiency_ann = round((fees_usd / active_liq) * (365 / days), 4) if active_liq > 0 else None
        utilization    = round(active_liq / total_liquidity_usd, 4)  if total_liquidity_usd > 0 else None

        decimal_adj = 10 ** (token0_decimals - token1_decimals)
        current_price_f = float(Decimal('1.0001') ** Decimal(end_tick)) * decimal_adj
        high_price_f    = float(Decimal('1.0001') ** Decimal(tf_max))   * decimal_adj
        low_price_f     = float(Decimal('1.0001') ** Decimal(tf_min))   * decimal_adj
        p_high = max(high_price_f, low_price_f)
        p_low  = min(high_price_f, low_price_f)
        range_width_pct = round((p_high - p_low) / current_price_f * 100, 4) if current_price_f > 0 else None

        timeframes[tf].update({
            'volume_usd':            volume_usd,
            'fees_usd':              fees_usd,
            'active_liquidity_usd':  active_liq,
            'fee_yield':             fee_yield_pct,
            'turnover':              turnover,
            'efficiency_annualized': efficiency_ann,
            'utilization':           utilization,
            'range_width_pct':       range_width_pct,
        })

    ## ── Serialize hourly_fee_yield (Decimal → float, rounded to 6 decimals) ──
    hourly_fee_yield_serialized = {
        h: {t: round(float(v), 6) for t, v in ticks.items()}
        for h, ticks in hourly_fee_yield.items()
    }

    processing_time = time.time() - t_start
    total_swaps = single_tick_count + multi_tick_count

    ## ── Build output ──
    pool_data = {
        "metadata": {
            "pool_id": pool_address,
            "platform": platform,
            "chain": chain,
            "fee": fee_tier,
            "tick_spacing": tick_spacing,
            "token0_symbol": token0_symbol,
            "token1_symbol": token1_symbol,
            "token0_address": token0_address,
            "token1_address": token1_address,
            "token0_anchor": token0_anchor,
            "token1_anchor": token1_anchor,
            "token0_decimals": token0_decimals,
            "token1_decimals": token1_decimals,
            "token0_price": token0_price,
            "token1_price": token1_price,
            "current_tick": end_tick,
            "tick_range_min": min_tick_aligned,
            "tick_range_max": max_tick_aligned,
            "swap_count": total_swaps,
            "single_tick_count": single_tick_count,
            "multi_tick_count": multi_tick_count,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "processing_time_seconds": round(processing_time, 2),
            "sanity_checks": {
                "liquidity_matches": liq_matches,
                "liquidity_mismatches": liq_mismatches,
                "output_deviation_flags": output_flags,
            },
            "total_active_positions": len(positions_list),
            "total_liquidity_usd": round(total_liquidity_usd, 2),
            "hhi": hhi,
            "latest_swap": latest_swap,
            "ago_swaps": ago_swaps,
            "timeframes": timeframes,
        },
        "positions": positions_list,
        "time_frame_volume": time_frame_volume,
        "hourly_fee_yield": hourly_fee_yield_serialized,
    }

    print(f"    {pool_address[:16]}... done in {processing_time:.1f}s — {total_swaps} swaps, {liq_matches}/{liq_matches + liq_mismatches} liq match, {output_flags} output flags, {len(positions_list)} positions")

    return pool_data


## ═══════════════════════════════════════════════════════════════
## MAIN
## ═══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import re

    parquet_dir = os.path.join(os.path.dirname(__file__), '..', 'data_fetcher_bq', 'output')
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)

    ## Auto-discover all uniswapv3 chain_protocols from parquet files
    all_parquets = glob.glob(os.path.join(parquet_dir, '*_uniswapv3_pool_creations_*.parquet'))
    chain_protocols = sorted(set(re.match(r'(.+_uniswapv3)', os.path.basename(f)).group(1) for f in all_parquets))
    if not chain_protocols:
        print("No uniswapv3 pool_creations parquets found")
        exit(1)
    print(f"Found V3 data for: {chain_protocols}")

    pc_frames, sw_frames, liq_frames = [], [], []
    for chain_protocol in chain_protocols:
        pc_path = find_latest_parquet(parquet_dir, chain_protocol, 'pool_creations')
        swaps_path = find_latest_parquet(parquet_dir, chain_protocol, 'swaps')
        liq_path = find_latest_parquet(parquet_dir, chain_protocol, 'liquidity_movements')

        if not all([pc_path, swaps_path, liq_path]):
            print(f"Missing parquets for {chain_protocol} — skip")
            continue

        print(f"Loading parquets for {chain_protocol}...")
        pc_frames.append(pd.read_parquet(pc_path))
        sw_frames.append(pd.read_parquet(swaps_path))
        liq_frames.append(pd.read_parquet(liq_path))

    pc_df = pd.concat(pc_frames, ignore_index=True)
    swaps_df = pd.concat(sw_frames, ignore_index=True)
    liq_df = pd.concat(liq_frames, ignore_index=True)
    print(f"  Total — Pool creations: {len(pc_df)}, Swaps: {len(swaps_df)}, Liq movements: {len(liq_df)}")

    ## Process each pool
    upload_queue = []
    flagged_pools = []
    pools_to_trim = []
    for _, pool_row in pc_df.iterrows():
        pool_address = pool_row['pool_address']
        tick_spacing = int(pool_row['tickSpacing'])
        fee_tier = int(pool_row['fee'])
        platform = pool_row.get('protocol', 'uniswap_v3')

        token0_decimals = pool_row.get('token0_decimals')
        token1_decimals = pool_row.get('token1_decimals')
        token0_price = pool_row.get('token0_price')
        token1_price = pool_row.get('token1_price')
        token0_symbol = pool_row.get('token0_symbol', '?')
        token1_symbol = pool_row.get('token1_symbol', '?')
        token0_anchor = pool_row.get('token0_anchor') or None
        token1_anchor = pool_row.get('token1_anchor') or None

        ## Flag missing data — do not process without price/decimals
        missing = []
        if pd.isna(token0_decimals): missing.append(f"token0_decimals ({pool_row['token0']})")
        if pd.isna(token1_decimals): missing.append(f"token1_decimals ({pool_row['token1']})")
        if pd.isna(token0_price): missing.append(f"token0_price ({pool_row['token0']})")
        if pd.isna(token1_price): missing.append(f"token1_price ({pool_row['token1']})")
        if missing:
            print(f"\n  !! SKIP {pool_address[:16]}... — missing: {', '.join(missing)}")
            continue

        token0_decimals = int(token0_decimals)
        token1_decimals = int(token1_decimals)
        token0_price = float(token0_price)
        token1_price = float(token1_price)

        print(f"\n  Processing {pool_address[:16]}... {token0_symbol}/{token1_symbol} (ts={tick_spacing}, fee={fee_tier}, prices=${token0_price:.2f}/${token1_price:.2f})")

        pool_data = process_pool(
            pool_address, swaps_df, liq_df, tick_spacing, fee_tier,
            token0_decimals, token1_decimals, token0_price, token1_price,
            chain=pool_row['chain'], platform=platform,
            token0_address=pool_row['token0'], token1_address=pool_row['token1'],
            token0_symbol=token0_symbol, token1_symbol=token1_symbol,
            token0_anchor=token0_anchor, token1_anchor=token1_anchor
        )

        if pool_data is None:
            continue

        ## Serialize in memory first — size gate before any disk write
        chain_prefix = pool_row['chain'].replace('_c', '')  ## avalanche_c → avalanche
        json_serializer = lambda o: int(o) if isinstance(o, np.integer) else float(o) if isinstance(o, np.floating) else str(o)
        json_str = json.dumps(pool_data, default=json_serializer)
        file_size_kb = len(json_str.encode('utf-8')) / 1024
        print(f"    Serialized: {file_size_kb:.1f} KB")

        ## Size gate: >5MB → queue for trimmed processor, skip disk write entirely
        if file_size_kb > 5120:
            pools_to_trim.append({
                'pool_address': pool_address,
                'chain': pool_row['chain'],
                'fee': int(pool_row['fee']),
                'tickSpacing': int(pool_row['tickSpacing']),
                'token0': pool_row['token0'],
                'token1': pool_row['token1'],
                'token0_symbol': token0_symbol,
                'token1_symbol': token1_symbol,
            })
            print(f"    !! SIZE {file_size_kb:.1f} KB > 5MB — queued for trimmed processor, skipping")
            continue

        ## Write to disk
        json_path = os.path.join(output_dir, f'pool_data_{chain_prefix}_{pool_address[:10]}.json')
        with open(json_path, 'w') as f:
            f.write(json_str)
        file_size_mb = file_size_kb / 1024
        size_str = f"{file_size_mb:.2f} MB" if file_size_mb >= 1 else f"{file_size_kb:.1f} KB"
        print(f"    Saved: {json_path} ({size_str})")

        ## Upload gate: only queue if no flags
        sanity = pool_data['metadata']['sanity_checks']
        if sanity['liquidity_mismatches'] > 0 or sanity['output_deviation_flags'] > 0:
            print(f"    !! FLAG: {sanity['liquidity_mismatches']} liq mismatches, {sanity['output_deviation_flags']} output flags — NOT uploading")
            flagged_pools.append({
                'pool': pool_address,
                'chain': pool_row['chain'],
                'pair': f"{token0_symbol}/{token1_symbol}",
                'liq_mismatches': sanity['liquidity_mismatches'],
                'output_flags': sanity['output_deviation_flags'],
            })
        else:
            upload_queue.append(json_path)

    ## ── Upload clean files to R2 ──
    if upload_queue:
        print(f"\n  Uploading {len(upload_queue)} clean pool files to R2...")
        try:
            import boto3
            from botocore.config import Config as BotoConfig

            R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
            R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
            R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
            R2_BUCKET_NAME = os.environ.get('R2_BUCKET_NAME')
            R2_PREFIX = 'per_pool_data/'

            if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
                print("  !! R2 credentials not set in environment — skipping upload")
            else:
                r2 = boto3.client('s3',
                    endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
                    aws_access_key_id=R2_ACCESS_KEY_ID,
                    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
                    config=BotoConfig(signature_version='s3v4', retries={'max_attempts': 3, 'mode': 'adaptive'}),
                )
                for json_path in upload_queue:
                    filename = os.path.basename(json_path)
                    remote_key = f'{R2_PREFIX}{filename}'
                    with open(json_path, 'rb') as f:
                        r2.put_object(Bucket=R2_BUCKET_NAME, Key=remote_key, Body=f, ContentType='application/json')
                    print(f"    Uploaded: {filename} → {remote_key}")
                print(f"  {len(upload_queue)} files uploaded to R2")
        except ImportError:
            print("  !! boto3 not installed — skipping R2 upload")
        except Exception as e:
            print(f"  !! R2 upload failed: {e}")
    else:
        print(f"\n  No clean files to upload.")

    ## ── Summary: flagged pools ──
    if flagged_pools:
        print(f"\n{'='*60}")
        print(f"  FLAGGED POOLS — not uploaded ({len(flagged_pools)}):")
        print(f"{'='*60}")
        for fp in flagged_pools:
            print(f"  {fp['chain']} | {fp['pool'][:16]}... | {fp['pair']} | liq_mismatches={fp['liq_mismatches']} output_flags={fp['output_flags']}")

    ## ── Write trim queue for oversized pools ──
    trim_queue_path = os.path.join(output_dir, 'uniswapv3_pools_to_trim.json')
    with open(trim_queue_path, 'w') as f:
        json.dump(pools_to_trim, f, indent=2)
    if pools_to_trim:
        print(f"\n  Trim queue: {len(pools_to_trim)} pools written to {trim_queue_path}")
        for p in pools_to_trim:
            print(f"    {p['chain']} | {p['pool_address'][:16]}... | {p['token0_symbol']}/{p['token1_symbol']}")

    print(f"\nDone. {len(upload_queue)} uploaded, {len(flagged_pools)} flagged, {len(pools_to_trim)} queued for trim.")
