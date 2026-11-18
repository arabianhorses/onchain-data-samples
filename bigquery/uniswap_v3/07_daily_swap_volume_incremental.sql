-- Daily incremental: MERGE last 2 days into daily_swap_volume
-- Scheduled query: runs daily at 02:30 UTC
-- 2-day lookback catches late-arriving bronze data + recalculates today's partial day
-- Covers: Avalanche (Blackhole, Uniswap V4, Pangolin V3, Uniswap V3) + Ethereum (Uniswap V4, Uniswap V3)

DECLARE refresh_start TIMESTAMP;
SET refresh_start = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY);

MERGE lpscanner.daily_swap_volume AS target
USING (
  WITH
  bh_fees AS (
      SELECT pool_address, generatedIndex AS fee_gi, FEE,
             LEAD(generatedIndex) OVER (PARTITION BY pool_address ORDER BY generatedIndex) AS next_fee_gi
      FROM lpscanner.avalanche_blackhole_fee_changes
  )
  , pg_fees AS (
      SELECT pool_address, generatedIndex AS fee_gi, fee AS FEE,
             LEAD(generatedIndex) OVER (PARTITION BY pool_address ORDER BY generatedIndex) AS next_fee_gi
      FROM lpscanner.avalanche_pangolin_fee_changes
  )
  , swaps AS (
  -- Avalanche Blackhole (dynamic fees via bh_fees CTE)
  SELECT
      swa.chain, swa.protocol
      , swa.pool_address
      , CONCAT(pf0.token_symbol, '/', pf1.token_symbol, '-', fee.FEE/10000, '%' , ' | ', 't.s. ', pc.tickSpacing) AS poolK
      , swa.block_timestamp, swa.generatedIndex
      , CASE WHEN swa.amount0 > 0 THEN (swa.amount0/POW(10,pf0.decimals)) * pf0.avg_price
        ELSE (swa.amount1/POW(10,pf1.decimals)) * pf1.avg_price END AS volume
      , CASE WHEN swa.amount0 > 0 THEN (swa.amount0/POW(10,pf0.decimals)) * pf0.avg_price ELSE (swa.amount1/POW(10,pf1.decimals)) * pf1.avg_price END * fee.FEE / 1000000 AS lpFee
  FROM lpscanner.avalanche_blackhole_swaps swa
  LEFT JOIN lpscanner.avalanche_blackhole_pool_creations pc ON swa.pool_address = pc.pool_address
  LEFT JOIN bh_fees fee ON swa.pool_address = fee.pool_address AND swa.generatedIndex >= fee.fee_gi AND (swa.generatedIndex < fee.next_fee_gi OR fee.next_fee_gi IS NULL)
  INNER JOIN lpscanner.hourly_price_feed pf0 ON swa.chain = pf0.chain AND pc.token0 = pf0.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, HOUR) = pf0.hour_
  INNER JOIN lpscanner.hourly_price_feed pf1 ON swa.chain = pf1.chain AND pc.token1 = pf1.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, HOUR) = pf1.hour_
  WHERE swa.block_timestamp >= refresh_start

  UNION ALL

  -- Avalanche Uniswap V4 (per-swap fee from swap event — handles dynamic fees from hooks)
  SELECT
      swa.chain, swa.protocol
      , swa.poolId AS pool_address, CONCAT(pf0.token_symbol, '/', pf1.token_symbol, '-', pc.fee/10000, '%', ' | ', 't.s. ', pc.tickSpacing) AS poolK
      , swa.block_timestamp, swa.generatedIndex
      , CASE WHEN swa.amount0 > 0 THEN (ABS(swa.amount1)/POW(10,pf1.decimals)) * pf1.avg_price ELSE (ABS(swa.amount0)/POW(10,pf0.decimals)) * pf0.avg_price END AS volume
      , CASE WHEN swa.amount0 > 0 THEN (ABS(swa.amount1)/POW(10,pf1.decimals)) * pf1.avg_price ELSE (ABS(swa.amount0)/POW(10,pf0.decimals)) * pf0.avg_price END * swa.fee /1000000 AS lpFee
  FROM lpscanner.avalanche_uniswapv4_swaps swa
  LEFT JOIN lpscanner.avalanche_uniswapv4_pool_creations pc ON swa.poolId = pc.poolId
  INNER JOIN lpscanner.hourly_price_feed pf0 ON swa.chain = pf0.chain AND pc.currency0 = pf0.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, HOUR) = pf0.hour_
  INNER JOIN lpscanner.hourly_price_feed pf1 ON swa.chain = pf1.chain AND pc.currency1 = pf1.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, HOUR) = pf1.hour_
  WHERE swa.block_timestamp >= refresh_start

  UNION ALL

  -- Avalanche Pangolin V3 (dynamic fees via pg_fees CTE)
  SELECT
      swa.chain, swa.protocol
      , swa.pool_address, CONCAT(pf0.token_symbol, '/', pf1.token_symbol, '-', fee.FEE/10000, '%', ' | ', 't.s. ', pc.tickSpacing) AS poolK
      , swa.block_timestamp, swa.generatedIndex
      , CASE WHEN swa.amount0 > 0 THEN (swa.amount0/POW(10,pf0.decimals)) * pf0.avg_price ELSE (swa.amount1/POW(10,pf1.decimals)) * pf1.avg_price END AS volume
      , CASE WHEN swa.amount0 > 0 THEN (swa.amount0/POW(10,pf0.decimals)) * pf0.avg_price ELSE (swa.amount1/POW(10,pf1.decimals)) * pf1.avg_price END * fee.FEE/1000000 AS lpFee
  FROM lpscanner.avalanche_pangolin_swaps swa
  LEFT JOIN lpscanner.avalanche_pangolin_pool_creations pc ON swa.pool_address = pc.pool_address
  LEFT JOIN pg_fees fee ON swa.pool_address = fee.pool_address AND swa.generatedIndex >= fee.fee_gi AND (swa.generatedIndex < fee.next_fee_gi OR fee.next_fee_gi IS NULL)
  INNER JOIN lpscanner.hourly_price_feed pf0 ON swa.chain = pf0.chain AND pc.token0 = pf0.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, HOUR) = pf0.hour_
  INNER JOIN lpscanner.hourly_price_feed pf1 ON swa.chain = pf1.chain AND pc.token1 = pf1.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, HOUR) = pf1.hour_
  WHERE swa.block_timestamp >= refresh_start

  UNION ALL

  -- Avalanche Uniswap V3 (static fees)
  SELECT
      swa.chain, swa.protocol
      , swa.pool_address
      , CONCAT(pf0.token_symbol, '/', pf1.token_symbol, '-', pc.fee/10000, '%', ' | ', 't.s. ', pc.tickSpacing) AS poolK
      , swa.block_timestamp, swa.generatedIndex
      , CASE WHEN swa.amount0 > 0 THEN (swa.amount0/POW(10,pf0.decimals)) * pf0.avg_price
        ELSE (swa.amount1/POW(10,pf1.decimals)) * pf1.avg_price END AS volume
      , CASE WHEN swa.amount0 > 0 THEN (swa.amount0/POW(10,pf0.decimals)) * pf0.avg_price
        ELSE (swa.amount1/POW(10,pf1.decimals)) * pf1.avg_price END * pc.fee / 1000000 AS lpFee
  FROM lpscanner.avalanche_uniswapv3_swaps swa
  LEFT JOIN lpscanner.avalanche_uniswapv3_pool_creations pc ON swa.pool_address = pc.pool_address
  INNER JOIN lpscanner.hourly_price_feed pf0 ON swa.chain = pf0.chain AND pc.token0 = pf0.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, HOUR) = pf0.hour_
  INNER JOIN lpscanner.hourly_price_feed pf1 ON swa.chain = pf1.chain AND pc.token1 = pf1.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, HOUR) = pf1.hour_
  WHERE swa.block_timestamp >= refresh_start

  UNION ALL

  -- Ethereum Uniswap V4 (BIGNUMERIC columns — per-swap fee from swap event)
  SELECT
      swa.chain, swa.protocol
      , swa.poolId AS pool_address, CONCAT(pf0.token_symbol, '/', pf1.token_symbol, '-', CAST(pc.fee AS FLOAT64)/10000, '%', ' | ', 't.s. ', pc.tickSpacing) AS poolK
      , swa.block_timestamp, swa.generatedIndex
      , CASE WHEN swa.amount0 > 0 THEN (ABS(CAST(swa.amount1 AS FLOAT64))/POW(10,pf1.decimals)) * pf1.avg_price ELSE (ABS(CAST(swa.amount0 AS FLOAT64))/POW(10,pf0.decimals)) * pf0.avg_price END AS volume
      , CASE WHEN swa.amount0 > 0 THEN (ABS(CAST(swa.amount1 AS FLOAT64))/POW(10,pf1.decimals)) * pf1.avg_price ELSE (ABS(CAST(swa.amount0 AS FLOAT64))/POW(10,pf0.decimals)) * pf0.avg_price END * CAST(swa.fee AS FLOAT64) /1000000 AS lpFee
  FROM lpscanner.ethereum_uniswapv4_swaps swa
  LEFT JOIN lpscanner.ethereum_uniswapv4_pool_creations pc ON swa.poolId = pc.poolId
  INNER JOIN lpscanner.hourly_price_feed pf0 ON swa.chain = pf0.chain AND pc.currency0 = pf0.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, HOUR) = pf0.hour_
  INNER JOIN lpscanner.hourly_price_feed pf1 ON swa.chain = pf1.chain AND pc.currency1 = pf1.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, HOUR) = pf1.hour_
  WHERE swa.block_timestamp >= refresh_start

  UNION ALL

  -- Ethereum Uniswap V3 (STRING columns — need CAST to FLOAT64)
  SELECT
      swa.chain, swa.protocol
      , swa.pool_address
      , CONCAT(pf0.token_symbol, '/', pf1.token_symbol, '-', CAST(pc.fee AS FLOAT64)/10000, '%', ' | ', 't.s. ', pc.tickSpacing) AS poolK
      , swa.block_timestamp, swa.generatedIndex
      , CASE WHEN CAST(swa.amount0 AS FLOAT64) > 0 THEN (CAST(swa.amount0 AS FLOAT64)/POW(10,pf0.decimals)) * pf0.avg_price
        ELSE (CAST(swa.amount1 AS FLOAT64)/POW(10,pf1.decimals)) * pf1.avg_price END AS volume
      , CASE WHEN CAST(swa.amount0 AS FLOAT64) > 0 THEN (CAST(swa.amount0 AS FLOAT64)/POW(10,pf0.decimals)) * pf0.avg_price
        ELSE (CAST(swa.amount1 AS FLOAT64)/POW(10,pf1.decimals)) * pf1.avg_price END * CAST(pc.fee AS FLOAT64) / 1000000 AS lpFee
  FROM lpscanner.ethereum_uniswapv3_swaps swa
  LEFT JOIN lpscanner.ethereum_uniswapv3_pool_creations pc ON swa.pool_address = pc.pool_address
  INNER JOIN lpscanner.hourly_price_feed pf0 ON swa.chain = pf0.chain AND pc.token0 = pf0.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, HOUR) = pf0.hour_
  INNER JOIN lpscanner.hourly_price_feed pf1 ON swa.chain = pf1.chain AND pc.token1 = pf1.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, HOUR) = pf1.hour_
  WHERE swa.block_timestamp >= refresh_start
  )

  SELECT
    DATE(block_timestamp) AS day
    , chain, protocol, pool_address, poolK
    , SUM(volume) AS volume
    , SUM(lpFee) AS lpFee
    , COUNT(generatedIndex) AS numberOfSwaps
  FROM swaps
  GROUP BY 1, 2, 3, 4, 5
) AS source
ON target.day = source.day
   AND target.chain = source.chain
   AND target.protocol = source.protocol
   AND target.pool_address = source.pool_address
WHEN MATCHED THEN UPDATE SET
  volume = source.volume,
  lpFee = source.lpFee,
  numberOfSwaps = source.numberOfSwaps,
  poolK = source.poolK
WHEN NOT MATCHED THEN INSERT ROW
