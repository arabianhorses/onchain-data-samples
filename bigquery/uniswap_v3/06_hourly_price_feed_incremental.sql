-- Scheduled every 4 hours. Scans last 6 hours of swaps (2h overlap for safety).
-- MERGEs into hourly_price_feed — updates existing rows, inserts new ones.

MERGE lpscanner.hourly_price_feed AS target
USING (

WITH
stable_token_prices AS (
  SELECT
    hour_ , tm.chain, tm.token_symbol, tm.contract_address AS token_address, tm.decimals AS tokenDecimals
    , 1.0 AS avg_price
  FROM lpscanner.token_mapping tm
  CROSS JOIN UNNEST(
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR), HOUR),
      CURRENT_TIMESTAMP(),
      INTERVAL 1 HOUR
    )
  ) AS hour_
  WHERE tm.is_dollar_pegged = TRUE AND tm.chain IN ('ethereum', 'avalanche')
)

, price_feed_from_stables AS (
SELECT
  hour_, chain, token_symbol, token_address, tokenDecimals
  , AVG(tokenPriceInfo) AS avg_price
FROM (

  -- ================================================================
  -- ============ ETHEREUM ==========================================
  -- ================================================================

  -- ETH V4: Token1 price derived from token0 being dollar-pegged
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk1Symbol AS token_symbol, tk1Address AS token_address, tk1Decimals AS tokenDecimals
    , ABS(amount0/amount1)/POW(10, (tk0Decimals - tk1Decimals)) AS tokenPriceInfo
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.poolId
      , pc.currency0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.currency1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.poolId, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , ABS(swa.amount0/POW(10, tk0.decimals)) AS tk0Amount
    FROM lpscanner.ethereum_uniswapv4_swaps swa
    LEFT JOIN lpscanner.ethereum_uniswapv4_pool_creations pc ON swa.poolId = pc.poolId
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.currency0 AND tk0.chain = 'ethereum' AND tk0.is_dollar_pegged = TRUE
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.currency1 AND tk1.chain = 'ethereum'
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk0Amount > 100

  UNION ALL

  -- ETH V4: Token0 price derived from token1 being dollar-pegged
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk0Symbol AS token_symbol, tk0Address AS token_address, tk0Decimals AS tokenDecimals
    , ABS(amount1/amount0)/POW(10, (tk1Decimals - tk0Decimals)) AS tokenPriceInfo
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.poolId
      , pc.currency0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.currency1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.poolId, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , ABS(swa.amount1/POW(10, tk1.decimals)) AS tk1Amount
    FROM lpscanner.ethereum_uniswapv4_swaps swa
    LEFT JOIN lpscanner.ethereum_uniswapv4_pool_creations pc ON swa.poolId = pc.poolId
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.currency0 AND tk0.chain = 'ethereum'
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.currency1 AND tk1.chain = 'ethereum' AND tk1.is_dollar_pegged = TRUE
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk1Amount > 100

  UNION ALL

  -- ETH V3: Token1 price derived from token0 being dollar-pegged
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk1Symbol AS token_symbol, tk1Address AS token_address, tk1Decimals AS tokenDecimals
    , ABS(amount0/amount1)/POW(10, (tk0Decimals - tk1Decimals)) AS tokenPriceInfo
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, tk0.token_symbol AS tk0Symbol, CAST(swa.amount0 AS FLOAT64) AS amount0
      , pc.token1 AS tk1Address, tk1.token_symbol AS tk1Symbol, CAST(swa.amount1 AS FLOAT64) AS amount1
      , tk0.decimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , ABS(CAST(swa.amount0 AS FLOAT64)/POW(10, tk0.decimals)) AS tk0Amount
    FROM lpscanner.ethereum_uniswapv3_swaps swa
    LEFT JOIN lpscanner.ethereum_uniswapv3_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.token0 AND tk0.chain = 'ethereum' AND tk0.is_dollar_pegged = TRUE
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.token1 AND tk1.chain = 'ethereum'
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(CAST(swa.amount0 AS FLOAT64)) > 0 AND ABS(CAST(swa.amount1 AS FLOAT64)) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk0Amount > 100

  UNION ALL

  -- ETH V3: Token0 price derived from token1 being dollar-pegged
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk0Symbol AS token_symbol, tk0Address AS token_address, tk0Decimals AS tokenDecimals
    , ABS(amount1/amount0)/POW(10, (tk1Decimals - tk0Decimals)) AS tokenPriceInfo
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, tk0.token_symbol AS tk0Symbol, CAST(swa.amount0 AS FLOAT64) AS amount0
      , pc.token1 AS tk1Address, tk1.token_symbol AS tk1Symbol, CAST(swa.amount1 AS FLOAT64) AS amount1
      , tk0.decimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , ABS(CAST(swa.amount1 AS FLOAT64)/POW(10, tk1.decimals)) AS tk1Amount
    FROM lpscanner.ethereum_uniswapv3_swaps swa
    LEFT JOIN lpscanner.ethereum_uniswapv3_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.token0 AND tk0.chain = 'ethereum'
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.token1 AND tk1.chain = 'ethereum' AND tk1.is_dollar_pegged = TRUE
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(CAST(swa.amount0 AS FLOAT64)) > 0 AND ABS(CAST(swa.amount1 AS FLOAT64)) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk1Amount > 100

  UNION ALL

  -- ================================================================
  -- ============ AVALANCHE =========================================
  -- ================================================================

  -- AVAX V4: Token1 price derived from token0 being dollar-pegged
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk1Symbol AS token_symbol, tk1Address AS token_address, tk1Decimals AS tokenDecimals
    , ABS(amount0/amount1)/POW(10, (tk0Decimals - tk1Decimals)) AS tokenPriceInfo
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.poolId
      , pc.currency0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.currency1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.poolId, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , ABS(swa.amount0/POW(10, tk0.decimals)) AS tk0Amount
    FROM lpscanner.avalanche_uniswapv4_swaps swa
    LEFT JOIN lpscanner.avalanche_uniswapv4_pool_creations pc ON swa.poolId = pc.poolId
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.currency0 AND tk0.chain = 'avalanche' AND tk0.is_dollar_pegged = TRUE
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.currency1 AND tk1.chain = 'avalanche'
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk0Amount > 100

  UNION ALL

  -- AVAX V4: Token0 price derived from token1 being dollar-pegged
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk0Symbol AS token_symbol, tk0Address AS token_address, tk0Decimals AS tokenDecimals
    , ABS(amount1/amount0)/POW(10, (tk1Decimals - tk0Decimals)) AS tokenPriceInfo
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.poolId
      , pc.currency0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.currency1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.poolId, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , ABS(swa.amount1/POW(10, tk1.decimals)) AS tk1Amount
    FROM lpscanner.avalanche_uniswapv4_swaps swa
    LEFT JOIN lpscanner.avalanche_uniswapv4_pool_creations pc ON swa.poolId = pc.poolId
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.currency0 AND tk0.chain = 'avalanche'
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.currency1 AND tk1.chain = 'avalanche' AND tk1.is_dollar_pegged = TRUE
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk1Amount > 100

  UNION ALL

  -- AVAX V3: Token1 price derived from token0 being dollar-pegged
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk1Symbol AS token_symbol, tk1Address AS token_address, tk1Decimals AS tokenDecimals
    , ABS(amount0/amount1)/POW(10, (tk0Decimals - tk1Decimals)) AS tokenPriceInfo
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.token1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , ABS(swa.amount0/POW(10, tk0.decimals)) AS tk0Amount
    FROM lpscanner.avalanche_uniswapv3_swaps swa
    LEFT JOIN lpscanner.avalanche_uniswapv3_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.token0 AND tk0.chain = 'avalanche' AND tk0.is_dollar_pegged = TRUE
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.token1 AND tk1.chain = 'avalanche'
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk0Amount > 100

  UNION ALL

  -- AVAX V3: Token0 price derived from token1 being dollar-pegged
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk0Symbol AS token_symbol, tk0Address AS token_address, tk0Decimals AS tokenDecimals
    , ABS(amount1/amount0)/POW(10, (tk1Decimals - tk0Decimals)) AS tokenPriceInfo
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.token1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , ABS(swa.amount1/POW(10, tk1.decimals)) AS tk1Amount
    FROM lpscanner.avalanche_uniswapv3_swaps swa
    LEFT JOIN lpscanner.avalanche_uniswapv3_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.token0 AND tk0.chain = 'avalanche'
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.token1 AND tk1.chain = 'avalanche' AND tk1.is_dollar_pegged = TRUE
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk1Amount > 100

  UNION ALL

  -- AVAX Blackhole: Token1 price derived from token0 being dollar-pegged
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk1Symbol AS token_symbol, tk1Address AS token_address, tk1Decimals AS tokenDecimals
    , ABS(amount0/amount1)/POW(10, (tk0Decimals - tk1Decimals)) AS tokenPriceInfo
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.token1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , ABS(swa.amount0/POW(10, tk0.decimals)) AS tk0Amount
    FROM lpscanner.avalanche_blackhole_swaps swa
    LEFT JOIN lpscanner.avalanche_blackhole_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.token0 AND tk0.chain = 'avalanche' AND tk0.is_dollar_pegged = TRUE
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.token1 AND tk1.chain = 'avalanche'
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk0Amount > 100

  UNION ALL

  -- AVAX Blackhole: Token0 price derived from token1 being dollar-pegged
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk0Symbol AS token_symbol, tk0Address AS token_address, tk0Decimals AS tokenDecimals
    , ABS(amount1/amount0)/POW(10, (tk1Decimals - tk0Decimals)) AS tokenPriceInfo
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.token1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , ABS(swa.amount1/POW(10, tk1.decimals)) AS tk1Amount
    FROM lpscanner.avalanche_blackhole_swaps swa
    LEFT JOIN lpscanner.avalanche_blackhole_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.token0 AND tk0.chain = 'avalanche'
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.token1 AND tk1.chain = 'avalanche' AND tk1.is_dollar_pegged = TRUE
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk1Amount > 100

  UNION ALL

  -- AVAX Pangolin: Token1 price derived from token0 being dollar-pegged
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk1Symbol AS token_symbol, tk1Address AS token_address, tk1Decimals AS tokenDecimals
    , ABS(amount0/amount1)/POW(10, (tk0Decimals - tk1Decimals)) AS tokenPriceInfo
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.token1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , ABS(swa.amount0/POW(10, tk0.decimals)) AS tk0Amount
    FROM lpscanner.avalanche_pangolin_swaps swa
    LEFT JOIN lpscanner.avalanche_pangolin_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.token0 AND tk0.chain = 'avalanche' AND tk0.is_dollar_pegged = TRUE
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.token1 AND tk1.chain = 'avalanche'
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk0Amount > 100

  UNION ALL

  -- AVAX Pangolin: Token0 price derived from token1 being dollar-pegged
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk0Symbol AS token_symbol, tk0Address AS token_address, tk0Decimals AS tokenDecimals
    , ABS(amount1/amount0)/POW(10, (tk1Decimals - tk0Decimals)) AS tokenPriceInfo
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.token1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , ABS(swa.amount1/POW(10, tk1.decimals)) AS tk1Amount
    FROM lpscanner.avalanche_pangolin_swaps swa
    LEFT JOIN lpscanner.avalanche_pangolin_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.token0 AND tk0.chain = 'avalanche'
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.token1 AND tk1.chain = 'avalanche' AND tk1.is_dollar_pegged = TRUE
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk1Amount > 100

)
GROUP BY 1, 2, 3, 4, 5
)

, aa AS (

  -- ================================================================
  -- ============ ETHEREUM ==========================================
  -- ================================================================

  -- ETH V4: Token1 price derived from token0 via price_feed_from_stables
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk1Symbol AS token_symbol, tk1Address AS token_address, tk1Decimals AS tokenDecimals
    , AVG(ABS(amount0/amount1)/POW(10, (tk0Decimals - tk1Decimals)) * tk0Price) AS avg_price
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.poolId
      , pc.currency0 AS tk0Address, pffs.token_symbol AS tk0Symbol, amount0
      , pc.currency1 AS tk1Address, tk1.token_symbol AS tk1Symbol, amount1
      , pffs.tokenDecimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.poolId, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , (ABS(swa.amount0)/POW(10, pffs.tokenDecimals)) * pffs.avg_price AS tk0Usd
      , pffs.avg_price AS tk0Price
    FROM lpscanner.ethereum_uniswapv4_swaps swa
    LEFT JOIN lpscanner.ethereum_uniswapv4_pool_creations pc ON swa.poolId = pc.poolId
    INNER JOIN price_feed_from_stables pffs ON pc.currency0 = pffs.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, hour) = pffs.hour_ AND pffs.chain = swa.chain
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.currency1 AND tk1.chain = 'ethereum'
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(amount0) > 0 AND ABS(amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk0Usd > 100
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL

  -- ETH V4: Token0 price derived from token1 via price_feed_from_stables
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk0Symbol AS token_symbol, tk0Address AS token_address, tk0Decimals AS tokenDecimals
    , AVG(ABS(amount1/amount0)/POW(10, (tk1Decimals - tk0Decimals)) * tk1Price) AS avg_price
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.poolId
      , pc.currency0 AS tk0Address, tk0.token_symbol AS tk0Symbol, amount0
      , pc.currency1 AS tk1Address, pffs.token_symbol AS tk1Symbol, amount1
      , tk0.decimals AS tk0Decimals, pffs.tokenDecimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.poolId, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , (ABS(swa.amount1)/POW(10, pffs.tokenDecimals)) * pffs.avg_price AS tk1Usd
      , pffs.avg_price AS tk1Price
    FROM lpscanner.ethereum_uniswapv4_swaps swa
    LEFT JOIN lpscanner.ethereum_uniswapv4_pool_creations pc ON swa.poolId = pc.poolId
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.currency0 AND tk0.chain = 'ethereum'
    INNER JOIN price_feed_from_stables pffs ON pc.currency1 = pffs.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, hour) = pffs.hour_ AND pffs.chain = swa.chain
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(amount0) > 0 AND ABS(amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk1Usd > 100
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL

  -- ETH V3: Token1 price derived from token0 via price_feed_from_stables
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk1Symbol AS token_symbol, tk1Address AS token_address, tk1Decimals AS tokenDecimals
    , AVG(ABS(amount0/amount1)/POW(10, (tk0Decimals - tk1Decimals)) * tk0Price) AS avg_price
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, pffs.token_symbol AS tk0Symbol, CAST(swa.amount0 AS FLOAT64) AS amount0
      , pc.token1 AS tk1Address, tk1.token_symbol AS tk1Symbol, CAST(swa.amount1 AS FLOAT64) AS amount1
      , pffs.tokenDecimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , (ABS(CAST(swa.amount0 AS FLOAT64))/POW(10, pffs.tokenDecimals)) * pffs.avg_price AS tk0Usd
      , pffs.avg_price AS tk0Price
    FROM lpscanner.ethereum_uniswapv3_swaps swa
    LEFT JOIN lpscanner.ethereum_uniswapv3_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN price_feed_from_stables pffs ON pc.token0 = pffs.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, hour) = pffs.hour_ AND pffs.chain = swa.chain
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.token1 AND tk1.chain = 'ethereum'
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(CAST(swa.amount0 AS FLOAT64)) > 0 AND ABS(CAST(swa.amount1 AS FLOAT64)) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk0Usd > 100
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL

  -- ETH V3: Token0 price derived from token1 via price_feed_from_stables
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk0Symbol AS token_symbol, tk0Address AS token_address, tk0Decimals AS tokenDecimals
    , AVG(ABS(amount1/amount0)/POW(10, (tk1Decimals - tk0Decimals)) * tk1Price) AS avg_price
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, tk0.token_symbol AS tk0Symbol, CAST(swa.amount0 AS FLOAT64) AS amount0
      , pc.token1 AS tk1Address, pffs.token_symbol AS tk1Symbol, CAST(swa.amount1 AS FLOAT64) AS amount1
      , tk0.decimals AS tk0Decimals, pffs.tokenDecimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , (ABS(CAST(swa.amount1 AS FLOAT64))/POW(10, pffs.tokenDecimals)) * pffs.avg_price AS tk1Usd
      , pffs.avg_price AS tk1Price
    FROM lpscanner.ethereum_uniswapv3_swaps swa
    LEFT JOIN lpscanner.ethereum_uniswapv3_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.token0 AND tk0.chain = 'ethereum'
    INNER JOIN price_feed_from_stables pffs ON pc.token1 = pffs.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, hour) = pffs.hour_ AND pffs.chain = swa.chain
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(CAST(swa.amount0 AS FLOAT64)) > 0 AND ABS(CAST(swa.amount1 AS FLOAT64)) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk1Usd > 100
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL

  -- ================================================================
  -- ============ AVALANCHE =========================================
  -- ================================================================

  -- AVAX V4: Token1 price derived from token0 via price_feed_from_stables
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk1Symbol AS token_symbol, tk1Address AS token_address, tk1Decimals AS tokenDecimals
    , AVG(ABS(amount0/amount1)/POW(10, (tk0Decimals - tk1Decimals)) * tk0Price) AS avg_price
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.poolId
      , pc.currency0 AS tk0Address, pffs.token_symbol AS tk0Symbol, amount0
      , pc.currency1 AS tk1Address, tk1.token_symbol AS tk1Symbol, amount1
      , pffs.tokenDecimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.poolId, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , (ABS(swa.amount0)/POW(10, pffs.tokenDecimals)) * pffs.avg_price AS tk0Usd
      , pffs.avg_price AS tk0Price
    FROM lpscanner.avalanche_uniswapv4_swaps swa
    LEFT JOIN lpscanner.avalanche_uniswapv4_pool_creations pc ON swa.poolId = pc.poolId
    INNER JOIN price_feed_from_stables pffs ON pc.currency0 = pffs.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, hour) = pffs.hour_ AND pffs.chain = swa.chain
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.currency1 AND tk1.chain = 'avalanche'
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(amount0) > 0 AND ABS(amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk0Usd > 100
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL

  -- AVAX V4: Token0 price derived from token1 via price_feed_from_stables
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk0Symbol AS token_symbol, tk0Address AS token_address, tk0Decimals AS tokenDecimals
    , AVG(ABS(amount1/amount0)/POW(10, (tk1Decimals - tk0Decimals)) * tk1Price) AS avg_price
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.poolId
      , pc.currency0 AS tk0Address, tk0.token_symbol AS tk0Symbol, amount0
      , pc.currency1 AS tk1Address, pffs.token_symbol AS tk1Symbol, amount1
      , tk0.decimals AS tk0Decimals, pffs.tokenDecimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.poolId, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , (ABS(swa.amount1)/POW(10, pffs.tokenDecimals)) * pffs.avg_price AS tk1Usd
      , pffs.avg_price AS tk1Price
    FROM lpscanner.avalanche_uniswapv4_swaps swa
    LEFT JOIN lpscanner.avalanche_uniswapv4_pool_creations pc ON swa.poolId = pc.poolId
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.currency0 AND tk0.chain = 'avalanche'
    INNER JOIN price_feed_from_stables pffs ON pc.currency1 = pffs.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, hour) = pffs.hour_ AND pffs.chain = swa.chain
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(amount0) > 0 AND ABS(amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk1Usd > 100
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL

  -- AVAX V3: Token1 price derived from token0 via price_feed_from_stables
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk1Symbol AS token_symbol, tk1Address AS token_address, tk1Decimals AS tokenDecimals
    , AVG(ABS(amount0/amount1)/POW(10, (tk0Decimals - tk1Decimals)) * tk0Price) AS avg_price
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, pffs.token_symbol AS tk0Symbol, swa.amount0
      , pc.token1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , pffs.tokenDecimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , (ABS(swa.amount0)/POW(10, pffs.tokenDecimals)) * pffs.avg_price AS tk0Usd
      , pffs.avg_price AS tk0Price
    FROM lpscanner.avalanche_uniswapv3_swaps swa
    LEFT JOIN lpscanner.avalanche_uniswapv3_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN price_feed_from_stables pffs ON pc.token0 = pffs.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, hour) = pffs.hour_ AND pffs.chain = swa.chain
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.token1 AND tk1.chain = 'avalanche'
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk0Usd > 100
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL

  -- AVAX V3: Token0 price derived from token1 via price_feed_from_stables
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk0Symbol AS token_symbol, tk0Address AS token_address, tk0Decimals AS tokenDecimals
    , AVG(ABS(amount1/amount0)/POW(10, (tk1Decimals - tk0Decimals)) * tk1Price) AS avg_price
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.token1 AS tk1Address, pffs.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, pffs.tokenDecimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , (ABS(swa.amount1)/POW(10, pffs.tokenDecimals)) * pffs.avg_price AS tk1Usd
      , pffs.avg_price AS tk1Price
    FROM lpscanner.avalanche_uniswapv3_swaps swa
    LEFT JOIN lpscanner.avalanche_uniswapv3_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.token0 AND tk0.chain = 'avalanche'
    INNER JOIN price_feed_from_stables pffs ON pc.token1 = pffs.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, hour) = pffs.hour_ AND pffs.chain = swa.chain
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk1Usd > 100
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL

  -- AVAX Blackhole: Token1 price derived from token0 via price_feed_from_stables
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk1Symbol AS token_symbol, tk1Address AS token_address, tk1Decimals AS tokenDecimals
    , AVG(ABS(amount0/amount1)/POW(10, (tk0Decimals - tk1Decimals)) * tk0Price) AS avg_price
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, pffs.token_symbol AS tk0Symbol, swa.amount0
      , pc.token1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , pffs.tokenDecimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , (ABS(swa.amount0)/POW(10, pffs.tokenDecimals)) * pffs.avg_price AS tk0Usd
      , pffs.avg_price AS tk0Price
    FROM lpscanner.avalanche_blackhole_swaps swa
    LEFT JOIN lpscanner.avalanche_blackhole_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN price_feed_from_stables pffs ON pc.token0 = pffs.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, hour) = pffs.hour_ AND pffs.chain = swa.chain
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.token1 AND tk1.chain = 'avalanche'
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk0Usd > 100
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL

  -- AVAX Blackhole: Token0 price derived from token1 via price_feed_from_stables
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk0Symbol AS token_symbol, tk0Address AS token_address, tk0Decimals AS tokenDecimals
    , AVG(ABS(amount1/amount0)/POW(10, (tk1Decimals - tk0Decimals)) * tk1Price) AS avg_price
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.token1 AS tk1Address, pffs.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, pffs.tokenDecimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , (ABS(swa.amount1)/POW(10, pffs.tokenDecimals)) * pffs.avg_price AS tk1Usd
      , pffs.avg_price AS tk1Price
    FROM lpscanner.avalanche_blackhole_swaps swa
    LEFT JOIN lpscanner.avalanche_blackhole_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.token0 AND tk0.chain = 'avalanche'
    INNER JOIN price_feed_from_stables pffs ON pc.token1 = pffs.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, hour) = pffs.hour_ AND pffs.chain = swa.chain
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk1Usd > 100
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL

  -- AVAX Pangolin: Token1 price derived from token0 via price_feed_from_stables
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk1Symbol AS token_symbol, tk1Address AS token_address, tk1Decimals AS tokenDecimals
    , AVG(ABS(amount0/amount1)/POW(10, (tk0Decimals - tk1Decimals)) * tk0Price) AS avg_price
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, pffs.token_symbol AS tk0Symbol, swa.amount0
      , pc.token1 AS tk1Address, tk1.token_symbol AS tk1Symbol, swa.amount1
      , pffs.tokenDecimals AS tk0Decimals, tk1.decimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , (ABS(swa.amount0)/POW(10, pffs.tokenDecimals)) * pffs.avg_price AS tk0Usd
      , pffs.avg_price AS tk0Price
    FROM lpscanner.avalanche_pangolin_swaps swa
    LEFT JOIN lpscanner.avalanche_pangolin_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN price_feed_from_stables pffs ON pc.token0 = pffs.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, hour) = pffs.hour_ AND pffs.chain = swa.chain
    INNER JOIN lpscanner.token_mapping tk1 ON tk1.contract_address = pc.token1 AND tk1.chain = 'avalanche'
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk0Usd > 100
  GROUP BY 1, 2, 3, 4, 5

  UNION ALL

  -- AVAX Pangolin: Token0 price derived from token1 via price_feed_from_stables
  SELECT
    TIMESTAMP_TRUNC(block_timestamp, hour) AS hour_
    , chain, tk0Symbol AS token_symbol, tk0Address AS token_address, tk0Decimals AS tokenDecimals
    , AVG(ABS(amount1/amount0)/POW(10, (tk1Decimals - tk0Decimals)) * tk1Price) AS avg_price
  FROM (
    SELECT
      swa.chain, swa.protocol, swa.pool_address
      , pc.token0 AS tk0Address, tk0.token_symbol AS tk0Symbol, swa.amount0
      , pc.token1 AS tk1Address, pffs.token_symbol AS tk1Symbol, swa.amount1
      , tk0.decimals AS tk0Decimals, pffs.tokenDecimals AS tk1Decimals
      , swa.block_timestamp, swa.transaction_hash, swa.generatedIndex
      , COUNT(swa.generatedIndex) OVER(PARTITION BY swa.pool_address, TIMESTAMP_TRUNC(swa.block_timestamp, day)) AS swapCntByPoolDay
      , (ABS(swa.amount1)/POW(10, pffs.tokenDecimals)) * pffs.avg_price AS tk1Usd
      , pffs.avg_price AS tk1Price
    FROM lpscanner.avalanche_pangolin_swaps swa
    LEFT JOIN lpscanner.avalanche_pangolin_pool_creations pc ON swa.pool_address = pc.pool_address
    INNER JOIN lpscanner.token_mapping tk0 ON tk0.contract_address = pc.token0 AND tk0.chain = 'avalanche'
    INNER JOIN price_feed_from_stables pffs ON pc.token1 = pffs.token_address AND TIMESTAMP_TRUNC(swa.block_timestamp, hour) = pffs.hour_ AND pffs.chain = swa.chain
    WHERE swa.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    AND ABS(swa.amount0) > 0 AND ABS(swa.amount1) > 0
  )
  WHERE swapCntByPoolDay > 20 AND tk1Usd > 100
  GROUP BY 1, 2, 3, 4, 5
)

SELECT
  hour_ , chain, token_symbol, token_address
  , CAST(tokenDecimals AS INT64) AS decimals
  , AVG(avg_price) AS avg_price
FROM (
  SELECT * FROM aa
  UNION ALL
  SELECT * FROM stable_token_prices
  UNION ALL
  SELECT * FROM price_feed_from_stables
)
GROUP BY 1, 2, 3, 4, 5

) AS source
ON target.hour_ = source.hour_
  AND target.chain = source.chain
  AND target.token_address = source.token_address

WHEN MATCHED THEN
  UPDATE SET
    token_symbol = source.token_symbol,
    decimals = source.decimals,
    avg_price = source.avg_price

WHEN NOT MATCHED THEN
  INSERT (hour_, chain, token_symbol, token_address, decimals, avg_price)
  VALUES (source.hour_, source.chain, source.token_symbol, source.token_address, source.decimals, source.avg_price)
