DECLARE max_ts TIMESTAMP;

  SET max_ts = (
    SELECT MAX(block_timestamp)
    FROM `lpscanner.ethereum_uniswapv3_pool_creations`
    WHERE block_timestamp > CURRENT_TIMESTAMP() - INTERVAL 48 HOUR
  );

  IF max_ts IS NULL THEN
    SET max_ts = (
      SELECT MAX(block_timestamp)
      FROM `lpscanner.ethereum_uniswapv3_pool_creations`
      WHERE block_timestamp > TIMESTAMP_TRUNC(CURRENT_TIMESTAMP() - INTERVAL 30 DAY, MONTH)
    );
  END IF;

  IF max_ts IS NULL THEN
    SET max_ts = TIMESTAMP('2021-05-04');
  END IF;

  MERGE `lpscanner.ethereum_uniswapv3_pool_creations` AS target
  USING (
    SELECT
      'ethereum' AS chain,
      'uniswap_v3' AS protocol,
      LOWER(CONCAT('0x', SUBSTR(data, 91, 40))) AS pool_address,
      CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(1)], 27, 40)) AS token0,
      CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(2)], 27, 40)) AS token1,
      lpscanner.hex_to_int_str(topics[SAFE_OFFSET(3)]) AS fee,
      lpscanner.hex_to_int_str(SUBSTR(data, 3, 64)) AS tickSpacing,
      block_timestamp,
      block_number,
      transaction_hash,
      generatedindex AS generatedIndex
    FROM `lpscanner.ethereum_target_data`
    WHERE address = '0x1f98431c8ad98523631ae4a59f267346ea31f984'
      AND topics[SAFE_OFFSET(0)] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
      AND block_timestamp >= max_ts
  ) AS source
  ON target.pool_address = source.pool_address
  WHEN NOT MATCHED THEN INSERT ROW;
