DECLARE max_ts TIMESTAMP;

  SET max_ts = (
    SELECT MAX(block_timestamp)
    FROM `lpscanner.ethereum_uniswapv3_swaps`
    WHERE block_timestamp > CURRENT_TIMESTAMP() - INTERVAL 48 HOUR
  );

  IF max_ts IS NULL THEN
    SET max_ts = (
      SELECT MAX(block_timestamp)
      FROM `lpscanner.ethereum_uniswapv3_swaps`
      WHERE block_timestamp > TIMESTAMP_TRUNC(CURRENT_TIMESTAMP() - INTERVAL 30 DAY, MONTH)
    );
  END IF;

  IF max_ts IS NULL THEN
    SET max_ts = TIMESTAMP('2021-05-04');
  END IF;

  MERGE `lpscanner.ethereum_uniswapv3_swaps` AS target
  USING (
    SELECT
      'ethereum' AS chain,
      'uniswap_v3' AS protocol,
      t.address AS pool_address,
      CONCAT('0x', SUBSTR(t.topics[SAFE_OFFSET(1)], 27, 40)) AS sender,
      CONCAT('0x', SUBSTR(t.topics[SAFE_OFFSET(2)], 27, 40)) AS recipient,
      lpscanner.hex_to_int_str(SUBSTR(t.data, 3, 64)) AS amount0,
      lpscanner.hex_to_int_str(SUBSTR(t.data, 67, 64)) AS amount1,
      lpscanner.hex_to_uint(SUBSTR(t.data, 131, 64)) AS sqrtPriceX96,
      lpscanner.hex_to_int_str(SUBSTR(t.data, 195, 64)) AS liquidity,
      lpscanner.hex_to_int_str(SUBSTR(t.data, 259, 64)) AS tick,
      t.transaction_hash,
      t.block_timestamp,
      t.block_number,
      t.log_index,
      t.generatedindex AS generatedIndex
    FROM `lpscanner.ethereum_target_data` t
    INNER JOIN `lpscanner.ethereum_known_pool_addresses` kp
      ON kp.pool_address = t.address
    WHERE kp.protocol = 'uniswap_v3'
      AND t.topics[SAFE_OFFSET(0)] = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
      AND t.block_timestamp >= max_ts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t.generatedindex ORDER BY t.block_timestamp) = 1
  ) AS source
  ON target.generatedIndex = source.generatedIndex
  WHEN NOT MATCHED THEN INSERT ROW;



  SET max_ts = (
    SELECT MAX(block_timestamp)
    FROM `lpscanner.ethereum_uniswapv3_liquidity_movements`
    WHERE block_timestamp > CURRENT_TIMESTAMP() - INTERVAL 48 HOUR
  );

  IF max_ts IS NULL THEN
    SET max_ts = (
      SELECT MAX(block_timestamp)
      FROM `lpscanner.ethereum_uniswapv3_liquidity_movements`
      WHERE block_timestamp > TIMESTAMP_TRUNC(CURRENT_TIMESTAMP() - INTERVAL 30 DAY, MONTH)
    );
  END IF;

  IF max_ts IS NULL THEN
    SET max_ts = TIMESTAMP('2021-05-04');
  END IF;

  MERGE `lpscanner.ethereum_uniswapv3_liquidity_movements` AS target
  USING (
      SELECT
        'ethereum' AS chain,
        'uniswap_v3' AS protocol,
        mint.address AS pool_address,
        incl.topics[SAFE_OFFSET(1)] AS salt,
        CONCAT('0x', SUBSTR(mint.topics[SAFE_OFFSET(1)], 27, 40)) AS ownerAddress,
        lpscanner.hex_to_int_str(SUBSTR(mint.topics[SAFE_OFFSET(2)], 3, 64)) AS tickLower,
        lpscanner.hex_to_int_str(SUBSTR(mint.topics[SAFE_OFFSET(3)], 3, 64)) AS tickUpper,
        lpscanner.hex_to_int_str(SUBSTR(mint.data, 67, 64)) AS liquidityDelta,
        lpscanner.hex_to_int_str(SUBSTR(mint.data, 131, 64)) AS amount0,
        lpscanner.hex_to_int_str(SUBSTR(mint.data, 195, 64)) AS amount1,
        mint.transaction_hash,
        mint.block_timestamp,
        mint.block_number,
        mint.log_index,
        mint.generatedindex AS generatedIndex
      FROM `lpscanner.ethereum_target_data` mint
      LEFT JOIN `lpscanner.ethereum_target_data` incl
        ON incl.transaction_hash = mint.transaction_hash
        AND incl.block_timestamp = mint.block_timestamp
        AND SUBSTR(incl.data, 3, 64) = SUBSTR(mint.data, 67, 64)
        AND incl.topics[SAFE_OFFSET(0)] = '0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f'
        AND incl.address = '0xc36442b4a4522e871399cd717abdd847ab11fe88'
        AND incl.block_timestamp >= max_ts
      INNER JOIN `lpscanner.ethereum_known_pool_addresses` kp
        ON kp.pool_address = mint.address
        AND kp.protocol = 'uniswap_v3'
      WHERE mint.topics[SAFE_OFFSET(0)] = '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde'
        AND mint.block_timestamp >= max_ts
      QUALIFY ROW_NUMBER() OVER (PARTITION BY mint.transaction_hash, mint.log_index ORDER BY incl.log_index) = 1

      UNION ALL

      SELECT
        'ethereum' AS chain,
        'uniswap_v3' AS protocol,
        burn.address AS pool_address,
        decl.topics[SAFE_OFFSET(1)] AS salt,
        CONCAT('0x', SUBSTR(burn.topics[SAFE_OFFSET(1)], 27, 40)) AS ownerAddress,
        lpscanner.hex_to_int_str(SUBSTR(burn.topics[SAFE_OFFSET(2)], 3, 64)) AS tickLower,
        lpscanner.hex_to_int_str(SUBSTR(burn.topics[SAFE_OFFSET(3)], 3, 64)) AS tickUpper,
        lpscanner.hex_to_neg_int_str(SUBSTR(burn.data, 3, 64)) AS liquidityDelta,
        lpscanner.hex_to_neg_int_str(SUBSTR(burn.data, 67, 64)) AS amount0,
        lpscanner.hex_to_neg_int_str(SUBSTR(burn.data, 131, 64)) AS amount1,
        burn.transaction_hash,
        burn.block_timestamp,
        burn.block_number,
        burn.log_index,
        burn.generatedindex AS generatedIndex
      FROM `lpscanner.ethereum_target_data` burn
      LEFT JOIN `lpscanner.ethereum_target_data` decl
        ON decl.transaction_hash = burn.transaction_hash
        AND decl.block_timestamp = burn.block_timestamp
        AND SUBSTR(decl.data, 3, 64) = SUBSTR(burn.data, 3, 64)
        AND decl.topics[SAFE_OFFSET(0)] = '0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4'
        AND decl.address = '0xc36442b4a4522e871399cd717abdd847ab11fe88'
        AND decl.block_timestamp >= max_ts
      INNER JOIN `lpscanner.ethereum_known_pool_addresses` kp
        ON kp.pool_address = burn.address
        AND kp.protocol = 'uniswap_v3'
      WHERE burn.topics[SAFE_OFFSET(0)] = '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c'
        AND burn.block_timestamp >= max_ts
      QUALIFY ROW_NUMBER() OVER (PARTITION BY burn.transaction_hash, burn.log_index ORDER BY decl.log_index) = 1
  ) AS source
  ON target.generatedIndex = source.generatedIndex
  WHEN NOT MATCHED THEN INSERT ROW;
