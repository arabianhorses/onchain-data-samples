DECLARE max_ts TIMESTAMP;
DECLARE max_gi INT64;

SET (max_ts, max_gi) = (
  SELECT AS STRUCT MAX(block_timestamp), MAX(generatedindex)
  FROM `lpscanner.ethereum_target_data`
  WHERE block_timestamp > CURRENT_TIMESTAMP() - INTERVAL 48 HOUR
);

IF max_ts IS NULL THEN
  SET (max_ts, max_gi) = (
    SELECT AS STRUCT MAX(block_timestamp), MAX(generatedindex)
    FROM `lpscanner.ethereum_target_data`
    WHERE block_timestamp > TIMESTAMP_TRUNC(CURRENT_TIMESTAMP() - INTERVAL 30 DAY, MONTH)
  );
END IF;

IF max_ts IS NULL THEN
  SET max_ts = TIMESTAMP('2021-05-04');
  SET max_gi = 0;
END IF;

MERGE `lpscanner.ethereum_target_data` AS target
USING (

  SELECT
    l.block_timestamp,
    l.block_number,
    l.transaction_hash,
    l.log_index,
    l.address,
    l.topics,
    l.data,
    l.block_number * 1000000 + l.log_index AS generatedindex
  FROM `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.logs` l
  INNER JOIN (
    SELECT DISTINCT target_address FROM (

      SELECT target_address FROM UNNEST([
        '0x1f98431c8ad98523631ae4a59f267346ea31f984',
        '0xc36442b4a4522e871399cd717abdd847ab11fe88',
        '0xbaceb8ec6b9355dfc0269c18bac9d6e2bdc29c4f',
        '0x2214a42d8e2a1d20635c2cb0664422c528b6a432',
        '0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865',
        '0x46a15b0b27311cedf172ab29e4f4766fbe7f4364',
        '0x000000000004444c5dc75cb358380d2e3de08a90',
        '0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e',
        '0xe0e0e08a6a4b9dc7bd67bcb7aade5cf48157d444',
        '0xa37cc341634afd9e0919d334606e676dbab63e17'
      ]) AS target_address

      UNION ALL

      SELECT pool_address AS target_address
      FROM `lpscanner.ethereum_known_pool_addresses`
    )
  ) t ON l.address = t.target_address
  WHERE l.block_timestamp >= max_ts
    AND l.removed = false
    AND l.block_number * 1000000 + l.log_index > max_gi

) AS source
ON target.generatedindex = source.generatedindex
WHEN NOT MATCHED THEN INSERT ROW;
