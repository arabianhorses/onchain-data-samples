-- lpNFTTransfers — incremental MERGE
-- Schedule: every 2 hours at :30 (after pipeline finishes)
-- Scans ~230 MB per run (~$0.03/month)
-- Tracks current NFT owner per position (protocol, salt) from ERC-721 Transfer events

MERGE `lpscanner.lpNFTTransfers` AS target
USING (
  WITH new_transfers AS (
    SELECT
      address AS position_manager,
      CASE
        WHEN address = '0x3fed017ec0f5517cdf2e8a9a4156c64d74252146' THEN 'blackhole'
        WHEN address = '0xb74b1f14d2754acfcbbe1a221023a5cf50ab8acd' THEN 'uniswap_v4'
        WHEN address = '0x655c406ebfa14ee2006250925e54ec43ad184f8b' THEN 'uniswap_v3'
        WHEN address = '0xf40937279f38d0c1f97afa5919f1cb3cb7f06a7f' THEN 'pangolin_v3'
      END AS protocol,
      'avalanche_c' AS chain,
      CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(1)], 27)) AS from_address,
      CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(2)], 27)) AS to_address,
      CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(3)], 3, 64)) AS salt,
      block_timestamp,
      generatedindex
    FROM `lpscanner.avalanche_c_targetData`
    WHERE topics[SAFE_OFFSET(0)] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
      AND address IN (
        '0x3fed017ec0f5517cdf2e8a9a4156c64d74252146',
        '0xb74b1f14d2754acfcbbe1a221023a5cf50ab8acd',
        '0x655c406ebfa14ee2006250925e54ec43ad184f8b',
        '0xf40937279f38d0c1f97afa5919f1cb3cb7f06a7f'
      )
      AND generatedindex > COALESCE(
        (SELECT MAX(generatedindex) FROM `lpscanner.lpNFTTransfers`), 0
      )
      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
      AND CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(2)], 27)) != '0x0000000000000000000000000000000000000000'
  ),
  ranked AS (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY protocol, salt ORDER BY generatedindex DESC) AS rn
    FROM new_transfers
  )
  SELECT protocol, salt, to_address AS current_owner, from_address AS previous_owner,
         block_timestamp AS last_transfer_time, chain, generatedindex
  FROM ranked
  WHERE rn = 1
) AS source
ON target.protocol = source.protocol AND target.salt = source.salt
WHEN MATCHED AND source.generatedindex > target.generatedindex THEN
  UPDATE SET
    current_owner = source.current_owner,
    previous_owner = source.previous_owner,
    last_transfer_time = source.last_transfer_time,
    generatedindex = source.generatedindex
WHEN NOT MATCHED THEN
  INSERT (protocol, salt, current_owner, previous_owner, last_transfer_time, chain, generatedindex)
  VALUES (source.protocol, source.salt, source.current_owner, source.previous_owner,
          source.last_transfer_time, source.chain, source.generatedindex)
