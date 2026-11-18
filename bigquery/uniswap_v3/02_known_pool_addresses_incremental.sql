MERGE `lpscanner.ethereum_known_pool_addresses` AS target
USING (
  SELECT DISTINCT
    CASE
      WHEN address = '0x1f98431c8ad98523631ae4a59f267346ea31f984' THEN 'uniswap_v3'
      WHEN address = '0xbaceb8ec6b9355dfc0269c18bac9d6e2bdc29c4f' THEN 'sushiswap_v3'
      WHEN address = '0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865' THEN 'pancakeswap_v3'
    END AS protocol,
    LOWER(CONCAT('0x', SUBSTR(data, 91, 40))) AS pool_address
  FROM `lpscanner.ethereum_target_data`
  WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
    AND address IN (
      '0x1f98431c8ad98523631ae4a59f267346ea31f984',
      '0xbaceb8ec6b9355dfc0269c18bac9d6e2bdc29c4f',
      '0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865'
    )
    AND topics[SAFE_OFFSET(0)] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
) AS source
ON target.pool_address = source.pool_address
WHEN NOT MATCHED THEN
  INSERT (protocol, pool_address)
  VALUES (source.protocol, source.pool_address);
