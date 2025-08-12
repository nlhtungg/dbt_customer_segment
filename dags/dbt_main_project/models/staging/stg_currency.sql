{{
    config(
        materialized='table',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/staging'
    )
}}

SELECT
    currency_code,
    currency_name,
    exchange_rate,
    last_updated,
    MD5(CONCAT(
        COALESCE(currency_name, ''),
        COALESCE(exchange_rate, ''),
        COALESCE(last_updated, '')
    )) as record_hash
FROM {{ ref('t24_currency') }}