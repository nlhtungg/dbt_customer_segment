{{ config(
    materialized='table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/output/orders',
    table_properties={'write.format.default': 'parquet'}
) }}

SELECT
    date,
    COUNT(*) as order_count
FROM {{ ref('orders') }}
WHERE date > '2018-01-01'
GROUP BY date