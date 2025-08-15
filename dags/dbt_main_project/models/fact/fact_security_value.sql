{{
    config(
        materialized='incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/output/facts'
    )
}}

SELECT
    '{{ var("run_date") }}' AS dtf_Day_ID,
    dsec.dim_security_id AS Dim_Security_ID,
    sec.purchase_price AS purchase_price,
    sec.market_value AS market_value,
    sec.purchase_date AS purchase_date
FROM {{ ref('t24_security') }} sec
INNER JOIN {{ ref('dim_security') }} dsec
    ON sec.security_id = dsec.security_id
WHERE sec.security_id IS NOT NULL