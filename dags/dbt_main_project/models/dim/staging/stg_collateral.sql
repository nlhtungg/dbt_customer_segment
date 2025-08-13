{{
    config(
        materialized='table',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/staging/dimensions'
    )
}}

SELECT
    col.*,
    dcu.dim_customer_id,
    MD5(CONCAT(
        COALESCE(col.owner_id, ''),
        COALESCE(col.collateral_type, ''),
        COALESCE(col.collateral_value, ''),
        COALESCE(col.description, ''),
        COALESCE(dcu.dim_customer_id, '')
    )) AS record_hash
FROM {{ ref('t24_collateral') }} col
LEFT JOIN {{ ref('dim_customer') }} dcu 
    ON col.owner_id = dcu.customer_id AND dcu.dtf_current_flag = 'Y'