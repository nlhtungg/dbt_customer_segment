{{
    config(
        materialized='table',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/staging/dimensions'
    )
}}

SELECT 
    CONCAT('CT0',ROW_NUMBER() OVER (ORDER BY card_description)) AS card_type_id,
    card_description,
    MD5(CONCAT(COALESCE(card_description,''))) AS record_hash
FROM (
    SELECT DISTINCT card_type AS card_description
    FROM {{ ref('w4_card') }}
    ORDER BY card_description
)