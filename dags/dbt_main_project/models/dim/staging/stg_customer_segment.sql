{{ config(
    materialized='table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging/dimensions'
) }}

SELECT
    segment_code,
    segment_name,
    -- Create a hash for change detection
    MD5(CONCAT(
        COALESCE(segment_name, '')
    )) AS record_hash
FROM {{ ref('t24_customer_segment') }}