{{ config(
    materialized='table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging'
) }}

SELECT
    industry_id AS industry_id,
    industry_name,
    description,
    created_at,
    -- Create a hash for change detection
    MD5(CONCAT(
        COALESCE(industry_name, ''),
        COALESCE(description, ''),
        COALESCE(created_at, '')
    )) AS record_hash
FROM {{ ref('t24_industry') }}