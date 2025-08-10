{{ config(
    materialized='table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging/t24_branch'
) }}

SELECT
    branch_code AS branch_id,
    branch_name,
    description,
    parent_branch,
    level_code,
    -- Create a hash for change detection
    MD5(CONCAT(
        COALESCE(branch_name, ''),
        COALESCE(description, ''),
        COALESCE(parent_branch, ''),
        CAST(level_code AS STRING)
    )) AS record_hash
FROM {{ ref('t24_branch') }}
