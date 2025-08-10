{{ config(
    materialized='incremental',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/output/dimensions',
    table_properties={'write.format.default': 'parquet'},
    incremental_strategy='merge',
    unique_key='dim_branch_id',
    merge_exclude_columns=['dim_branch_id']
) }}

WITH source_data AS (
    SELECT
        branch_code AS branch_id,
        branch_name,
        description,
        parent_branch,
        level_code
    FROM {{ ref('t24_branch') }}
)

{% if is_incremental() %}
,
-- Get all existing data (both current and historical)
existing_data AS (
    SELECT * FROM {{ this }}
),

-- Get only current records
current_records AS (
    SELECT * FROM existing_data
    WHERE dtf_current_flag = 'Y'
),

-- Create hash for comparison
source_with_hash AS (
    SELECT *,
        MD5(CONCAT(
            COALESCE(branch_name, ''),
            COALESCE(description, ''),
            COALESCE(parent_branch, ''),
            CAST(level_code AS STRING)
        )) AS record_hash
    FROM source_data
),

current_with_hash AS (
    SELECT *,
        MD5(CONCAT(
            COALESCE(branch_name, ''),
            COALESCE(description, ''),
            COALESCE(parent_branch, ''),
            CAST(level_code AS STRING)
        )) AS record_hash
    FROM current_records
),

-- Find NEW records (in source but not in current)
new_records AS (
    SELECT s.*
    FROM source_with_hash s
    LEFT JOIN current_with_hash c ON s.branch_id = c.branch_id
    WHERE c.branch_id IS NULL
),

-- Find CHANGED records (same branch_id but different data)
changed_records AS (
    SELECT s.*
    FROM source_with_hash s
    INNER JOIN current_with_hash c ON s.branch_id = c.branch_id
    WHERE s.record_hash != c.record_hash
),

-- Find DELETED records (in current but not in source)
deleted_branch_ids AS (
    SELECT c.branch_id
    FROM current_with_hash c
    LEFT JOIN source_with_hash s ON c.branch_id = s.branch_id
    WHERE s.branch_id IS NULL
),

-- Mark existing current records as expired (for changed and deleted records)
expired_current_records AS (
    SELECT 
        dim_branch_id,
        branch_id,
        branch_name,
        description,
        parent_branch,
        level_code,
        dtf_start_date,
        CURRENT_DATE() AS dtf_end_date,
        'N' AS dtf_current_flag
    FROM current_records
    WHERE branch_id IN (
        SELECT branch_id FROM changed_records
        UNION ALL
        SELECT branch_id FROM deleted_branch_ids
    )
),

-- Create new current records for new and changed data only
new_current_records AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY branch_id) + 
        COALESCE((SELECT MAX(dim_branch_id) FROM existing_data), 0) AS dim_branch_id,
        branch_id,
        branch_name,
        description,
        parent_branch,
        level_code,
        CURRENT_DATE() AS dtf_start_date,
        CAST(NULL AS DATE) AS dtf_end_date,
        'Y' AS dtf_current_flag
    FROM (
        SELECT branch_id, branch_name, description, parent_branch, level_code FROM new_records
        UNION ALL
        SELECT branch_id, branch_name, description, parent_branch, level_code FROM changed_records
    ) combined_new
),

-- Keep unchanged current records as they are
unchanged_current_records AS (
    SELECT *
    FROM current_records
    WHERE branch_id NOT IN (
        SELECT branch_id FROM changed_records
        UNION ALL
        SELECT branch_id FROM deleted_branch_ids
    )
),

-- Keep all historical records
historical_records AS (
    SELECT *
    FROM existing_data
    WHERE dtf_current_flag = 'N'
),

-- Combine everything
final_result AS (
    SELECT * FROM historical_records
    UNION ALL
    SELECT * FROM expired_current_records
    UNION ALL
    SELECT * FROM unchanged_current_records
    UNION ALL
    SELECT * FROM new_current_records
)

SELECT * FROM final_result

{% else %}
-- Initial load
SELECT
    ROW_NUMBER() OVER (ORDER BY branch_id) AS dim_branch_id,
    branch_id,
    branch_name,
    description,
    parent_branch,
    level_code,
    CURRENT_DATE() AS dtf_start_date,
    CAST(NULL AS DATE) AS dtf_end_date,
    'Y' AS dtf_current_flag
FROM source_data

{% endif %}
