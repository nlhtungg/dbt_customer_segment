{{ config(
    materialized='incremental',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/output/dimensions',
    table_properties={'write.format.default': 'parquet'},
    incremental_strategy='merge',
    unique_key='dim_branch_id',
    merge_exclude_columns=['dim_branch_id']
) }}

WITH staged_branch_data AS (
    SELECT * FROM {{ ref('stg_branch') }}
)

{{ scd_type_2(
    source_table='staged_branch_data',
    unique_key='branch_id',
    compare_columns=['branch_name', 'description', 'parent_branch', 'level_code'],
    surrogate_key_column = 'dim_branch_id'
) }}