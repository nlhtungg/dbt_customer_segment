{{ config(
    materialized='incremental',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/output/dimensions',
    table_properties={'write.format.default': 'parquet'},
    incremental_strategy='merge',
    unique_key='dim_industry_id',
    merge_exclude_columns=['dim_industry_id']
) }}

WITH staged_industry_data AS (
    SELECT * FROM {{ ref('stg_t24_industry') }}
)

{{ scd_type_2(
    source_table='staged_industry_data',
    unique_key='industry_id',
    compare_columns=['industry_name', 'description', 'created_at']
) }}
