{{ config(
    materialized='incremental',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/output/dimensions',
    table_properties={'write.format.default': 'parquet'},
    incremental_strategy='merge',
    unique_key='dim_cusSeg_id',
    merge_exclude_columns=['dim_cusSeg_id']
) }}

WITH staged_cusseg_data AS (
    SELECT * FROM {{ ref('stg_customer_segment') }}
)

{{ scd_type_2(
    source_table='staged_cusseg_data',
    unique_key='segment_code',
    compare_columns=['segment_name'],
    surrogate_key_column = 'dim_cusSeg_id'
) }}