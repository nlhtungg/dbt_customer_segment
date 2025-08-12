{{ config(
    materialized='incremental',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/output/dimensions',
    table_properties={'write.format.default': 'parquet'},
    incremental_strategy='merge',
    unique_key='dim_collateral_right_id',
    merge_exclude_columns=['dim_collateral_right_id']
) }}

WITH staged_collateral_right_data AS (
    SELECT * FROM {{ ref('stg_collateral_right') }}
)

{{ scd_type_2(
    source_table='staged_collateral_right_data',
    unique_key='right_id',
    compare_columns=['collateral_id', 'dim_coll_id', 'description', 'collateral_type',
                    'right_type', 'right_holder', 'start_date', 'end_date'],
    surrogate_key_column = 'dim_collateral_right_id'
) }}