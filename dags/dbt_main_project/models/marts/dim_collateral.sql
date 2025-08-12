{{ config(
    materialized='incremental',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/output/dimensions',
    table_properties={'write.format.default': 'parquet'},
    incremental_strategy='merge',
    unique_key='dim_coll_id',
    merge_exclude_columns=['dim_coll_id']
) }}

WITH staged_collateral_data AS (
    SELECT * FROM {{ ref('stg_collateral') }}
)

{{ scd_type_2(
    source_table='staged_collateral_data',
    unique_key='collateral_id',
    compare_columns=['owner_id', 'collateral_type', 'collateral_value', 'description', 'dim_customer_id'],
    surrogate_key_column = 'dim_coll_id'
) }}