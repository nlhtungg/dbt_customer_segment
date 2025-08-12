{{ config(
    materialized='incremental',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/output/dimensions',
    table_properties={'write.format.default': 'parquet'},
    incremental_strategy='merge',
    unique_key='dim_currency_id',
    merge_exclude_columns=['dim_currency_id']
) }}

WITH staged_currency_data AS (
    SELECT * FROM {{ ref('stg_currency') }}
)

{{ scd_type_2(
    source_table='staged_currency_data',
    unique_key='currency_code',
    compare_columns=['currency_name', 'exchange_rate', 'last_updated'],
    surrogate_key_column = 'dim_currency_id'
) }}