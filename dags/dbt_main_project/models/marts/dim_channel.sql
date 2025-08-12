{{
    config(
        materialized = 'incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/dimensions',
        table_properties={'write.format.default': 'parquet'},
        incremental_strategy='merge',
        unique_key='dim_channel_id',
        merge_exclude_columns=['dim_channel_id']
    )
}}

WITH staged_channel_data AS (
    SELECT * FROM {{ ref('stg_channel') }}
)

{{ scd_type_2(
    source_table='staged_channel_data',
    unique_key='channel_code',
    compare_columns=['channel_name'],
    surrogate_key_column = 'dim_channel_id'
)}}