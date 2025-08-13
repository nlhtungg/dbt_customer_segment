{{
    config(
        materialized = 'incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/output/dimensions',
        table_properties={'write.format.default': 'parquet'},
        incremental_strategy='merge',
        unique_key='dim_type_name_channel_id',
        merge_exclude_columns=['dim_type_name_channel_id']
    )
}}

WITH staged_tnc_data AS(
    SELECT * FROM {{ ref('stg_type_name_channel') }}
)

{{ scd_type_2(
    source_table='staged_tnc_data',
    unique_key='tnc_key',
    compare_columns=['service_type_code','service_name_code','service_channel_code'],
    surrogate_key_column = 'dim_type_name_channel_id'
)}}