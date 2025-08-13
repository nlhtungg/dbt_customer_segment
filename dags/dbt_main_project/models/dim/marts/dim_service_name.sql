{{
    config(
        materialized = 'incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/output/dimensions',
        table_properties={'write.format.default': 'parquet'},
        incremental_strategy='merge',
        unique_key='dim_service_name_id',
        merge_exclude_columns=['dim_service_name_id']
    )
}}

WITH staged_service_name_data AS(
    SELECT * FROM {{ ref('stg_service_name') }}
)

{{ scd_type_2(
    source_table='staged_service_name_data',
    unique_key='service_name_code',
    compare_columns=['description', 'use_for'],
    surrogate_key_column = 'dim_service_name_id'
)}}