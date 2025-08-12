{{ config(
    materialized='incremental',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/output/dimensions',
    table_properties={'write.format.default': 'parquet'},
    incremental_strategy='merge',
    unique_key='dim_partner_id',
    merge_exclude_columns=['dim_partner_id']
) }}

WITH staged_partner_data AS (
    SELECT * FROM {{ ref('stg_partner') }}
)

{{
    scd_type_2(
        source_table='staged_partner_data',
        unique_key='partner_code',
        compare_columns=['partner_name', 'contact_info', 'bank_details'],
        surrogate_key_column='dim_partner_id'
    )
}}