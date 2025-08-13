{{ config(
    materialized='incremental',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/output/dimensions',
    table_properties={'write.format.default': 'parquet'},
    incremental_strategy='merge',
    unique_key='dim_security_id',
    merge_exclude_columns=['dim_security_id']
) }}

WITH staged_security_data AS (
    SELECT * FROM {{ ref('stg_security') }}
)

{{ scd_type_2(
    source_table='staged_security_data',
    unique_key='security_id',
    compare_columns=['security_type', 'partner_code', 'dim_partner_id'],
    surrogate_key_column = 'dim_security_id'
) }}