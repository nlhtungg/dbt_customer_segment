{{
    config(
        materialized = 'incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/output/dimensions',
        table_properties={'write.format.default': 'parquet'},
        incremental_strategy='merge',
        unique_key='dim_customer_id',
        merge_exclude_columns=['dim_customer_id']
    )
}}

WITH staged_customer_data AS (
    SELECT * FROM {{ ref('stg_customer') }}
)

{{ scd_type_2(
    source_table='staged_customer_data',
    unique_key='customer_id',
    compare_columns=['name', 'date_of_birth', 'segment_code', 'dim_cusseg_id',
                    'address', 'industry_code', 'dim_industry_id',
                    'marital_status', 'income_level', 'branch_code', 'dim_branch_id'],
    surrogate_key_column = 'dim_customer_id'
)}}