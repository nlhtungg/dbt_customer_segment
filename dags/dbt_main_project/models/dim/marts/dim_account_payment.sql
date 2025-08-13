{{
    config(
        materialized = 'incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/output/dimensions',
        table_properties={'write.format.default': 'parquet'},
        incremental_strategy='merge',
        unique_key='dim_account_payment_id',
        merge_exclude_columns=['dim_account_payment_id']
    )
}}

WITH staged_account_payment_data AS(
    SELECT * FROM {{ ref('stg_account_payment') }}
)

{{ scd_type_2(
    source_table='staged_account_payment_data',
    unique_key='account_payment_id',
    compare_columns=['customer_id','account_name','account_status','branch_id','open_date'],
    surrogate_key_column = 'dim_account_payment_id'
)}}