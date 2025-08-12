{{ config(
    materialized='incremental',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/output/dimensions',
    table_properties={'write.format.default': 'parquet'},
    incremental_strategy='merge',
    unique_key='dim_account_deposit_id',
    merge_exclude_columns=['dim_account_deposit_id']
) }}

WITH staged_account_deposit_data AS (
    SELECT * FROM {{ ref('stg_account_deposit') }}
)

{{ scd_type_2(
    source_table='staged_account_deposit_data',
    unique_key='deposit_id',
    compare_columns=['customer_id', 'deposit_type', 'deposit_term',
                    'maturity_date', 'dim_branch_id', 'branch_id',
                    'created_at', 'updated_at'],
    surrogate_key_column = 'dim_account_deposit_id'
) }}