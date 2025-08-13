{{
    config(
        materialized = 'incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/output/dimensions',
        table_properties={'write.format.default': 'parquet'},
        incremental_strategy='merge',
        unique_key='dim_card_id',
        merge_exclude_columns=['dim_card_id']
    )
}}

WITH staged_card_data AS(
    SELECT * FROM {{ ref('stg_card') }}
)

{{ scd_type_2(
    source_table='staged_card_data',
    unique_key='card_id',
    compare_columns=['account_payment_id','card_type_id','expiry_date',
                    'cvv', 'card_status','opening_branch','card_create_at'],
    surrogate_key_column = 'dim_card_id'
)}}