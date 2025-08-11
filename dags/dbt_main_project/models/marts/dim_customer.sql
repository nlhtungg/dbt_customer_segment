{{
    config(
        materialized = 'incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/dimensions',
        table_properties={'write.format.default': 'parquet'},
        incremental_strategy='merge',
        unique_key='dim_customer_id',
        merge_exclude_columns=['dim_customer_id']
    )
}}

{{ scd_type_2(
    source_table=ref('stg_customer'),
    unique_key='customer_id',
    compare_columns=['name', 'segment_code', 'address', 'dim_branch_id', 'dim_industry_id'],
    surrogate_key_column = 'dim_customer_id'
)}}