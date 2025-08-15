{{
    config(
        materialized='incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/output/facts'
    )
}}

SELECT
    '{{ var("run_date") }}' AS dtf_Day_ID,
    ft.transaction_id,
    ft.ref_id,
    ft.service_type,
    ft.channel,
    ft.service_name,
    ft.service_detail,
    ft.amount,
    ft.ccy,
    ft.fee_amount,
    ft.description,
    ft.booking_date,
    ft.status,
    ft.from_account,
    ft.to_account
FROM {{ ref('t24_funds_transfer') }} ft
INNER JOIN {{ ref('dim_service_detail') }} sed 
    ON ft.service_detail = sed.service_detail
        AND sed.dtf_current_flag = 'Y'
INNER JOIN {{ ref('dim_service_type') }} ste
    ON CONCAT('FT_', ft.service_type) = ste.service_type_code
        AND ste.dtf_current_flag = 'Y'
INNER JOIN {{ ref('dim_service_name') }} sen
    ON CONCAT('FT_', ft.service_name) = sen.service_name_code
        AND sen.dtf_current_flag = 'Y'
INNER JOIN {{ ref('dim_channel') }} cha
    ON CONCAT('FT_', ft.channel) = cha.channel_code
        AND cha.dtf_current_flag = 'Y'
WHERE CAST(ft.booking_date AS DATE) = '{{ var("run_date") }}'