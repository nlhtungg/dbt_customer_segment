{{
    config(
        materialized='incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/output/facts'
    )
}}

SELECT
    '{{ var("run_date") }}' AS dtf_Day_ID,
    cat.card_transaction_id,
    ca.card_id,
    cat.card_service_type,
    cat.card_channel,
    cat.card_service_name,
    sed.service_detail_code AS service_detail_id,
    cat.amount,
    cat.description,
    cat.transaction_date
FROM {{ ref('w4_card_transaction') }} cat
INNER JOIN {{ ref('dim_service_detail') }} sed 
    ON cat.card_service_detail = sed.service_detail
        AND sed.dtf_current_flag = 'Y'
INNER JOIN {{ ref('dim_card') }} ca 
    ON cat.card_id = ca.card_id
        AND ca.dtf_current_flag = 'Y'
INNER JOIN {{ ref('dim_service_type') }} ste
    ON CONCAT('C_', cat.card_service_type) = ste.service_type_code
        AND ste.dtf_current_flag = 'Y'
INNER JOIN {{ ref('dim_service_name') }} sen
    ON CONCAT('C_', cat.card_service_name) = sen.service_name_code
        AND sen.dtf_current_flag = 'Y'
INNER JOIN {{ ref('dim_channel') }} cha
    ON CONCAT('C_', cat.card_channel) = cha.channel_code
        AND cha.dtf_current_flag = 'Y'
WHERE CAST(cat.transaction_date AS DATE) = '{{ var("run_date") }}'