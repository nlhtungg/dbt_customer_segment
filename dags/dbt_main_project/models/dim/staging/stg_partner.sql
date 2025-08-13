{{ config(
    materialized='table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging/dimensions'
) }}

SELECT
    partner_code,
    partner_name,
    contact_info,
    bank_details,
    MD5(CONCAT(
        COALESCE(partner_name,''),
        COALESCE(contact_info,''),
        COALESCE(bank_details,'')
    )) AS record_hash
FROM {{ ref('t24_partner') }}