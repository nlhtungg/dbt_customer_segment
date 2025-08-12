{{ config(
    materialized='table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging'
) }}

SELECT
    se.security_id,
    se.security_type,
    se.partner_code,
    p.dim_partner_id,
    MD5(CONCAT(
        COALESCE(se.security_type,''),
        COALESCE(se.partner_code,''),
        COALESCE(p.dim_partner_id,'')
    )) AS record_hash
FROM {{ ref('t24_security') }} se
LEFT JOIN {{ ref('dim_partner') }} p 
    ON se.partner_code = p.partner_code AND p.dtf_current_flag = 'Y'
    