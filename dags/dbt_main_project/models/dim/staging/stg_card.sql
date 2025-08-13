{{ config(
    materialized = 'table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging/dimensions'
)}}

SELECT
    c.card_id,
    daccp.account_payment_id,
    dct.card_type_id,
    c.expiry_date,
    SHA2(CAST(c.cvv AS STRING),256) AS cvv,
    c.card_status,
    dbr.branch_id AS opening_branch,
    c.card_create_at,
    MD5(CONCAT(
        COALESCE(daccp.account_payment_id, ''),
        COALESCE(dct.card_type_id, ''),
        COALESCE(c.expiry_date, ''),
        COALESCE(SHA2(CAST(c.cvv AS STRING),256), ''),
        COALESCE(c.card_status, ''),
        COALESCE(dbr.branch_id, ''),
        COALESCE(c.card_create_at, '')
    )) AS record_hash
FROM {{ ref('w4_card') }} c
INNER JOIN {{ ref('dim_account_payment') }} daccp
    ON c.account_payment_id = daccp.account_payment_id AND daccp.dtf_current_flag='Y'
LEFT JOIN {{ ref('dim_card_type') }} dct
    ON c.card_type = dct.card_description AND dct.dtf_current_flag='Y'
LEFT JOIN {{ ref('dim_branch') }} dbr
    ON c.opening_branch = dbr.branch_id AND dbr.dtf_current_flag='Y'