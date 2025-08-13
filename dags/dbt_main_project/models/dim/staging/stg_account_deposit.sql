{{ config(
    materialized='table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging/dimensions'
) }}

SELECT
    accd.deposit_id,
    dcus.customer_id,
    accd.deposit_type,
    accd.deposit_term,
    CAST(accd.maturity_date AS DATE) AS maturity_date,
    dbr.dim_branch_id,
    dbr.branch_id,
    CAST(accd.created AS DATE) AS created_at,
    CAST(accd.updated AS DATE) AS updated_at,
    MD5(CONCAT(
        COALESCE(dcus.customer_id,''),
        COALESCE(accd.deposit_type,''),
        COALESCE(accd.deposit_term,''),
        COALESCE(CAST(accd.maturity_date AS DATE),''),
        COALESCE(dbr.dim_branch_id,''),
        COALESCE(dbr.branch_id,''),
        COALESCE(CAST(accd.created AS DATE),''),
        COALESCE(CAST(accd.updated AS DATE),'')
    )) AS record_hash
FROM {{ ref('t24_account_deposit') }} accd
LEFT JOIN {{ ref('dim_customer') }} dcus
    ON accd.customer_id = dcus.customer_id AND dcus.dtf_current_flag = 'Y'
LEFT JOIN {{ ref('dim_branch') }} dbr
    ON accd.opening_branch = dbr.branch_id AND dbr.dtf_current_flag = 'Y'