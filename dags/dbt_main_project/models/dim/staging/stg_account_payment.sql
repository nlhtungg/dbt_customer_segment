{{ config(
    materialized = 'table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging/dimensions'
)}}

SELECT
    accp.account_payment_id,
    dcus.customer_id,
    UPPER(dcus.name) AS account_name,
    accp.account_status,
    dbr.branch_id,
    accp.open_date,
    MD5(CONCAT(
        COALESCE(dcus.customer_id,''),
        COALESCE(UPPER(dcus.name),''),
        COALESCE(accp.account_status,''),
        COALESCE(dbr.branch_id,''),
        COALESCE(accp.open_date,'')
    )) AS record_hash
FROM {{ ref('t24_account_payment') }} accp
LEFT JOIN {{ ref('dim_branch') }} dbr
    ON accp.opening_branch = dbr.branch_id AND dbr.dtf_current_flag='Y'
INNER JOIN {{ ref('dim_customer') }} dcus
    ON accp.customer_id = dcus.customer_id AND dcus.dtf_current_flag='Y'
