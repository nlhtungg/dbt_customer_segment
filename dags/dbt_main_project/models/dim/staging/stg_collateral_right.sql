{{
    config(
        materialized='table',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/staging/dimensions'
    )
}}

SELECT
    r.right_id,
    r.collateral_id,
    dcoll.dim_coll_id,
    dcoll.description,
    dcoll.collateral_type,
    r.right_type,
    r.right_holder,
    CAST(r.start_date AS DATE) AS start_date,
    CAST(r.end_date AS DATE) AS end_date,
    MD5(CONCAT(
        COALESCE(r.collateral_id,''),
        COALESCE(dcoll.dim_coll_id,''),
        COALESCE(dcoll.description,''),
        COALESCE(dcoll.collateral_type,''),
        COALESCE(r.right_type,''),
        COALESCE(r.right_holder,''),
        COALESCE(CAST(r.start_date AS DATE),''),
        COALESCE(CAST(r.end_date AS DATE),'')
    )) AS record_hash
FROM {{ ref('t24_collateral_right') }} r
LEFT JOIN {{ ref('dim_collateral') }} dcoll
    ON r.collateral_id = dcoll.collateral_id AND dcoll.dtf_current_flag = 'Y'