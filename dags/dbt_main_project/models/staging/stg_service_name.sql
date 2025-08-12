{{ config(
    materialized = 'table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging'
)}}

SELECT 
    CONCAT('FT_', service_name_code) AS service_name_code,
    CONCAT(service_name_code, '_F') AS description,
    'F' AS use_for,
    MD5(CONCAT(
        COALESCE(CONCAT(service_name_code, '_F'),''),
        COALESCE('F','')
    )) AS record_hash
FROM (
    SELECT DISTINCT service_name AS service_name_code
    FROM {{ ref('t24_funds_transfer') }}
    WHERE service_name IS NOT NULL
) t1

UNION ALL

SELECT 
    CONCAT('C_', service_name_code) AS service_name_code,
    CONCAT(service_name_code, '_C') AS description,
    'C' AS use_for,
    MD5(CONCAT(
        COALESCE(CONCAT(service_name_code, '_C'),''),
        COALESCE('C','')
    )) AS record_hash
FROM (
    SELECT DISTINCT card_service_name AS service_name_code
    FROM {{ ref('w4_card_transaction') }}
    WHERE card_service_name IS NOT NULL
) t2