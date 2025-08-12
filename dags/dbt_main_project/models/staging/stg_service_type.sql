{{ config(
    materialized = 'table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging'
)}}

SELECT 
    CONCAT('FT_', service_type_code) AS service_type_code,
    CONCAT(service_type_code, '_F') AS description,
    MD5(CONCAT(
        COALESCE(CONCAT(service_type_code, '_F'),''),
        COALESCE('F','')
    )) AS record_hash
FROM (
    SELECT DISTINCT service_type AS service_type_code
    FROM {{ ref('t24_funds_transfer') }}
    WHERE service_type IS NOT NULL
) t1

UNION ALL

SELECT 
    CONCAT('C_', service_type_code) AS service_type_code,
    CONCAT(service_type_code, '_C') AS description,
    MD5(CONCAT(
        COALESCE(CONCAT(service_type_code, '_C'),''),
        COALESCE('C','')
    )) AS record_hash
FROM (
    SELECT DISTINCT card_service_type AS service_type_code
    FROM {{ ref('w4_card_transaction') }}
    WHERE card_service_type IS NOT NULL
) t2