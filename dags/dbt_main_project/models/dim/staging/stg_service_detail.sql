{{ config(
   materialized = 'table',
   file_format='iceberg',
   location_root='s3a://iceberg-warehouse/staging/dimensions'
) }}

WITH combined AS (
   -- FT
   SELECT DISTINCT
       CONCAT('FT_', service_name) AS service_name_code,
       service_detail
   FROM {{ ref('t24_funds_transfer') }}
   WHERE service_name IS NOT NULL
   UNION ALL
   -- Card
   SELECT DISTINCT
       CONCAT('C_', card_service_name) AS service_name_code,
       card_service_detail AS service_detail
   FROM {{ ref('w4_card_transaction') }}
   WHERE card_service_name IS NOT NULL
)

SELECT
   {{ generate_code('SD', 'service_name_code, service_detail', 2) }} AS service_detail_code,
   service_name_code,
   service_detail,
   MD5(CONCAT(
       COALESCE(service_name_code, ''),
       COALESCE(service_detail, '')
   )) AS record_hash
FROM combined;