{{ config(
    materialized = 'table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging/dimensions'
) }}

WITH combined AS (
    SELECT
        service_type       AS service_type_code,
        service_name       AS service_name_code,
        channel            AS service_channel_code
    FROM {{ ref('t24_funds_transfer') }}

    UNION

    SELECT
        card_service_type  AS service_type_code,
        card_service_name  AS service_name_code,
        card_channel       AS service_channel_code
    FROM {{ ref('w4_card_transaction') }}
),

hashed AS ( -- tính hash trước để ORDER BY ổn định
    SELECT
        service_type_code,
        service_name_code,
        service_channel_code,
        MD5(CONCAT(
            COALESCE(service_type_code, ''),
            COALESCE(service_name_code, ''),
            COALESCE(service_channel_code, '')
        )) AS record_hash
    FROM combined
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            record_hash,
            service_type_code,
            service_name_code,
            service_channel_code
    ) AS tnc_key,
    service_type_code,
    service_name_code,
    service_channel_code,
    record_hash
FROM hashed;
 