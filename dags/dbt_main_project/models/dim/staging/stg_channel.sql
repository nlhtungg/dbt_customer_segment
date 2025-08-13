{{ config(
    materialized = 'table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging/dimensions'
)}}

SELECT 
    CONCAT('FT_', channel_code) AS channel_code,
    CASE channel_code
        WHEN 'MB' THEN 'Mobile Banking'
        WHEN 'IB' THEN 'Internet Banking'
        WHEN 'ATM' THEN 'Giao dịch qua ATM'
        WHEN 'TL' THEN 'Giao dịch tại quầy'
        WHEN 'POS' THEN 'Giao dịch thẻ tại quầy thanh toán'
        WHEN 'VTK' THEN 'Vay/ Tiết kiệm'
        WHEN 'OTHER' THEN 'Kênh khác'
        ELSE 'Không xác định'
    END AS channel_name,
    MD5(CONCAT(
        COALESCE(
            CASE channel_code
                WHEN 'MB' THEN 'Mobile Banking'
                WHEN 'IB' THEN 'Internet Banking'
                WHEN 'ATM' THEN 'Giao dịch qua ATM'
                WHEN 'TL' THEN 'Giao dịch tại quầy'
                WHEN 'POS' THEN 'Giao dịch thẻ tại quầy thanh toán'
                WHEN 'VTK' THEN 'Vay/ Tiết kiệm'
                WHEN 'OTHER' THEN 'Kênh khác'
                ELSE 'Không xác định'
            END, '')
    )) AS record_hash
FROM (
    SELECT DISTINCT channel AS channel_code
    FROM {{ ref('t24_funds_transfer') }}
    WHERE channel IS NOT NULL
) t1

UNION ALL

SELECT 
    CONCAT('C_', channel_code) AS channel_code,
    CASE channel_code
        WHEN 'MB' THEN 'Mobile Banking'
        WHEN 'IB' THEN 'Internet Banking'
        WHEN 'ATM' THEN 'Giao dịch qua ATM'
        WHEN 'TL' THEN 'Giao dịch tại quầy'
        WHEN 'POS' THEN 'Giao dịch thẻ tại quầy thanh toán'
        WHEN 'VTK' THEN 'Vay/ Tiết kiệm'
        WHEN 'OTHER' THEN 'Kênh khác'
        ELSE 'Không xác định'
    END AS channel_name,
    MD5(CONCAT(
        COALESCE(
            CASE channel_code
                WHEN 'MB' THEN 'Mobile Banking'
                WHEN 'IB' THEN 'Internet Banking'
                WHEN 'ATM' THEN 'Giao dịch qua ATM'
                WHEN 'TL' THEN 'Giao dịch tại quầy'
                WHEN 'POS' THEN 'Giao dịch thẻ tại quầy thanh toán'
                WHEN 'VTK' THEN 'Vay/ Tiết kiệm'
                WHEN 'OTHER' THEN 'Kênh khác'
                ELSE 'Không xác định'
            END, '')
    )) AS record_hash
FROM (
    SELECT DISTINCT card_channel AS channel_code
    FROM {{ ref('w4_card_transaction') }}
    WHERE card_channel IS NOT NULL
) t1