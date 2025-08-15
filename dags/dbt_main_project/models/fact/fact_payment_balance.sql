{{
    config(
        materialized='incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/output/facts'
    )
}}

WITH tmp AS (
    SELECT 
        From_Account as Account_Payment_ID,
        SUM(Amount) AS amount
    FROM {{ ref('t24_funds_transfer') }}
    GROUP BY From_Account
)

SELECT
    '{{ var("run_date") }}' AS dtf_Day_ID,
    COALESCE(ACCP.Account_Balance - tmp.amount, ACCP.Account_Balance) AS Account_Balance,
    ACCP.Account_Payment_ID AS Dim_Account_Payment_ID,
    ACCP.Customer_ID AS Dim_Customer_ID,
    ACCP.Updated_At AS Dim_Updated
FROM {{ ref('t24_account_payment') }} ACCP
INNER JOIN {{ ref('dim_customer') }} DCUS
    ON ACCP.Customer_ID = DCUS.Customer_ID
INNER JOIN {{ ref('dim_account_payment') }} DACCP
    ON ACCP.Account_Payment_ID = DACCP.account_payment_id
    AND DACCP.dtf_current_flag = 'Y'
LEFT JOIN tmp
    ON ACCP.Account_Payment_ID = tmp.Account_Payment_ID
WHERE ACCP.Account_Payment_ID IS NOT NULL
