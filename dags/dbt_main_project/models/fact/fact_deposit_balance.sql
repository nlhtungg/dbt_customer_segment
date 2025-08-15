{{
    config(
        materialized='incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/output/facts'
    )
}}

WITH tmp AS (
    SELECT
        From_Account as Account_Deposit_ID,
        SUM(Amount) AS amount
    FROM {{ ref('t24_funds_transfer') }}
    GROUP BY From_Account
)

SELECT
    '{{ var("run_date")}}' AS dtf_Day_ID,
    DAD.Dim_Account_Deposit_ID AS Dim_Deposit_ID,
    COALESCE(AD.Balance - tmp.amount, AD.Balance) AS Balance,
    AD.interest_rate AS Interest_Rate,
    AD.Customer_ID AS Dim_Customer_ID,
    AD.Updated AS Dim_Updated
FROM {{ ref('t24_account_deposit') }} AD
INNER JOIN {{ ref('dim_customer') }} DCUS
    ON AD.Customer_ID = DCUS.Customer_ID
INNER JOIN {{ ref('dim_account_deposit') }} DAD
    ON AD.Deposit_ID = DAD.deposit_id
    AND DAD.dtf_current_flag = 'Y'
LEFT JOIN tmp
    ON AD.Deposit_ID = tmp.Account_Deposit_ID
WHERE AD.Deposit_ID IS NOT NULL

    