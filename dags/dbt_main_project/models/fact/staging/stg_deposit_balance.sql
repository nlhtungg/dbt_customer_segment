{{ config(
   materialized = 'table',
   file_format='iceberg',
   location_root='s3a://iceberg-warehouse/staging/facts'
) }}

-- CTE for funds transfer aggregation (tmp table)
WITH tmp AS (
    SELECT 
        CASE 
            WHEN ft.to_account LIKE 'DEP%' THEN ft.to_account
            WHEN ft.from_account LIKE 'DEP%' THEN ft.from_account
            ELSE NULL
        END AS account_deposit_id,
        SUM(
            CASE 
                WHEN ft.to_account LIKE 'DEP%' THEN ft.amount  -- Money coming in (positive)
                WHEN ft.from_account LIKE 'DEP%' THEN -ft.amount  -- Money going out (negative)
                ELSE 0
            END
        ) AS transaction_amount
    FROM {{ ref('t24_funds_transfer') }} ft
    WHERE ft.status = 'Completed'
    GROUP BY 
        CASE 
            WHEN ft.to_account LIKE 'DEP%' THEN ft.to_account
            WHEN ft.from_account LIKE 'DEP%' THEN ft.from_account
            ELSE NULL
        END
),

-- Main data from account deposit with updated balance calculation
base_data AS (
    SELECT 
        accd.deposit_id AS account_deposit_id,
        accd.customer_id,
        accd.balance AS original_balance,
        accd.interest_rate,
        tmp.transaction_amount,
        -- Calculate updated balance: original balance + transaction amounts
        COALESCE(accd.balance + COALESCE(tmp.transaction_amount, 0), accd.balance) AS updated_balance,
        accd.created,
        accd.updated
    FROM {{ ref('t24_account_deposit') }} accd
    LEFT JOIN tmp ON accd.deposit_id = tmp.account_deposit_id
)

SELECT 
    account_deposit_id,
    customer_id,
    original_balance,
    transaction_amount,
    updated_balance AS balance,
    interest_rate,
    created,
    updated
FROM base_data
WHERE account_deposit_id IS NOT NULL