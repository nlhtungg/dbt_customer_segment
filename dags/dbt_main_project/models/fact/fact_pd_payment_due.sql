{{
    config(
        materialized='incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/output/facts'
    )
}}

SELECT
    '{{ var("run_date") }}' AS dtf_Day_ID,
    PDPD.Customer_ID AS PD_Customer_ID,
    PDPD.PD_Account_ID,
    Fact_LD.maturity_date AS PD_Maturity_Date,
    PDPD.PD_Num_Overdue_Day,
    Fact_LD.penalty_rate AS PD_Rates_Principle,
    Fact_LD.penalty_spread AS PD_Rates_Interest,
    PDPD.Penalty_Amt_Principal,
    PDPD.Penalty_Amt_Interest,
    PDPD.Tot_Penalty,
    PDPD.PD_Loan_Type
FROM {{ ref('t24_pd_payment_due') }} PDPD
LEFT JOIN {{ ref('fact_loan_and_deposit') }} Fact_LD
    ON PDPD.PD_Account_ID = Fact_LD.Loan_ID
WHERE Fact_LD.loan_status = 'Overdue'
    AND Fact_LD.dtf_day_ID = '{{ var("run_date") }}'
