{{
    config(
        materialized='incremental',
        file_format='iceberg',
        location_root='s3a://iceberg-warehouse/output/facts'
    )
}}

SELECT
    '{{ var("run_date") }}' AS dtf_Day_ID,
    LD.Loan_ID,
    DCUS.Dim_Customer_ID,
    LD.Collateral_ID,
    DColl.collateral_type AS coll_type,
    DColl.description AS coll_description,
    DColR.dim_collateral_right_id AS dim_right_id,
    LD.Loan_Type,
    LD.Interest_Rate,
    LD.Loan_Term,
    LD.Disbursement_Date,
    LD.maturity_date,
    LD.Disbursement_Amount,
    LD.Outstanding_Balance,
    LD.Industry_Code,
    DIND.dim_industry_id AS dim_industry_id,
    LD.Loan_Status,
    LD.Payment_Schedule,
    LD.Penalty_Rate,
    LD.Penalty_Spread,
    DBR.dim_branch_id AS dim_branch_id,
    DBR.branch_name,
    LD.Created_At,
    LD.Update_At
FROM {{ ref('t24_loan_and_deposit') }} LD
LEFT JOIN {{ ref('dim_customer')}} DCUS
    ON LD.Customer_ID = DCUS.Customer_ID AND DCUS.dtf_current_flag = 'Y'
LEFT JOIN {{ ref('dim_collateral') }} DColl
    ON LD.Collateral_ID = DColl.Collateral_ID AND DColl.dtf_current_flag = 'Y'
LEFT JOIN {{ ref('dim_collateral_right') }} DColR
    ON LD.Right_ID = DColR.Right_ID AND DColR.dtf_current_flag = 'Y'
LEFT JOIN {{ ref('dim_industry') }} DIND
    ON LD.Industry_Code = DIND.Industry_id AND DIND.dtf_current_flag = 'Y'
LEFT JOIN {{ ref('dim_branch') }} DBR
    ON LD.Opening_Branch = DBR.branch_id AND DBR.dtf_current_flag = 'Y'
