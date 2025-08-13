      
{{ config(
    materialized='table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging/dimensions'
) }}

SELECT
    CAST(cu.Customer_ID AS STRING) AS customer_id,
    TRIM(cu.Name) AS name,
    CAST(cu.Date_of_birth AS DATE) AS date_of_birth,
    CAST(cu.Phone AS STRING) AS phone,
    cu.Email AS email,
    cu.Segment_Code AS segment_code,
    -- Tạo địa chỉ đầy đủ
    CONCAT_WS(',', cu.Address_Street, cu.Address_District, cu.Address_City) AS address,
    TRIM(cu.Gender) AS gender,
    cu.Nationality AS nationality,
    cu.Industry_Code AS industry_code,
    cu.Branch_Code AS branch_code,
    cu.ID_Type AS id_type,
    CAST(cu.ID_Number AS STRING) AS id_number,
    CAST(cu.ID_Issue_Date AS DATE) AS id_issue_date,
    cu.ID_Issuer AS id_issue,
    cu.Marital_Status AS marital_status,
    cu.Income_Level AS income_level,
    CAST(cu.Customer_Open_Date AS DATE) AS customer_open_date,
    db.dim_branch_id,
    di.dim_industry_id,
    cs.dim_cusseg_id,
    -- Tạo hash để phục vụ SCD Type 2
    MD5(CONCAT(
        COALESCE(cu.Name, ''),
        COALESCE(cu.Date_of_birth, ''),
        COALESCE(cu.Segment_Code, ''),
        COALESCE(cs.dim_cusseg_id,''),
        COALESCE(CONCAT_WS(',', cu.Address_Street, cu.Address_District, cu.Address_City), ''),
        COALESCE(cu.Industry_Code, ''),
        COALESCE(di.dim_industry_id, ''),
        COALESCE(cu.Marital_Status, ''),
        COALESCE(cu.Income_Level, ''),
        COALESCE(cu.Branch_Code, ''),
        COALESCE(db.dim_branch_id, '')
    )) AS record_hash

FROM {{ ref('t24_customer') }} cu
LEFT JOIN {{ ref('dim_branch') }} db 
    ON cu.branch_code = db.branch_id AND db.dtf_current_flag = 'Y'
LEFT JOIN {{ ref('dim_industry') }} di 
    ON cu.industry_code = di.industry_id AND di.dtf_current_flag = 'Y'
LEFT JOIN {{ ref('dim_customer_segment') }} cs
    ON cu.segment_code = cs.segment_code AND cs.dtf_current_flag = 'Y'
WHERE Customer_ID IS NOT NULL