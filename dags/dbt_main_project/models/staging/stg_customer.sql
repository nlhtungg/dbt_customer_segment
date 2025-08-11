      
{{ config(
    materialized='table',
    file_format='iceberg',
    location_root='s3a://iceberg-warehouse/staging'
) }}

SELECT
    CAST(Customer_ID AS STRING) AS customer_id,
    TRIM(Name) AS name,
    CAST(Date_of_birth AS DATE) AS date_of_birth,
    CAST(Phone AS STRING) AS phone,
    Email AS email,
    Segment_Code AS segment_code,
    -- Tạo địa chỉ đầy đủ
    CONCAT_WS(', ', Address_Street, Address_District, Address_City) AS address,
    TRIM(Gender) AS gender,
    Nationality AS nationality,
    Industry_Code AS industry_code,
    Branch_Code AS branch_code,
    ID_Type AS id_type,
    CAST(ID_Number AS STRING) AS id_number,
    CAST(ID_Issue_Date AS DATE) AS id_issue_date,
    ID_Issuer AS id_issue,
    Marital_Status AS marital_status,
    Income_Level AS income_level,
    CAST(Customer_Open_Date AS DATE) AS customer_open_date,
    -- Thêm dim_branch_id và dim_industry_id
    db.dim_branch_id,
    di.dim_industry_id,
    -- Tạo hash để phục vụ SCD Type 2
    MD5(CONCAT(
        COALESCE(TRIM(Name), ''),
        COALESCE(Date_of_birth, ''),
        COALESCE(Phone, ''),
        COALESCE(Email, ''),
        COALESCE(Segment_Code, ''),
        COALESCE(Industry_Code, ''),
        COALESCE(Branch_Code, ''),
        COALESCE(Gender, ''),
        COALESCE(Nationality, ''),
        COALESCE(ID_Type, ''),
        COALESCE(ID_Number, ''),
        COALESCE(ID_Issue_Date, ''),
        COALESCE(ID_Issuer, ''),
        COALESCE(Marital_Status, ''),
        COALESCE(Income_Level, ''),
        COALESCE(Customer_Open_Date, ''),
        COALESCE(CONCAT_WS(', ', Address_Street, Address_District, Address_City), ''),
        COALESCE(CAST(db.dim_branch_id AS STRING), ''),
        COALESCE(CAST(di.dim_industry_id AS STRING), '')
    )) AS record_hash

FROM {{ ref('t24_customer') }}
LEFT JOIN {{ ref('dim_branch') }} db 
    ON {{ ref('t24_customer') }}.branch_code = db.branch_id 
    AND db.dtf_current_flag = 'Y'
LEFT JOIN {{ ref('dim_industry') }} di 
    ON {{ ref('t24_customer') }}.industry_code = di.industry_id 
    AND di.dtf_current_flag = 'Y'
WHERE Customer_ID IS NOT NULL

    