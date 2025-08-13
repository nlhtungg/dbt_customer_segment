{{ config(
   materialized = 'table',
   file_format='iceberg',
   location_root='s3a://iceberg-warehouse/staging/facts'
) }}

select
    cast(Security_ID as string),
    cast(Purchase_Price as decimal(18,2)) as purchase_price,
    cast(Market_value as decimal(18,2)) as market_value,
    cast(Purchase_Date as date) as purchase_date
from {{ ref('t24_security') }}