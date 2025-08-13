{{ config(
   materialized = 'incremental',
   incremental_strategy = 'merge',
   unique_key = ['security_id','purchase_date'],
   file_format = 'iceberg',
   partition_by = ['purchase_date'],
   location_root = 's3a://iceberg-warehouse/output/facts'
) }}

with run_param as (
 select {{ get_run_date() }} as run_date
),
sec as (
 select *
 from {{ ref('stg_security_value') }}
),
dsec as (
 select
     cast(dim_security_id as string)  as dim_security_id,
     cast(security_id as string)   as security_id
 from {{ ref('dim_security') }}
),
filtered as (
 select
     s.security_id,
     s.purchase_price,
     s.market_value,
     s.purchase_date
 from sec s
 cross join run_param r
 where s.security_id is not null
   and s.purchase_date = r.run_date
),
joined as (
 select
     /* Foreign key tới DIM: dùng surrogate key */
     d.dim_security_id as security_id,
     /* dtf_day_id kiểu YYYYMMDD */
     cast(date_format(f.purchase_date, 'yyyyMMdd') as int) as dtf_day_id,
     f.purchase_price,
     f.market_value,
     f.purchase_date
 from filtered f
 inner join dsec d
         on d.security_id = f.security_id
)

select *
from joined

{% if is_incremental() %}
-- Với MERGE, dbt sẽ tự sinh câu MERGE dựa trên unique_key ở trên.
-- Không cần where bổ sung. Nếu engine của bạn yêu cầu, có thể thêm filter nhẹ.
{% endif %}