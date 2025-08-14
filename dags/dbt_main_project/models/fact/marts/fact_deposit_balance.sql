{{ config(
   materialized = 'incremental',
   incremental_strategy = 'merge',
   unique_key = ['dim_deposit_id','dtf_day_id'],
   file_format = 'iceberg',
   partition_by = ['dtf_day_id'],
   location_root = 's3a://iceberg-warehouse/output/facts'
) }}

with run_param as (
    select {{ get_run_date() }} as run_date
),

-- Staging data
stg_deposit as (
    select *
    from {{ ref('stg_deposit_balance') }}
),

-- Dimension lookups
dim_deposit as (
    select
        cast(dim_account_deposit_id as string) as dim_account_deposit_id,
        cast(deposit_id as string) as deposit_id
    from {{ ref('dim_account_deposit') }}
    where dtf_current_flag = 'Y'
),

dim_customer as (
    select
        cast(dim_customer_id as string) as dim_customer_id,
        cast(customer_id as string) as customer_id
    from {{ ref('dim_customer') }}
    where dtf_current_flag = 'Y'
),

-- Get transaction data for transaction_id mapping with ranking
transaction_data as (
    select 
        ft.transaction_id,
        CASE 
            WHEN ft.to_account LIKE 'DEP%' THEN ft.to_account
            WHEN ft.from_account LIKE 'DEP%' THEN ft.from_account
            ELSE NULL
        END AS account_deposit_id,
        ft.booking_date,
        row_number() over (
            partition by CASE 
                WHEN ft.to_account LIKE 'DEP%' THEN ft.to_account
                WHEN ft.from_account LIKE 'DEP%' THEN ft.from_account
                ELSE NULL
            END 
            order by ft.booking_date desc
        ) as rn
    from {{ ref('t24_funds_transfer') }} ft
    where ft.status = 'Completed'
),

-- Latest transaction per deposit account
latest_transactions as (
    select 
        account_deposit_id,
        transaction_id
    from transaction_data
    where rn = 1
),

-- Filtered staging data based on run date
filtered as (
    select
        s.account_deposit_id,
        s.customer_id,
        s.balance,
        s.interest_rate,
        s.updated
    from stg_deposit s
    cross join run_param r
    where s.account_deposit_id is not null
        and cast(s.updated as date) = r.run_date
),

-- Join with dimensions
joined as (
    select
        /* Foreign keys using surrogate keys */
        dd.dim_account_deposit_id as dim_deposit_id,
        dc.dim_customer_id as dim_customer_id,
        
        /* Transaction ID - get the most recent transaction for this deposit account */
        lt.transaction_id as dim_transaction_id,
        
        /* Date dimension - format as YYYYMMDD */
        cast(date_format(f.updated, 'yyyyMMdd') as int) as dtf_day_id,
        
        /* Fact measures */
        f.balance,
        f.interest_rate,
        
        /* Audit fields */
        current_timestamp() as created_at,
        f.updated as source_updated_at
        
    from filtered f
    left join dim_deposit dd
        on dd.deposit_id = f.account_deposit_id
    left join dim_customer dc
        on dc.customer_id = f.customer_id
    left join latest_transactions lt
        on lt.account_deposit_id = f.account_deposit_id
)

select *
from joined

{% if is_incremental() %}
    -- Only process records that are newer than what we already have
    where dtf_day_id > (select coalesce(max(dtf_day_id), 0) from {{ this }})
{% endif %}
