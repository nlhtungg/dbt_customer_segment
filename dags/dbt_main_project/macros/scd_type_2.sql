      
{% macro scd_type_2(source_table, unique_key, compare_columns, surrogate_key_column, exclude_columns=[]) %}

{% if is_incremental() %}
,
-- Get all existing data
existing_data AS (
    SELECT * FROM {{ this }}
),

-- Get only current records
current_records AS (
    SELECT * FROM existing_data
    WHERE dtf_current_flag = 'Y'
),

-- Get source data with hash
source_data AS (
    SELECT *
    FROM {{ source_table }}
),

-- Add hash to current records for comparison
current_with_hash AS (
    SELECT *,
        MD5(CONCAT(
            {% for col in compare_columns %}
            COALESCE({{ col }}, ''){{ "," if not loop.last }}
            {% endfor %}
        )) AS record_hash
    FROM current_records
),

-- Find NEW records (in source but not in current)
new_records AS (
    SELECT s.*
    FROM source_data s
    LEFT JOIN current_with_hash c ON s.{{ unique_key }} = c.{{ unique_key }}
    WHERE c.{{ unique_key }} IS NULL
),

-- Find CHANGED records (same key but different data)
changed_records AS (
    SELECT s.*
    FROM source_data s
    INNER JOIN current_with_hash c ON s.{{ unique_key }} = c.{{ unique_key }}
    WHERE s.record_hash != c.record_hash
),

-- Find DELETED records (in current but not in source)
deleted_keys AS (
    SELECT c.{{ unique_key }}
    FROM current_with_hash c
    LEFT JOIN source_data s ON c.{{ unique_key }} = s.{{ unique_key }}
    WHERE s.{{ unique_key }} IS NULL
),

-- Mark existing current records as expired
expired_current_records AS (
    SELECT 
        {{ surrogate_key_column }},
        {{ unique_key }},
        {% for col in compare_columns %}
        {{ col }},
        {% endfor %}
        dtf_start_date,
        CURRENT_DATE() AS dtf_end_date,
        'N' AS dtf_current_flag
    FROM current_records
    WHERE {{ unique_key }} IN (
        SELECT {{ unique_key }} FROM changed_records
        UNION ALL
        SELECT {{ unique_key }} FROM deleted_keys
    )
),

-- Create new current records for new and changed data
new_current_records AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY {{ unique_key }}) + 
        COALESCE((SELECT MAX({{ surrogate_key_column }}) FROM existing_data), 0) AS {{ surrogate_key_column }},
        {{ unique_key }},
        {% for col in compare_columns %}
        {{ col }},
        {% endfor %}
        CURRENT_DATE() AS dtf_start_date,
        CAST(NULL AS DATE) AS dtf_end_date,
        'Y' AS dtf_current_flag
    FROM (
        SELECT {{ unique_key }}, {% for col in compare_columns %}{{ col }}{{ "," if not loop.last }}{% endfor %} FROM new_records
        UNION ALL
        SELECT {{ unique_key }}, {% for col in compare_columns %}{{ col }}{{ "," if not loop.last }}{% endfor %} FROM changed_records
    ) combined_new
),

-- Keep unchanged current records
unchanged_current_records AS (
    SELECT 
        {{ surrogate_key_column }},
        {{ unique_key }},
        {% for col in compare_columns %}
        {{ col }},
        {% endfor %}
        dtf_start_date,
        dtf_end_date,
        dtf_current_flag
    FROM current_records
    WHERE {{ unique_key }} NOT IN (
        SELECT {{ unique_key }} FROM changed_records
        UNION ALL
        SELECT {{ unique_key }} FROM deleted_keys
    )
),

-- Keep all historical records
historical_records AS (
    SELECT 
        {{ surrogate_key_column }},
        {{ unique_key }},
        {% for col in compare_columns %}
        {{ col }},
        {% endfor %}
        dtf_start_date,
        dtf_end_date,
        dtf_current_flag
    FROM existing_data
    WHERE dtf_current_flag = 'N'
),

-- Final result
scd_result AS (
    SELECT * FROM historical_records
    UNION ALL
    SELECT * FROM expired_current_records
    UNION ALL
    SELECT * FROM unchanged_current_records
    UNION ALL
    SELECT * FROM new_current_records
)

SELECT * FROM scd_result

{% else %}
-- Initial load
SELECT
    ROW_NUMBER() OVER (ORDER BY {{ unique_key }}) AS {{ surrogate_key_column }},
    {{ unique_key }},
    {% for col in compare_columns %}
    {{ col }},
    {% endfor %}
    CURRENT_DATE() AS dtf_start_date,
    CAST(NULL AS DATE) AS dtf_end_date,
    'Y' AS dtf_current_flag
FROM {{ source_table }}

{% endif %}

{% endmacro %}