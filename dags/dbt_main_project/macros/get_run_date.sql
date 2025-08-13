{% macro get_run_date() %}

    {% if var('run_date', none) is not none %}
        {{ "'" ~ var('run_date') ~ "'" }}        -- 'YYYY-MM-DD'
    {% else %}
        current_date()
    {% endif %}

{% endmacro %}