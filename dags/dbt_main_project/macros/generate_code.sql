{% macro generate_code(prefix, order_by, pad_length=2) %}

 {%- set str_type = "STRING" if target.type in ["spark", "databricks"] else "VARCHAR" -%}
 CONCAT(
   '{{ prefix }}',
   LPAD(
     CAST(ROW_NUMBER() OVER (ORDER BY {{ order_by }}) AS {{ str_type }}),
     {{ pad_length }},
     '0'
   )
 )

{% endmacro %}