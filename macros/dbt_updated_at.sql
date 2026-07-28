{% macro dbt_updated_at() %}
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
{% endmacro %}
