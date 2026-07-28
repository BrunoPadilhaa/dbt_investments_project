{% macro incremental_load_filter(load_ts_column) %}
    {{ load_ts_column }} > (SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endmacro %}
