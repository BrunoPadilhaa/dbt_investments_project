{% macro running_total(column, partition_by, order_by) %}
   SUM({{ column }}) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
    )
{% endmacro %}
