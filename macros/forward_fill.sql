{% macro forward_fill(column, partition_by, order_by) %}
    LAST_VALUE({{ column }} IGNORE NULLS) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
{% endmacro %}
