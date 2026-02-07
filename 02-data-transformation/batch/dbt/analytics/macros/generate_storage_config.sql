{% macro portable_partition(cols) %}
    {% if target.type == 'databricks' %}
        {{ return({'partition_by': cols}) }}
    {% elif target.type == 'snowflake' %}
        {{ return({'cluster_by': cols}) }}
    {% else %}
        {{ return({}) }}
    {% endif %}
{% endmacro %}