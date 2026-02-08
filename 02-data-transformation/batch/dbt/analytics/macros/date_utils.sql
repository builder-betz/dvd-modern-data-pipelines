{% macro portable_datediff(start, end) %}
  {% if target.type == 'snowflake' %}
    datediff('day', {{ start }}, {{ end }})
  {% else %}
    datediff({{ end }}, {{ start }})
  {% endif %}
{% endmacro %}
