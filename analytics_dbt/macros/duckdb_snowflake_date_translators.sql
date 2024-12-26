{% macro strftime(timestamp, format) %}
  {% if target.type == 'duckdb' %}
    strftime({{ timestamp }}::timestamp, {{ format }})
  {% elif target.type == 'snowflake' %}
    to_char({{ timestamp }}::timestamp, {{ format }})
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}

{% macro date_part(part, timestamp) %}
  {% if target.type == 'duckdb' %}
    date_part('{{ part }}', {{ timestamp }}::timestamp)
  {% elif target.type == 'snowflake' %}
    date_part('{{ part }}', {{ timestamp }}::timestamp)
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}

{% macro date_trunc(part, timestamp) %}
  {% if target.type == 'duckdb' %}
    date_trunc('{{ part }}', {{ timestamp }}::timestamp)
  {% elif target.type == 'snowflake' %}
    date_trunc('{{ part }}', {{ timestamp }}::timestamp)
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}

{% macro strptime(date_string, format) %}
  {% if target.type == 'duckdb' %}
    strptime({{ date_string }}, {{ format }})
  {% elif target.type == 'snowflake' %}
    to_date({{ date_string }}, 'MM-DD-YYYY')
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}
