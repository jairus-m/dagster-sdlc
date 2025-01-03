{# 
  This macro casts a timestamp into a date.
  It supports DuckDB and Snowflake.

  Args:
    timestamp (str): The timestamp to format.

  Returns:
    date: 'YYYY-12-29'

  Example:
    {{ get_date('my_timestamp') }}
#}
{% macro get_time(timestamp) %}
  {% if target.type == 'duckdb' %}
    cast({{ timestamp }}::timestamp as time)
  {% elif target.type == 'snowflake' %}
    to_time({{ timestamp }}::timestamp)
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}
{# 
  This macro casts a timestamp into a time.
  It supports DuckDB and Snowflake.

  Args:
    timestamp (str): The timestamp to format.

  Returns:
    date: 'YYYY-12-29'

  Example:
    {{ get_date('my_timestamp') }}
#}
{% macro get_date(timestamp) %}
  {% if target.type == 'duckdb' %}
    cast({{ timestamp }}::timestamp as date)
  {% elif target.type == 'snowflake' %}
    date({{ timestamp }}::timestamp)
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}
{# 
  This macro formats a timestamp into a string based on the specified format.
  It supports DuckDB and Snowflake.

  Args:
    timestamp (str): The timestamp to format.
    format (str): The format string to use.

  Returns:
    str: The formatted timestamp as a string.

  Example:
    {{ strftime('my_timestamp', "'YYYY-MM-DD'") }}
#}
{% macro strftime(timestamp, format) %}
  {% if target.type == 'duckdb' %}
    strftime({{ timestamp }}::timestamp, {{ format }})
  {% elif target.type == 'snowflake' %}
    to_char({{ timestamp }}::timestamp, {{ format }})
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}

{# 
  This macro extracts a specific part from a timestamp.
  It supports DuckDB and Snowflake.

  Args:
    part (str): The part to extract (e.g., 'year', 'month', 'day').
    timestamp (str): The timestamp to extract from.

  Returns:
    int: The extracted part as an integer.

  Example:
    {{ date_part('year', 'my_timestamp') }}
#}
{% macro date_part(part, timestamp) %}
  {% if target.type == 'duckdb' %}
    date_part('{{ part }}', {{ timestamp }}::timestamp)
  {% elif target.type == 'snowflake' %}
    date_part('{{ part }}', {{ timestamp }}::timestamp)
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}

{# 
  This macro truncates a timestamp to a specified part.
  It supports DuckDB and Snowflake.

  Args:
    part (str): The part to truncate to (e.g., 'year', 'month', 'day').
    timestamp (str): The timestamp to truncate.

  Returns:
    timestamp: The truncated timestamp.

  Example:
    {{ date_trunc('month', 'my_timestamp') }}
#}
{% macro date_trunc(part, timestamp) %}
  {% if target.type == 'duckdb' %}
    date_trunc('{{ part }}', {{ timestamp }}::timestamp)
  {% elif target.type == 'snowflake' %}
    date_trunc('{{ part }}', {{ timestamp }}::timestamp)
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}

{# 
  This macro parses a date string into a date object.
  It supports DuckDB and Snowflake, but note that the Snowflake implementation
  is currently hardcoded to a specific format.

  Args:
    date_string (str): The date string to parse.
    format (str): The format of the date string (used in DuckDB only).

  Returns:
    date: The parsed date object.

  Example:
    {{ strptime("'12-31-2023'", "'%m-%d-%Y'") }}
#}
{% macro strptime(date_string, format) %}
  {% if target.type == 'duckdb' %}
    strptime({{ date_string }}, {{ format }})
  {% elif target.type == 'snowflake' %}
    to_date({{ date_string }}, 'MM-DD-YYYY')
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}
