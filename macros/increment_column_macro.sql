-- macros/increment_column_macro.sql
{% macro get_increment_column(schema, table, increment_time) %}
  SELECT
    TIMESTAMP_SUB(MAX(max_to_time), INTERVAL 2 DAY) AS increment_date
  FROM (
    SELECT
      MAX({{ increment_time }}) AS max_to_time
    FROM
      {{ schema }}.{{ table }}  -- Use the parameters here
    UNION ALL
    SELECT
      CAST('2024-01-01 00:00:00 UTC' AS TIMESTAMP) AS max_to_time
  ) AS combined
{% endmacro %}
