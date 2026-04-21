{% macro test_value_between(model, column_name, min_value, max_value) %}
-- Reusable macro: checks a column's values stay within min/max range
-- Usage in schema.yml:
--   data_tests:
--     - value_between:
--         min_value: 0
--         max_value: 100000

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND ({{ column_name }} < {{ min_value }}
   OR  {{ column_name }} > {{ max_value }})

{% endmacro %}
