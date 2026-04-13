-- Override dbt's default generate_schema_name behavior.
--
-- By default, dbt-spark appends the profile's default schema to the model's
-- custom schema: profile.schema + "_" + model.schema (e.g. "silver_silver").
-- This macro ignores the profile default and uses only the model's custom schema
-- (set via +schema in dbt_project.yml), falling back to the default if no custom
-- schema is specified.
--
-- Result:
--   Silver models (+schema: silver) → nessie.silver.<model>
--   Gold models   (+schema: gold)   → nessie.gold.<model>
--   Models without +schema          → nessie.silver.<model> (profile default)

{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ default__generate_schema_name(custom_schema_name, node) }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
