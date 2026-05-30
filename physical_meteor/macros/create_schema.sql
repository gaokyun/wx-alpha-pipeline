{% macro create_schema(relation) -%}
  {%- if relation.database == 'PHYSICAL_METEOR_GOLD' -%}
    {# No-op: MySQL database schema/database already exists and cannot be created via CREATE SCHEMA #}
    {%- do log("Bypassing schema creation for MySQL target database: " ~ relation.database, info=True) -%}
  {%- else -%}
    {# Default DuckDB behavior #}
    {%- call statement('create_schema') -%}
      create schema if not exists {{ relation.without_identifier() }}
    {%- endcall -%}
  {%- endif -%}
{%- endmacro %}
