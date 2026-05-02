{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {# This ensures the custom schema is used directly without prefixing #}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set target_name = target.name -%}

    {# 1. In Research/Dev notebooks, return none for portability #}
    {%- if target_name == 'dev_notebook' or target_name == 'research' -%}
        {{ return(none) }}

    {# 2. If a custom database is explicitly set in the model config, use it #}
    {%- elif custom_database_name is not none -%}
        {{ custom_database_name | trim }}

    {# 3. In Prod/Standard runs, return the target database from profiles.yml #}
    {%- else -%}
        {{ target.database | trim }}
    {%- endif -%}

{%- endmacro %}