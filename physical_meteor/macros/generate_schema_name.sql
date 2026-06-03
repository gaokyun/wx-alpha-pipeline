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

    {# 1. In Postgres target, return target.database to bypass cross-db reference error and avoid NoneType error #}
    {%- if target.type == 'postgres' -%}
        {{ return(target.database) }}

    {# 2. In Research/Dev notebooks, return none for portability #}
    {%- elif target_name == 'dev_notebook' or target_name == 'research' -%}
        {{ return(none) }}

    {# 3. If a custom database is explicitly set in the model config, use it #}
    {%- elif custom_database_name is not none -%}
        {{ custom_database_name | trim }}

    {# 4. In Prod/Standard runs, return the target database from profiles.yml #}
    {%- else -%}
        {{ target.database | trim }}
    {%- endif -%}

{%- endmacro %}