{% macro generate_schema_name(custom_schema_name, node) -%}
    {# 
      If a model has +schema: X, use X as the schema (e.g. MARTS, INTERMEDIATE).
      Otherwise, fall back to target.schema (e.g. STAGING).
    #}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | upper }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
