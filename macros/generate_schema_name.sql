{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema | trim | replace(' ', '_') -%}

    {%- if custom_schema_name is none or custom_schema_name | trim == "" -%}
        {{ default_schema }}
    {%- else -%}
        {%- set cleaned_custom_schema_name = custom_schema_name | trim | replace(' ', '_') -%}
        {{ default_schema }}_{{ cleaned_custom_schema_name }}
    {%- endif -%}
{%- endmacro %}
