{#
  d8a-de-workbench: use a model's custom schema as is.

  dbt's default prefixes a custom schema with the target schema
  (`staging` + `marts` becomes `staging_marts`). The projects of the path
  refer to `staging.`, `intermediate.` and `marts.` by name, so a model
  configured with `+schema: marts` lands in `marts`, and a model without a
  custom schema lands in the target's schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
