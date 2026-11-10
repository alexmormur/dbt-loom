{% macro known_tenants() -%}
    'emea', 'apac'
{%- endmacro %}

{% macro is_known_tenant(column) -%}
    {{ column }} in ({{ known_tenants() }})
{%- endmacro %}
