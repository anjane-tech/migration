{% macro __init__() %}
{%- set migrations -%}

{# -- Add all your models here below -- #}

- fact_2023_10_19_01
- fact_2023_10_19_02

{# -- Add all your models above ^^^ -- #}
{%- endset -%}
{{ return( migrations) }}
{%- endmacro -%}