{% macro fact_2023_10_19_02() %}
{% set migrations %}
{# /* Add migrations below */ #}


up:
  - name: 'fact'
    columns:
      - name: new_val
        type: modify
        datatype: VARCHAR


{# /* Add migrations above */ #}
{% endset %}
{{return(migrations)}}
{% endmacro %}
