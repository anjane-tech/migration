{% macro fact_2023_10_19_01() %}
{% set migrations %}
{# /* Add migrations below */ #}



up:
  - name: 'fact'
    columns:
      - name: new_val
        type: add
        datatype: VARCHAR(250)
    sql:
      - seq: 1
        text: "Update hackathon_2023.dbt_bramachandran.fact set new_val = 'Hi There ! - ' || val"

down:
  - name: 'fact'
    columns:
      - name: new_val
        type: drop


{# /* Add migrations above */ #}
{% endset %}
{{return(migrations)}}
{% endmacro %}