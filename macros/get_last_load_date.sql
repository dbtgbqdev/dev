{% macro get_last_load_date() %}
{% set query %}
   select IFNULL(CAST(max(last_load_date) AS STRING FORMAT 'YYYY-MM-DD'),'1900-01-01') from dbt_duser.dbt_results WHERE table_name = '{{ this.name }}' and status = 'success'
{% endset %}
{% set results = run_query(query) %}
{% if execute %}
{% set last_load_date = results.columns[0][0] %}
{% endif %}
{{ return(last_load_date) }}
{% endmacro %}