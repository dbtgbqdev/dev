{% macro get_last_load_date() %}
{% set query %}
   select IFNULL(CAST(max(LAST_EXTRACT_DATE) AS STRING FORMAT 'YYYY-MM-DD'),'1900-01-01') from control_tables.OSJL_TASK_CONTROL_G WHERE TARGET_TABLE_NAME = '{{ this.name }}' 
{% endset %}
{% set results = run_query(query) %}
{% if execute %}
{% set last_load_date = results.columns[0][0] %}
{% endif %}
{{ return(last_load_date) }}
{% endmacro %}