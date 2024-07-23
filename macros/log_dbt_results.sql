{% macro log_dbt_results(results) %}
    -- depends_on: {{ ref('dbt_results') }}
    {%- if execute -%}
        {%- set parsed_results = parse_dbt_results(results) -%}
        {%- if parsed_results | length  > 0 -%}
            {% set insert_dbt_results_query -%}
            {%- for parsed_result_dict in parsed_results -%}
                merge into osjl-kt.control_tables.OSJL_DATALOAD_CONTROL_G as dbt_results 
                using (select '{{ parsed_result_dict.get('table_name') }}' AS dbt_table_name) as dbt_internal_source     
                on (dbt_results.TARGET_TABLE_NAME = dbt_internal_source.dbt_table_name)
                when matched then update set 
                dbt_results.SCENARIO_RUN_ID = '{{ parsed_result_dict.get('invocation_id') }}', 
                dbt_results.ROWS_WRITTEN = {{ parsed_result_dict.get('rows_affected') }},
                dbt_results.STATUS = '{{ parsed_result_dict.get('status') }}',
                dbt_results.REASON = '{{ parsed_result_dict.get('status_message') }}',
                {{- "," if not loop.last else "" -}}
                    {%- endfor -%}
            {%- endset -%}
            {%- do run_query(insert_dbt_results_query) -%}
        {%- endif -%}
    {%- endif -%}
    -- This macro is called from an on-run-end hook and therefore must return a query txt to run. Returning an empty string will do the trick
    {{ return ('') }}
{% endmacro %}