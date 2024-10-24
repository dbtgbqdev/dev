{% macro parse_dbt_results(results) %}
    -- Create a list of parsed results
    {%- set parsed_results = [] %}
    -- Flatten results and add to list
    {% for run_result in results %}
        -- Convert the run result object to a simple dictionary
        {% set run_result_dict = run_result.to_dict() %}
        -- Get the underlying dbt graph node that was executed
        {% set node = run_result_dict.get('node') %}
        {% set timing_compile_dict = run_result_dict.get('timing')[1]%}
        {% set last_load_date = timing_compile_dict.get('started_at')%}
        {% set status_message = run_result_dict.get('message')|replace('\n',' ')%}
        {% set rows_affected = run_result_dict.get('adapter_response', {}).get('rows_affected', 0) %}
        {%- if not rows_affected -%}
            {% set rows_affected = 0 %}
        {%- endif -%}
        {% set parsed_result_dict = {
            'result_id': invocation_id ~ '.' ~ node.get('unique_id'),
                'invocation_id': invocation_id,
                'unique_id': node.get('unique_id'),
                'database_name': node.get('database'),
                'schema_name': node.get('schema'),
                'table_name': node.get('name'),
                'resource_type': node.get('resource_type'),
                'status': run_result_dict.get('status'),
                'status_message' : status_message,
                'execution_time': run_result_dict.get('execution_time'),
                'rows_affected': rows_affected,
                'last_load_date' : last_load_date
                }%}
        {% do parsed_results.append(parsed_result_dict) %}
    {% endfor %}
    {{ return(parsed_results) }}
{% endmacro %}