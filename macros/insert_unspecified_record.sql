{% macro insert_unspecified_record(table_name) %}
    {% do log("Running insert_unspecified_record for table: " ~ table_name, info=True) %}

    --  query to get column names for the specified table
    {% set columns_query %}
        SELECT column_name
        FROM `osjl-kt.control_tables.unspecified_column_control`
        WHERE table_name = '{{ table_name }}'
    {% endset %}
    
    {% do log("Query to get column names: " ~ columns_query, info=True) %}
    
    {% set columns_result = run_query(columns_query) %}
    
    {% if columns_result is none %}
        {% do log("No result from query for table: " ~ table_name, info=True) %}
        {% do return('') %}
    {% endif %}
    
    {% set column_names = columns_result.columns if columns_result else [] %}
    
    {% do log("Retrieved columns: " ~ column_names | join(', '), info=True) %}
    
    {% if column_names | length == 0 %}
        {% do log("No columns found for table: " ~ table_name, info=True) %}
        {% do return('') %}
    {% endif %}

    -- Generate the insert values based on column names
    {% set insert_values = [] %}
    {% for column_name in column_names %}
        {% if column_name.endswith('_ID') %}
            {% do insert_values.append('0') %}
        {% else %}
            {% do insert_values.append("'unspecified'") %}
        {% endif %}
    {% endfor %}

    -- Combine columns and insert values into an insert statement
    {% set insert_statement %}
        INSERT INTO `osjl-kt.stage.{{ table_name }}`
        (
            {{ column_names | join(', ') }}
        )
        VALUES
        (
            {{ insert_values | join(', ') }}
        )
    {% endset %}
    
    {% do log("Generated insert statement: " ~ insert_statement, info=True) %}
    
    {{ insert_statement }}

{% endmacro %}
