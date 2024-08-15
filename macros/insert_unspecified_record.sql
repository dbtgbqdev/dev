{% macro insert_unspecified_record(model_name) %}
    {# Define a SQL statement to get column names for the current model #}
    {% set columns_query %}
    SELECT column_name
    FROM `osjl-kt.control_tables.unspecified_column_control`
    WHERE table_name = '{{ model_name }}'
    {% endset %}

    {% set result = run_query(columns_query) %}
    
    {# Check if the result is not None and contains rows #}
    {% if result is not none and result.row_count > 0 %}
        {% set columns = result.columns['column_name'].values() %}
        
        {# Initialize lists to store column names and values #}
        {% set column_list = [] %}
        {% set value_list = [] %}
        
        {# Loop through columns and set default values #}
        {% for column in columns %}
            {% if column.endswith('_ID') %}
                {% set column_list = column_list.append(column) %}
                {% set value_list = value_list.append('0') %}
            {% else %}
                {% set column_list = column_list.append(column) %}
                {% set value_list = value_list.append("'unspecified'") %}
            {% endif %}
        {% endfor %}
        
        {# Join the columns and values into comma-separated strings #}
        {% set column_string = column_list | join(', ') %}
        {% set value_string = value_list | join(', ') %}
        
        {# Generate the INSERT statement #}
        INSERT INTO `osjl-kt.stage.{{ model_name }}`
        (
            {{ column_string }}
        )
        VALUES
        (
            {{ value_string }}
        );
    {% else %}
        {# Handle case where no columns are found #}
        {% do log('No columns found for table ' ~ model_name, info=True) %}
    {% endif %}
{% endmacro %}
