{% macro insert_unspecified_record() %}
    {# Get the current model name (table name) #}
    {% set table_name = target.name %}
    
    {# Query to get column names from the unspecified_column_control table #}
    {% set columns_query %}
    SELECT column_name
    FROM `osjl-kt.control_tables.unspecified_column_control`
    WHERE table_name = '{{ table_name }}'
    {% endset %}
    
    {% set columns = run_query(columns_query).columns['column_name'].values() %}
    
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
    INSERT INTO `osjl-kt.stage.{{ table_name }}`
    (
        {{ column_string }}
    )
    VALUES
    (
        {{ value_string }}
    );
{% endmacro %}
