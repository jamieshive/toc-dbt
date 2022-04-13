{# This function takes in a column of dates and returns a column of just the years of those dates #}

{# This function is primarily used for determining the Cohort to which a customer belongs #}

{% macro get_cohort_year(column_name) %}

    (extract(year from {{ column_name }} ))

{% endmacro %}
