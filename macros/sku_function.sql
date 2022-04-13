{# This function takes in a column of SKUs and builds a product table that lists product information for each SKU #}

{% set pizza_type = 

["Cheese", 
"Sausage", 
"Pepperoni"] %} {#, 
"V":"Veggie", 
"H":"Spinach", 
"X":"Crustless", 
"Y":"Heart Cheese", 
"Z":"Heart Sausage",
"E":"GF Cheese",
"A":"GF Sausage",
"N":"GF Pepperoni",
"M":"Meat",
"L":"Lous Special"}] %} #}

{% macro get_product_data(column_name) %}

{{ column_name }}::varchar as sku,
substring(sku,3,len(sku)-2) as child_sku,
{% for pizza in pizza_type %}
    null as {{ pizza }}_type,
   {# {% for abbr,long in pizza.items() %}
    case
        when
            charindex('{{ abbr }}',child_sku) > 0
        then 
            '{{ abbr }}'
        else '0'
    end as test,
    {% endfor %} #}
{% endfor %}

len(sku) as length

{% endmacro %}