{% test greater_than(model, column_name, number) %}

    select
        *

    from {{ model }}
    where {{ column_name }} <= {{ number }}

{% endtest %}