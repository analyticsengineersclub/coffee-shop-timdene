{% test one_record_per(model, column_name, per) %}

    select
        count(distinct {{ column_name }} ) as n_records

    from {{ model }}
    where {{ per }} is not null
    group by {{ per }}
    having n_records > 1

{% endtest %}