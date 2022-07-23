{% macro clean_target_schemas(dryrun=True) %}

  {% set get_schemas_query %}
      SELECT schema_name FROM `{{ target.project }}.region-US.INFORMATION_SCHEMA.SCHEMATA` 
      where schema_name like '{{ target.schema }}%'
      order by schema_name desc;
  {% endset %}

  {% set schemas = run_query(get_schemas_query).columns[0].values() %}

  {% for schema in schemas %}
    {% do log("Cleaning up " + schema + " schema", True) %}
    {{ clean_schema(schema=schema, dryrun=dryrun) }}
  {% endfor %}

{% endmacro %}