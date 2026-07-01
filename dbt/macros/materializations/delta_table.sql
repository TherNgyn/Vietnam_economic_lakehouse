{% materialization delta_table, adapter='spark' %}

    {%- set target_relation = this.incorporate(type='table') -%}

    {%- set existing_relation = adapter.get_relation(
        database=target_relation.database,
        schema=target_relation.schema,
        identifier=target_relation.identifier
    ) -%}

    {{ run_hooks(pre_hooks) }}

    {% do adapter.create_schema(target_relation) %}

    {% if existing_relation is not none %}
        {% do adapter.drop_relation(existing_relation) %}
        {% do adapter.cache_dropped(existing_relation) %}
    {% endif %}

    {% call statement('drop_table_if_exists') %}
        DROP TABLE IF EXISTS {{ target_relation }}
    {% endcall %}

    {% call statement('main') %}
        CREATE TABLE {{ target_relation }}
        USING DELTA
        AS
        {{ sql }}
    {% endcall %}

    {% do adapter.cache_added(target_relation) %}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
