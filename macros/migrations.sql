{%- macro createMigrationMetadata() -%}
{%- set createSql -%}
CREATE or replace TABLE migration_metadata(id varchar, migration_ts timestamp);
{%- endset -%}

{%- set createSql -%}
CREATE TABLE IF NOT EXISTS migration_metadata(id varchar, migration_ts timestamp);
{%- endset -%}


{% do log("Going to create Migration_Metadata table if not exists", info=True) %}
{% do run_query(createSql) %}

{%- endmacro -%}

{%- macro insertMigrationMetadata(migrationName) -%}
{%- set inserSql -%}
insert into migration_metadata
select distinct '{{migrationName}}' as id, current_timestamp() as migration_ts
where not exists (select 'X' from migration_metadata where id = '{{migrationName}}')
;
{%- endset -%}

{% do log("Going to insert into Metadata table", info=True) %}
{% do run_query(inserSql) %}

{%- endmacro -%}

{%- macro deleteMigrationMetadata(migrationName) -%}
{%- set deleteSql -%}
delete from migration_metadata
where id = '{{migrationName}}'
;
{%- endset -%}

{% do log("Going to delete from Metadata table", info=True) %}
{% do run_query(deleteSql) %}

{%- endmacro -%}

{%- macro getMetadata(orderBy) -%}
    {%- set queryMetadataSql -%}
        select id, migration_ts
        from migration_metadata
        order by migration_ts {{orderBy}}
    {%- endset -%}
    {% set metadata = dbt_utils.get_query_results_as_dict(queryMetadataSql) %}
    {% set metadata =  metadata['ID'] | unique %}
    {{return(metadata)}}
{%- endmacro -%}

{%- macro up() -%}
    {{createMigrationMetadata()}}
    {%- set migrationScripts = __init__() -%}
    {%- set migrationScripts = fromyaml(migrationScripts) -%}
    {{log("Received the list of migrations " ~ migrationScripts, info = True)}}
    {% set metadata = getMetadata('asc') %}
    {% set migrations = [] %}
    {% for ms in migrationScripts|sort() %}
        {% set model_exist = [] %}
        {% for mt in metadata %}
            {% if ms|trim|lower == mt|trim|lower %}
                {{model_exist.append('1') }}
            {% endif %}
        {% endfor %}
        {% if model_exist|length == 0  %}
            {{ migrations.append(ms) }}
        {% endif %}
    {% endfor %}
    {% set migrationYaml = [] %}
    {% for migration in migrations|sort %}
        {%- set migration_macro = context.get(migration) -%}
        {% set migrationYaml = fromyaml(migration_macro()) %}
        {{log("Running migration for " ~ migration, info=True)}}
        {{log("Running up  for " ~ migrationYaml.up, info=True)}}
        {% for node in graph.nodes.values()
            | selectattr("resource_type", "equalto", "model")%}
            {% for model in migrationYaml.up %}
                {% if model.name == node.name %}
                    {{modifyColumns(node.relation_name, model.columns)}}
                    {{runSqls(model.sql)}}
                {% endif%}
            {% endfor %}
        {% endfor %}
        {{insertMigrationMetadata(migration)}}
    {% endfor %}
{%- endmacro -%}

{%- macro down() -%}
    {%- set migrationScripts = __init__() -%}
    {%- set migrationScripts = fromyaml(migrationScripts) -%}
    {{log("Received the list of migrations " ~ migrationScripts, info = True)}}
    {% set metadata = getMetadata('desc') %}
    {% set migrations = [] %}
    {% for mt in metadata %}
        {% set model_exist = [] %}
        {% for ms in migrationScripts|sort() %}
            {% if ms|trim|lower == mt|trim|lower %}
                {{model_exist.append('1') }}
            {% endif %}
        {% endfor %}
        {{log("*** comparing for script ms " ~ ms ~ " and mt " ~ mt ~ " and exist = " ~ xist, info=true)}}
        {% if model_exist|length != 0 %}
            {{log("*** Checking for metadata " ~ mt, info=true)}}
            {{ migrations.append(mt) }}
        {% endif %}
    {% endfor %}
    {% set migrationYaml = [] %}
    {% for migration in migrations|sort %}
        {%- set migration_macro = context.get(migration) -%}
        {% set migrationYaml = fromyaml(migration_macro()) %}
        {{log("Running down migration for " ~ migration, info=True)}}
        {% for node in graph.nodes.values()
            | selectattr("resource_type", "equalto", "model")%}
            {% for model in migrationYaml.down %}
                {% if model.name == node.name %}
                    {{modifyColumns(node.relation_name, model.columns)}}
                    {{runSqls(model.sql)}}
                {% endif%}
            {% endfor %}
        {% endfor %}
        {{deleteMigrationMetadata(migration)}}
    {% endfor %}
{%- endmacro -%}

{%- macro modifyColumns(relation_name, columns)-%}
{% for col in columns %}
    {{log("Processing for column " ~ col.name, info=True)}}
    {% if col.type|trim|lower == 'add' %}
        {% set alterSql %}
        ALTER TABLE {{relation_name}} ADD column {{col.name}} {{col.datatype}}
        {% endset %}
    {% endif %}
    {% if col.type|trim|lower == 'modify' %}
        {% set alterSql %}
        ALTER TABLE {{relation_name}} Modify column {{col.name}} {{col.datatype}}
        {% endset %}
    {% endif %}
    {% if col.type|trim|lower == 'drop' %}
        {% set alterSql %}
        ALTER TABLE {{relation_name}} drop column {{col.name}}
        {% endset %}
    {% endif %}
    {{log(" Executing the alter table script " ~ alterSql, info=True)}}
    {% do run_query(alterSql) %}
{% endfor %}
{% endmacro %}

{%- macro runSqls(sqls)-%}
{% for s in sqls|sort(attribute='seq') %}
    {{log("Processing for sql " ~ s.text, info=True)}}
    {% do run_query(s.text) %}
{% endfor %}
{% endmacro %}