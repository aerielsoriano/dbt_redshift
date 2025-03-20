{% macro check_catalog() %}
   {% set query %}
        with
            late_binding as (
                select
                    table_schema,
                    table_name,
                    'LATE BINDING VIEW'::varchar as table_type,
                    null::text as table_comment,
                    column_name,
                    column_index,
                    column_type,
                    null::text as column_comment
                from pg_get_late_binding_view_cols() cols(
                    table_schema name,
                    table_name name,
                    column_name name,
                    column_type varchar,
                    column_index int
                )
            ),
            early_binding as (
                select
                    sch.nspname as table_schema,
                    tbl.relname as table_name,
                    case
                        when tbl.relkind = 'v' and mat_views.table_name is not null then 'MATERIALIZED VIEW'
                        when tbl.relkind = 'v' then 'VIEW'
                        else 'BASE TABLE'
                    end as table_type,
                    tbl_desc.description as table_comment,
                    col.attname as column_name,
                    col.attnum as column_index,
                    pg_catalog.format_type(col.atttypid, col.atttypmod) as column_type,
                    col_desc.description as column_comment
                from pg_catalog.pg_namespace sch
                join pg_catalog.pg_class tbl on tbl.relnamespace = sch.oid
                join pg_catalog.pg_attribute col on col.attrelid = tbl.oid
                left outer join pg_catalog.pg_description tbl_desc on tbl_desc.objoid = tbl.oid and tbl_desc.objsubid = 0
                left outer join pg_catalog.pg_description col_desc on col_desc.objoid = tbl.oid and col_desc.objsubid = col.attnum
                left outer join information_schema.views mat_views on mat_views.table_schema = sch.nspname and mat_views.table_name = tbl.relname and mat_views.view_definition ilike '%create materialized view%' and mat_views.table_catalog = current_database()
                where tbl.relkind in ('r', 'v', 'f', 'p')
                and col.attnum > 0
                and not col.attisdropped
            ),
            unioned as (
                select * from early_binding
                union all
                select * from late_binding
            ),
            table_owners as (
                select
                    schemaname as table_schema,
                    tablename as table_name,
                    tableowner as table_owner
                from pg_tables
                union all
                select
                    schemaname as table_schema,
                    viewname as table_name,
                    viewowner as table_owner
                from pg_views
            )
        select current_database() as table_database, *
        from unioned
        join table_owners using (table_schema, table_name)
        order by "column_index"
   {% endset %}

   {% if execute %}
    {% set res = run_query(query) %}
    {% for row in res %}
        {% do print(dict(row)) %}
    {% endfor %}
   {% endif %}
{% endmacro %}