schema_query = """
with tables as (
    select schemaname,
           tablename,
           tableowner
      from pg_catalog.pg_tables
     where schemaname != 'pg_catalog'
       and schemaname != 'information_schema'
)
select
    table_schema || '.' || table_name as table_name,
    json_agg(
        json_build_object(
            'name', column_name,
            'type', udt_name,
            'nullable', is_nullable
        )
    ) as columns
from tables tbs
    join information_schema.columns cls
        on cls.table_schema = tbs.schemaname and cls.table_name = tbs.tablename
where cls.table_catalog = $1
group by 1
;
"""

statements_query = """
select queryid,
       query,
       calls,
       total_exec_time,
       mean_exec_time
  from pg_stat_statements
 where dbid = (
    select oid 
      from pg_database
     where datname = $1
);
"""

activity_query = """
select query
  from pg_stat_activity
 where user is not null
   and query not like '%insufficient%'
   and query <> '';
"""