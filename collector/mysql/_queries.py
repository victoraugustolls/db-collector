schema_query = """
with tables as (
    select table_schema,
           table_name
      from information_schema.tables
     where table_type = 'BASE TABLE'
       and table_schema not in ('mysql', 'performance_schema', 'sys')
)
select concat_ws('.', tbs.table_schema, tbs.table_name) as table_name,
       json_arrayagg(
           json_object(
               'name', cls.column_name,
               'type', cls.column_type,
               'nullable', cls.is_nullable
           )
       ) as columns
  from tables as tbs
  join information_schema.columns cls
        on cls.table_schema = tbs.table_schema
       and cls.table_name = tbs.table_name
group by 1;
"""

statements_query = """
   select min(event_id)                                 as id,
          count(*)                                      as calls,
          digest_text                                   as query,
          sum(truncate(timer_wait/1000000000000,6)) as total_exec_time,
          avg(truncate(timer_wait/1000000000000,6)) as avg_exec_time
     from performance_schema.events_statements_history_long
    where digest_text regexp %s
      and digest_text not like %s
 group by digest_text;
"""

activity_query = """
select sql_text as query
  from performance_schema.events_statements_history_long
 where event_id = %s;
"""