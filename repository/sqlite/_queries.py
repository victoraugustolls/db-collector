insert_schema = """
insert into schema (table_name, columns)
values (?, ?);
"""

statement_stats = """
select
    calls,
    total_execution_time,
    mean_execution_time,
    created_at
from
    statement_execution
where
    id = ?
order by
    created_at desc
limit 1
;
"""

insert_statement = """
insert into statement (id, query, normalized_query)
values (?, ?, ?);
"""

insert_statement_stats = """
insert into statement_execution (id, calls, total_execution_time, mean_execution_time, created_at)
values (?, ?, ?, ?, ?);
"""

list_statements = """
select id, query
from statement;
"""

set_plan = """
update statement
    set plan = ?
where normalized_query = ?;
"""

set_plan_by_id = """
update statement
    set plan = ?
where id = ?;
"""

debug_schema = """
select table_name, columns
from schema;
"""

debug_statements = """
select
    sub.id,
    sub.query,
    sub.plan,
    json_group_array(
        json_object(
            'calls', sub.calls,
            'total_execution_time', sub.total_execution_time,
            'mean_execution_time', sub.mean_execution_time,
            'created_at', sub.created_at
        )
    )
from (
    select
        stmt.id,
        stmt.query,
        stmt.plan,
        se.calls,
        se.total_execution_time,
        se.mean_execution_time,
        se.created_at
    from statement stmt
        join statement_execution se
            on stmt.id = se.id
    where normalized_query is not null
    order by se.created_at
    ) as sub
group by sub.id, sub.query
;
"""