schema = """
create table if not exists schema
(
    table_name  text primary key,
    columns     text not null
);
"""

statement = """
create table if not exists statement
(
    id                  text primary key,
    query               text not null,
    normalized_query    text,
    plan                text
);
"""

statement_execution = """
create table if not exists statement_execution
(
    id                   text       not null references statement (id),
    calls                integer    not null,
    total_execution_time numeric    not null,
    mean_execution_time  numeric    not null,
    created_at           text default current_timestamp
);
"""
