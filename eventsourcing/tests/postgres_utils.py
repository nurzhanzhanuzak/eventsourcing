import os

import psycopg
from psycopg.sql import SQL, Identifier

from eventsourcing.postgres import PostgresDatastore


def pg_close_all_connections(
    name: str = "eventsourcing",
    host: str = "127.0.0.1",
    port: str = "5432",
    user: str = "postgres",
    password: str = "postgres",  # noqa: S107
) -> None:
    try:
        # For local development... probably.
        pg_conn = psycopg.connect(
            dbname=name,
            host=host,
            port=port,
        )
    except psycopg.Error:
        # For GitHub actions.
        """CREATE ROLE postgres LOGIN SUPERUSER PASSWORD 'postgres';"""
        pg_conn = psycopg.connect(
            dbname=name,
            host=host,
            port=port,
            user=user,
            password=password,
        )
    close_all_connections = """
    SELECT
        pg_terminate_backend(pid)
    FROM
        pg_stat_activity
    WHERE
        -- don't kill my own connection!
        pid <> pg_backend_pid();

    """
    pg_conn_cursor = pg_conn.cursor()
    pg_conn_cursor.execute(close_all_connections)


def drop_tables() -> None:

    for schema in ["public", "myschema"]:
        datastore = PostgresDatastore(
            dbname=os.environ.get("POSTGRES_DBNAME", "eventsourcing"),
            host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            user=os.environ.get("POSTGRES_USER", "eventsourcing"),
            password=os.environ.get("POSTGRES_PASSWORD", "eventsourcing"),
            schema=schema,
        )
        with datastore.transaction(commit=True) as curs:
            select_table_names = SQL(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s"
            )
            fetchall = curs.execute(select_table_names, (datastore.schema,)).fetchall()
            for row in fetchall:
                table_name = row["table_name"]
                # print(f"Dropping table '{table_name}' in schema '{schema}'")
                statement = SQL("DROP TABLE IF EXISTS {0}.{1}").format(
                    Identifier(datastore.schema), Identifier(table_name)
                )
                curs.execute(statement, prepare=False)
                # print(f"Dropped table '{table_name}' in schema '{schema}'")
