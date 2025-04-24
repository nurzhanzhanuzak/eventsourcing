import psycopg
from psycopg.sql import SQL, Identifier

from eventsourcing.persistence import PersistenceError
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


def drop_postgres_table(datastore: PostgresDatastore, table_name: str) -> None:
    statement = SQL("DROP TABLE {0}.{1}").format(
        Identifier(datastore.schema), Identifier(table_name)
    )
    # print(f"Dropping table {datastore.schema}.{table_name}")
    try:
        with datastore.transaction(commit=True) as curs:
            curs.execute(statement, prepare=False)
    except PersistenceError:
        pass
