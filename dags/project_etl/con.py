from airflow.hooks.base import BaseHook
import psycopg


def get_pg_connection(conn_id: str = "PG_WAREHOUSE_CONNECTION") -> psycopg.Connection:
    """Получить соединение с PostgreSQL из Airflow Connection."""
    conn = BaseHook.get_connection(conn_id)

    sslmode = conn.extra_dejson.get("sslmode", "require")

    connection = psycopg.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        sslmode=sslmode,
    )

    return connection
