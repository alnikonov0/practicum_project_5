import logging
import pendulum
from airflow.decorators import dag, task
from examples.dds.stg_to_dds.user_loader import LoaderUsers
from examples.dds.stg_to_dds.timestamp_loader import insert_data_ts
from examples.dds.stg_to_dds.restaurants_loader import insert_data_rest
from examples.dds.stg_to_dds.products_loader import insert_data_products
from examples.dds.stg_to_dds.orders_loader import insert_data_orders
from examples.dds.stg_to_dds.fct_product_sales_loader import insert_data_fact
from examples.dds.stg_to_dds.settlement_report_loader import (
    insert_data_settlement_report,
)
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага.
    catchup=False,  # Не выполнять даг для пропущенных периодов.
    tags=["sprint5", "stg", "origin", "example"],  # Теги для фильтрации.
    is_paused_upon_creation=True,  # Запускать даг сразу.
)
def STG_TO_DDS_DAG():
    # Создаем подключения к базам данных.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task(task_id="load_users")
    def load_users():
        users_loader = LoaderUsers()

    @task(task_id="load_timestamp")
    def load_timestamp():
        timestamp_loader = insert_data_ts()

    @task(task_id="load_restaurants")
    def load_restaurants():
        restaurants_loader = insert_data_rest()

    @task(task_id="load_products")
    def load_products():
        products_loader = insert_data_products()

    @task(task_id="load_orders")
    def load_orders():
        orders_loader = insert_data_orders()

    @task(task_id="load_facts")
    def load_facts():
        facts_loader = insert_data_fact()

    @task(task_id="load_report")
    def load_report():
        report_loader = insert_data_settlement_report()

    task_load_users = load_users()
    task_load_timestamp = load_timestamp()
    task_load_restaurants = load_restaurants()
    task_load_products = load_products()
    task_load_orders = load_orders()
    task_load_facts = load_facts()
    task_load_report = load_report()

    (
        task_load_users
        >> task_load_timestamp
        >> task_load_restaurants
        >> task_load_products
        >> task_load_orders
        >> task_load_facts
        >> task_load_report
    )


stg_to_dds_loader_dag = STG_TO_DDS_DAG()
