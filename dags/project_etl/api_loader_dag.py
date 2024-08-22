import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

sys.path.append("/lessons/dags/project_etl")
sys.path.append("/lessons/dags/project_etl/api_to_stg_dag")
sys.path.append("/lessons/dags/project_etl/stg_to_dds_dag")
sys.path.append("/lessons/dags/project_etl/cdm_loader_dag")

from api_to_stg_dag.get_couriers import loader_couriers
from api_to_stg_dag.get_deliveries import loader_deliveries

from stg_to_dds_dag.couriers_loader import insert_data_couriers
from stg_to_dds_dag.deliveries_loader import insert_data_deliveries

from cdm_loader_dag.dm_courier_ledger_loader import insert_data_courier_ledger


with DAG(
    dag_id="COURIER_DELIVERIES_LOADER_DAG",
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["project", "stg", "api", "loader"],
    is_paused_upon_creation=True,
) as dag:

    # ИНТЕГРАЦИЯ ДАННЫХ ЧЕРЕЗ АПИ В STG
    load_couriers_stg_task = PythonOperator(
        task_id="load_couriers_stg_task",
        python_callable=loader_couriers,
    )

    load_deliveries_stg_task = PythonOperator(
        task_id="load_deliveries_stg_task",
        python_callable=loader_deliveries,
    )

    # ЗАЛИВКА ДАННЫХ В DDS
    load_couriers_dds_task = PythonOperator(
        task_id="load_couriers_dds_task",
        python_callable=insert_data_couriers,
    )

    load_deliveries_dds_task = PythonOperator(
        task_id="load_deliveries_dds_task",
        python_callable=insert_data_deliveries,
    )

    # ЗАЛИВКА ДАННЫХ В CDM
    load_courier_cdm_ledger = PythonOperator(
        task_id="load_courier_cdm_ledger",
        python_callable=insert_data_courier_ledger,
    )

    (
        load_couriers_stg_task
        >> load_deliveries_stg_task
        >> load_couriers_dds_task
        >> load_deliveries_dds_task
        >> load_courier_cdm_ledger
    )
