import os
import sys

from .api_to_stg_dag.get_couriers import loader_couriers  # noqa
from .api_to_stg_dag.get_deliveries import loader_deliveries  # noqa

from .stg_to_dds_dag.couriers_loader import insert_data_couriers  # noqa
from .stg_to_dds_dag.deliveries_loader import insert_data_deliveries  # noqa

from .cdm_loader_dag.dm_courier_ledger_loader import insert_data_courier_ledger  # noqa

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
sys.path.append("/lessons/dags/project_etl")
sys.path.append("/lessons/dags/project_etl/create_structure")
sys.path.append("/lessons/dags/project_etl/api_to_stg_dag")
sys.path.append("/lessons/dags/project_etl/stg_to_dds_dag")
sys.path.append("/lessons/dags/project_etl/cdm_loader_dag")
