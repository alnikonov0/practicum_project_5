import sys

sys.path.append("/lessons/dags/project_etl")

from project_etl.con import get_pg_connection


connection = get_pg_connection()


def insert_data_couriers():
    with connection.cursor() as cur:
        cur.execute(
            """
                INSERT INTO DDS.dm_couriers (courier_id, courier_name)
                select courier_id, name
                from stg.api_couriers
                WHERE id > COALESCE((SELECT workflow_settings ->> 'last_loaded_id'
                                    from dds.srv_wf_settings
                                    where workflow_key = 'couriers_stg_to_dds_workflow')::INT, 0)
                ON CONFLICT (courier_id) DO UPDATE
                    SET courier_name = excluded.courier_name
                """
        )
        cur.execute(
            """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                SELECT 'couriers_stg_to_dds_workflow',
                    jsonb_build_object('last_loaded_id', coalesce(MAX(id), 0)) as workflow_setting
                FROM dds.dm_couriers
                ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings
            """
        )

    connection.commit()

    connection.close()

    return print("done")
