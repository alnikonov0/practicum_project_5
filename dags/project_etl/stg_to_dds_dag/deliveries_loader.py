import sys

sys.path.append("/lessons/dags/project_etl")

from project_etl.con import get_pg_connection


connection = get_pg_connection()


def insert_data_deliveries():
    with connection.cursor() as cur:
        cur.execute(
            """
                INSERT INTO dds.dm_deliveries
                select id,
                    order_id,
                    order_ts,
                    delivery_id,
                    courier_id,
                    delivery_ts, -- для инкрементальной загрузки
                    rate,
                    sum,
                    tip_sum
                from stg.api_deliveries
                WHERE delivery_ts::timestamp > COALESCE(
                        (SELECT workflow_settings ->> 'last_loaded_ts'
                        FROM dds.srv_wf_settings
                        WHERE workflow_key = 'deliveries_stg_to_dds_workflow')::TIMESTAMP,
                        '01-01-0001')
                """
        )
        cur.execute(
            """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                SELECT 'deliveries_stg_to_dds_workflow',
                    jsonb_build_object('last_loaded_ts', coalesce(MAX(delivery_ts), '01-01-0001')) AS workflow_setting
                FROM dds.dm_deliveries
                ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings
            """
        )

    connection.commit()

    connection.close()

    return print("done")
