import sys
sys.path.append('/Users/alnikonov/s5-lessons/dags')
from examples.dds.stg_to_dds import con


def insert_data_ts():
    connection = con.get_pg_connection()

    with connection.cursor() as cur:

        cur.execute('''
        WITH datamart AS (
            SELECT 
                TO_TIMESTAMP(object_value::json ->> 'date', 'YYYY-MM-DD HH24:MI:SS') AS date
            FROM stg.ordersystem_orders
            WHERE object_value::json ->> 'final_status' IN ('CLOSED')
        )
        INSERT INTO dds.dm_timestamps (ts, year, month, day, time, date)
        SELECT
            date::timestamp          AS ts,
            date_part('year', date)  AS year,
            date_part('month', date) AS month,
            date_part('day', date)   AS day,
            date::time               AS time,
            date::date               AS date
        FROM datamart dm
        WHERE dm.date::timestamp > COALESCE(
            (SELECT workflow_settings ->> 'last_loaded_ts'
             FROM dds.srv_wf_settings
             WHERE workflow_key = 'timestamp_stg_to_dds_workflow')::TIMESTAMP, 
            '01-01-0001'
        )
        ''')

        cur.execute(
            '''
            INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
            SELECT 'timestamp_stg_to_dds_workflow', jsonb_build_object('last_loaded_ts', coalesce(MAX(ts),'01-01-0001')) AS workflow_setting
            FROM dds.dm_timestamps
            ON CONFLICT (workflow_key) DO UPDATE
            SET workflow_settings = EXCLUDED.workflow_settings
            '''
        )

    connection.commit()

    connection.close()

    return print('done')
