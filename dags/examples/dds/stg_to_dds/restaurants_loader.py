import sys

sys.path.append("/Users/alnikonov/s5-lessons/dags")
from examples.dds.stg_to_dds import con


def insert_data_rest():
    connection = con.get_pg_connection()

    with connection.cursor() as cur:
        cur.execute(
            """
                INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                SELECT object_value::json ->> '_id'                    as restaurant_id,
                    object_value::json ->> 'name'                      restaurant_name,
                    (object_value::json ->> 'update_ts')::timestamp as active_from,
                    '2099-12-31 00:00:00.000'::timestamp            as active_to
                FROM stg.ordersystem_restaurants
                WHERE update_ts::timestamp > COALESCE((SELECT workflow_settings ->> 'last_loaded_ts'
                                                    from dds.srv_wf_settings
                                                    where workflow_key = 'restaurants_stg_to_dds_workflow')::TIMESTAMP, '01-01-0001')
                ON CONFLICT (restaurant_id) DO UPDATE
                    SET restaurant_id = excluded.restaurant_id
            """
        )

        cur.execute(
            """
                    INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                    SELECT 'restaurants_stg_to_dds_workflow', jsonb_build_object('last_loaded_ts', coalesce(MAX(active_from),'01-01-0001')) as workflow_setting
                    FROM dds.dm_restaurants
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET
                        workflow_settings = EXCLUDED.workflow_settings
                """
        )

    connection.commit()

    connection.close()

    return print("done")
