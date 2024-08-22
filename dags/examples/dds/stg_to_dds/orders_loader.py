import sys

sys.path.append("/Users/alnikonov/s5-lessons/dags")
from examples.dds.stg_to_dds import con


def insert_data_orders():
    connection = con.get_pg_connection()

    with connection.cursor() as cur:
        cur.execute(
            """
                        INSERT INTO dds.dm_orders (order_key, order_status, restaurant_id, timestamp_id, user_id)
                        SELECT
                                            o.object_id AS order_key,
                                            o.object_value::json ->> 'final_status' AS order_status,
                                            dr.id AS restaurant_id,
                                            ts.id AS timestamp_id,
                                            du.id AS user_id
                                        FROM stg.ordersystem_orders o
                                        JOIN dds.dm_restaurants dr ON o.object_value::json -> 'restaurant' ->> 'id' = dr.restaurant_id
                                        JOIN dds.dm_users du ON o.object_value::json -> 'user' ->> 'id' = du.user_id
                                        JOIN dds.dm_timestamps ts ON TO_TIMESTAMP(o.object_value::json ->> 'date', 'YYYY-MM-DD HH24:MI:SS:MS') = ts.ts
                        WHERE ts.id > COALESCE((SELECT workflow_settings ->> 'last_loaded_id'
                                                                                                from dds.srv_wf_settings
                                                                                                where workflow_key = 'orders_stg_to_dds_workflow')::int, 0)
                """
        )

        cur.execute(
            """
                    INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                        SELECT 'orders_stg_to_dds_workflow', jsonb_build_object('last_loaded_id', coalesce(MAX(timestamp_id),0)) as workflow_setting
                        FROM dds.dm_orders
                        ON CONFLICT (workflow_key) DO UPDATE
                        SET
                            workflow_settings = EXCLUDED.workflow_settings
                    """
        )
    connection.commit()

    connection.close()

    return print("done")
