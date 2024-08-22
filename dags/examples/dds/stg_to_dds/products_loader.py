import sys

sys.path.append("/Users/alnikonov/s5-lessons/dags")
from examples.dds.stg_to_dds import con


def insert_data_products():
    connection = con.get_pg_connection()

    with connection.cursor() as cur:
        cur.execute(
            """
                INSERT INTO dds.dm_products (product_id, product_name, product_price, active_from, active_to, restaurant_id)
                SELECT
                    jsonb_array_elements(object_value::jsonb->'menu')->>'_id' AS product_id,
                    jsonb_array_elements(object_value::jsonb->'menu')->>'name' AS product_name,
                    (jsonb_array_elements(object_value::jsonb->'menu')->>'price')::numeric(18,2) AS product_price,
                    update_ts AS active_from,
                    '2099-12-31 00:00:00.000' AS active_to,
                    dr.id
                FROM stg.ordersystem_restaurants rest
                JOIN dds.dm_restaurants dr ON rest.object_id = dr.restaurant_id
                WHERE update_ts > COALESCE((SELECT workflow_settings ->> 'last_loaded_ts'
                                            FROM dds.srv_wf_settings
                                            WHERE workflow_key = 'products_stg_to_dds_workflow')::TIMESTAMP, '01-01-0001')
            """
        )

        cur.execute(
            """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                SELECT 
                    'products_stg_to_dds_workflow', 
                    jsonb_build_object('last_loaded_ts', COALESCE(MAX(active_from), '01-01-0001')) AS workflow_setting
                FROM dds.dm_products
                ON CONFLICT (workflow_key) 
                DO UPDATE SET workflow_settings = EXCLUDED.workflow_settings
            """
        )

    connection.commit()

    connection.close()

    return print("done")
