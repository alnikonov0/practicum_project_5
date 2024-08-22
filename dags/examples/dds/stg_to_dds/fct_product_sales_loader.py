import sys

sys.path.append("/Users/alnikonov/s5-lessons/dags")
from examples.dds.stg_to_dds import con


def insert_data_fact():
    connection = con.get_pg_connection()
    with connection.cursor() as cur:
        cur.execute(
            """
                INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                with events as (select o.event_value::json ->> 'order_id'                                                as order_id,
                                    jsonb_array_elements(o.event_value::jsonb -> 'product_payments') ->> 'product_id' as product_id,
                                    (jsonb_array_elements(o.event_value::jsonb -> 'product_payments') ->>
                                        'bonus_payment')::numeric(19, 5)                                                 as bonus_payment,
                                    (jsonb_array_elements(o.event_value::jsonb -> 'product_payments') ->>
                                        'bonus_grant')::numeric(19, 5)                                                   as bonus_grant
                                from stg.bonussystem_events o),
                    orders as (SELECT o.object_id                                                                 as order_id,
                                    jsonb_array_elements(o.object_value::jsonb -> 'order_items') ->> 'id'       as product_id,
                                    jsonb_array_elements(o.object_value::jsonb -> 'order_items') ->> 'quantity' as count,
                                    jsonb_array_elements(o.object_value::jsonb -> 'order_items') ->> 'price'    as price
                                FROM stg.ordersystem_orders o),
                    final as (select o.product_id,
                                    o.order_id,
                                    o.count::int,
                                    o.price::int,
                                    o.price::int * o.count::int as total_sum,
                                    e.bonus_payment,
                                    e.bonus_grant
                            from orders o
                                        join events e USING (product_id, order_id))

                select dp.id,
                    dor.id,
                    count,
                    price,
                    total_sum,
                    coalesce(bonus_payment, 0) as bonus_payment,
                    coalesce(bonus_grant, 0)   as bonus_grant
                from final f
                        join dds.dm_products dp USING (product_id)
                        join dds.dm_orders dor ON f.order_id = dor.order_key
                WHERE CONCAT(dor.id, dp.id)::bigint > COALESCE((SELECT workflow_settings ->> 'last_loaded_id'
                                                                from dds.srv_wf_settings
                                                                where workflow_key = 'fct_product_sales_stg_to_dds_workflow')::bigint,
                                                            0)
            """
        )

        cur.execute(
            """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                SELECT 'fct_product_sales_stg_to_dds_workflow',
                    jsonb_build_object('last_loaded_id', coalesce(MAX(CONCAT(order_id, product_id)::bigint), 0)) as workflow_setting
                FROM dds.fct_product_sales
                ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings
                """
        )

    connection.commit()

    connection.close()

    return print("done")
