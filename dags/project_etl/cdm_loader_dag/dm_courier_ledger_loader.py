import sys

sys.path.append("/lessons/dags/project_etl")

from project_etl.con import get_pg_connection


connection = get_pg_connection()


def insert_data_courier_ledger():
    with connection.cursor() as cur:
        cur.execute(
            """
                insert into cdm.dm_courier_ledger (courier_id,
                                                courier_name,
                                                settlement_year,
                                                settlement_month,
                                                orders_count,
                                                orders_total_sum,
                                                rate_avg,
                                                order_processing_fee,
                                                courier_order_sum,
                                                courier_tips_sum,
                                                courier_reward_sum)
                with fact_sales as (select order_id,
                                        sum(count)            as orders_count,
                                        sum(total_sum)        as orders_total_sum,
                                        sum(total_sum) * 0.25 as order_processing_fee
                                    from dds.fct_product_sales
                                    group by order_id),
                    data_prep as (select dlv.courier_id,
                                        courier_name,
                                        EXTRACT(YEAR FROM dt.ts)  as settlement_year,
                                        EXTRACT(MONTH FROM dt.ts) as settlement_month,
                                        sum(fs.orders_count)      as orders_count,
                                        sum(fs.orders_total_sum)  as orders_total_sum,
                                        sum(order_processing_fee) as order_processing_fee,
                                        avg(rate)                 as rate_avg,
                                        sum(CASE
                                                WHEN rate < 4 THEN GREATEST(orders_total_sum * 0.05, 100)
                                                WHEN rate >= 4 AND rate < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150)
                                                WHEN rate >= 4.5 AND rate < 4.9
                                                    THEN GREATEST(orders_total_sum * 0.08, 175)
                                                WHEN rate >= 4.9 THEN GREATEST(orders_total_sum * 0.10, 200)
                                            END)                  AS courier_order_sum,
                                        sum(dlv.tip_sum)          as courier_tips_sum

                                from dds.dm_deliveries dlv
                                            join dds.dm_couriers dc on dc.courier_id = dlv.courier_id
                                            join dds.dm_orders d on dlv.order_id = d.order_key
                                            join dds.dm_timestamps dt on dt.id = d.timestamp_id
                                            join fact_sales fs ON d.id = fs.order_id
                                group by dlv.courier_id, courier_name, EXTRACT(YEAR FROM dt.ts), EXTRACT(MONTH FROM dt.ts))
                select courier_id,
                    courier_name,
                    settlement_year,
                    settlement_month,
                    orders_count,
                    orders_total_sum,
                    rate_avg,
                    order_processing_fee,
                    courier_order_sum,
                    courier_tips_sum,
                    courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum
                from data_prep
                where settlement_month > COALESCE(
                        (SELECT workflow_settings ->> 'last_loaded_month'
                        FROM dds.srv_wf_settings
                        WHERE workflow_key = 'dm_courier_ledger_workflow')::INT,
                        0)
            """
        )

        cur.execute(
            """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                SELECT 'dm_courier_ledger_workflow',
                    jsonb_build_object('last_loaded_month', coalesce(MAX(settlement_month), 0)) as workflow_setting
                FROM cdm.dm_courier_ledger
                ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings
                """
        )

    connection.commit()

    connection.close()

    return print("done")
