import sys

sys.path.append("/Users/alnikonov/s5-lessons/dags")
from examples.dds.stg_to_dds import con


def insert_data_settlement_report():
    connection = con.get_pg_connection()
    with connection.cursor() as cur:
        cur.execute(
            """
                    INSERT INTO cdm.dm_settlement_report (restaurant_id,
                                                        restaurant_name,
                                                        settlement_date,
                                                        orders_count,
                                                        orders_total_sum,
                                                        orders_bonus_payment_sum,
                                                        orders_bonus_granted_sum,
                                                        order_processing_fee,
                                                        restaurant_reward_sum)
                    select dor.restaurant_id,
                        dr.restaurant_name,
                        dt.date                                                               as settlement_date,
                        count(distinct dor.order_key)                                         as orders_count,
                        sum(fps.total_sum)                                                    as orders_total_sum,
                        sum(fps.bonus_payment)                                                as orders_bonus_payment_sum,
                        sum(fps.bonus_grant)                                                  as orders_bonus_granted_sum,
                        sum(fps.total_sum) * 0.25                                             as order_processing_fee,
                        sum(fps.total_sum) - (sum(fps.total_sum) * 0.25) - sum(bonus_payment) as restaurant_reward_sum

                    from dds.fct_product_sales fps
                            join dds.dm_orders dor ON fps.order_id = dor.id
                            join dds.dm_restaurants dr ON dor.restaurant_id = dr.id
                            join dds.dm_timestamps dt on dt.id = dor.timestamp_id
                    where order_status = 'CLOSED'
                    AND dt.date::timestamp > COALESCE(
                            (SELECT workflow_settings ->> 'last_loaded_date'
                            FROM dds.srv_wf_settings
                            WHERE workflow_key = 'dm_settlement_report_workflow')::TIMESTAMP,
                            '01-01-0001')
                    group by dor.restaurant_id,
                            dr.restaurant_name,
                            dt.date;
            """
        )

        cur.execute(
            """
                    INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                    SELECT 'dm_settlement_report_workflow',
                        jsonb_build_object('last_loaded_date', coalesce(MAX(settlement_date), '01-01-0001')) as workflow_setting
                    FROM cdm.dm_settlement_report
                    ON CONFLICT (workflow_key) DO UPDATE
                        SET workflow_settings = EXCLUDED.workflow_settings
                """
        )

    connection.commit()

    connection.close()

    return print("done")
