with test as (select dsra.id,
                     dsra.restaurant_id,
                     dsra.restaurant_name,
                     dsra.settlement_year,
                     dsra.settlement_month,
                     dsra.orders_count,
                     dsra.orders_total_sum,
                     dsra.orders_bonus_payment_sum,
                     dsra.orders_bonus_granted_sum,
                     dsra.order_processing_fee,
                     dsra.restaurant_reward_sum,
                     dsre.restaurant_id,
                     dsre.restaurant_name,
                     dsre.settlement_year,
                     dsre.settlement_month,
                     dsre.orders_count,
                     dsre.orders_total_sum,
                     dsre.orders_bonus_payment_sum,
                     dsre.orders_bonus_granted_sum,
                     dsre.order_processing_fee,
                     dsre.restaurant_reward_sum
              from public_test.dm_settlement_report_actual dsra
                       FULL OUTER JOIN public_test.dm_settlement_report_expected dsre on dsra.id = dsre.id
              where dsra.id is null
                 or dsre.id is null)
select current_timestamp                                  as test_date_time,
       'test_01'                                          as test_name,
       case when test.id is null then false else true end as test_result
from test