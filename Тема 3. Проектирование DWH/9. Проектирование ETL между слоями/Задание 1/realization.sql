
INSERT INTO cdm.dm_settlement_report (restaurant_id,
       restaurant_name,
       settlement_date,
       orders_count,
       orders_total_sum,
       orders_bonus_payment_sum,
       orders_bonus_granted_sum,
       order_processing_fee,
       restaurant_reward_sum)
select
--        fps.order_id,
       dor.restaurant_id,
       dr.restaurant_name,
       dt.date as settlement_date,
       count(distinct fps.order_id) as orders_count,
--        fps.price,
       sum(fps.total_sum) as orders_total_sum,
       sum(fps.bonus_payment) as orders_bonus_payment_sum,
       sum(fps.bonus_grant) as orders_bonus_granted_sum,
       sum(fps.total_sum) * 0.25 as order_processing_fee,
       sum(fps.total_sum) - (sum(fps.total_sum) * 0.25) - sum(bonus_payment) as restaurant_reward_sum

from dds.fct_product_sales fps
         join dds.dm_orders dor ON fps.order_id = dor.id
         join dds.dm_restaurants dr ON dor.restaurant_id = dr.id
         join dds.dm_timestamps dt on dt.id = dor.timestamp_id
where order_status = 'CLOSED'
group by dor.restaurant_id,
       dr.restaurant_name,
       dt.date