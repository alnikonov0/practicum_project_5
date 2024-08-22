CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report
(
    id                       serial
        primary key,
    restaurant_id            int              not null references dds.dm_restaurants(id),
    restaurant_name          varchar(50)              not null,
    settlement_date          date                     not null
        constraint dm_settlement_report_settlement_date_check
            check ((settlement_date >= '2022-01-01'::date) AND (settlement_date < '2500-01-01'::date)),
    orders_count             integer        default 0 not null
        constraint dm_settlement_report_orders_count_check
            check (orders_count >= 0),
    orders_total_sum         numeric(14, 2) default 0 not null
        constraint orders_total_sum_check
            check (orders_total_sum > (0)::numeric)
        constraint dm_settlement_report_orders_total_sum_check
            check (orders_total_sum >= (0)::numeric),
    orders_bonus_payment_sum numeric(14, 2) default 0 not null
        constraint orders_bonus_payment_sum_check
            check (orders_bonus_payment_sum > (0)::numeric)
        constraint dm_settlement_report_orders_bonus_payment_sum_check
            check (orders_bonus_payment_sum >= (0)::numeric),
    orders_bonus_granted_sum numeric(14, 2) default 0 not null
        constraint orders_bonus_granted_sum_check
            check (orders_bonus_granted_sum > (0)::numeric)
        constraint dm_settlement_report_orders_bonus_granted_sum_check
            check (orders_bonus_granted_sum >= (0)::numeric),
    order_processing_fee     numeric(14, 2) default 0 not null
        constraint dm_settlement_report_order_processing_fee_check
            check (order_processing_fee >= (0)::numeric),
    restaurant_reward_sum    numeric(14, 2) default 0 not null
        constraint restaurant_reward_sum_check
            check (restaurant_reward_sum > (0)::numeric)
        constraint dm_settlement_report_restaurant_reward_sum_check
            check (restaurant_reward_sum >= (0)::numeric),
    constraint dm_settlement_report_unique
        unique (restaurant_id, settlement_date)
);