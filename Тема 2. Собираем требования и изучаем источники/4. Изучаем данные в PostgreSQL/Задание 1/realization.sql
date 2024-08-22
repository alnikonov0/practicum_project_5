DROP TABLE IF EXISTS CDM.dm_settlement_report;
create table cdm.dm_settlement_report (
    id serial PRIMARY KEY NOT NULL,
    restaurant_id serial NOT NULL,
    restaurant_name varchar(50) NOT NULL,
    settlement_date date NOT NULL,
    orders_count integer not null,
    orders_total_sum integer not null,
    orders_bonus_payment_sum numeric(14, 2) not null,
    orders_bonus_granted_sum numeric(14, 2) not null,
    order_processing_fee numeric(14, 2) not null,
    restaurant_reward_sum numeric(14, 2) not null
)