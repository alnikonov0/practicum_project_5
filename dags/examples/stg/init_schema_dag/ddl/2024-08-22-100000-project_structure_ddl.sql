CREATE TABLE IF NOT EXISTS stg.api_deliveries
(
    id          serial PRIMARY KEY NOT NULL,
    order_id    varchar            NOT NULL,
    order_ts    timestamp          NOT NULL,
    delivery_id varchar            NOT NULL,
    courier_id  varchar            NOT NULL,
    address     varchar            NOT NULL,
    delivery_ts timestamp,
    rate        int                NOT NULL,
    sum         int                NOT NULL,
    tip_sum     int                NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.api_couriers
(
    id         serial PRIMARY KEY NOT NULL,
    courier_id varchar            NOT NULL,
    name       varchar            NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_couriers
(
    id           serial primary key,
    courier_id   VARCHAR(50)  NOT NULL UNIQUE,
    courier_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_deliveries
(
    id          serial PRIMARY KEY NOT NULL,
    order_id    varchar            NOT NULL UNIQUE,
    order_ts    timestamp          NOT NULL,
    delivery_id varchar            NOT NULL,
    courier_id  varchar            NOT NULL REFERENCES dds.dm_couriers (courier_id),
    delivery_ts timestamp          NOT NULL,
    rate        int                NOT NULL,
    sum         int                NOT NULL,
    tip_sum     int                NOT NULL
);

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger
(
    id                   SERIAL PRIMARY KEY NOT NULL,
    courier_id           VARCHAR            NOT NULL REFERENCES dds.dm_couriers (courier_id),
    courier_name         VARCHAR            NOT NULL,
    settlement_year      INT                NOT NULL,
    settlement_month     INT                NOT NULL CHECK (settlement_month BETWEEN 1 AND 12),
    orders_count         INT                NOT NULL,
    orders_total_sum     NUMERIC(15, 2)     NOT NULL,
    rate_avg             NUMERIC(3, 2)      NOT NULL,
    order_processing_fee NUMERIC(15, 2)     NOT NULL,
    courier_order_sum    NUMERIC(15, 2)     NOT NULL,
    courier_tips_sum     NUMERIC(15, 2)     NOT NULL,
    courier_reward_sum   NUMERIC(15, 2)     NOT NULL
);