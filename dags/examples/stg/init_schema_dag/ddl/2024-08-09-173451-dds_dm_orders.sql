CREATE TABLE IF NOT EXISTS dds.dm_orders
(
    id            serial
        primary key,
    order_key     varchar not null
        constraint order_key_unique
            unique,
    order_status  varchar not null,
    restaurant_id integer not null
        references dds.dm_restaurants(id),
    timestamp_id  integer not null
        references dds.dm_timestamps(id),
    user_id       integer not null
);