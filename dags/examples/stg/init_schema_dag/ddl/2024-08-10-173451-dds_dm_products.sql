CREATE TABLE IF NOT EXISTS dds.dm_products
(
    id            serial
        primary key,
    product_id    varchar                  not null,
    product_name  varchar                  not null,
    product_price numeric(14, 2) default 0 not null
        constraint dm_products_count_check
            check (product_price >= (0)::numeric),
    restaurant_id integer                  not null
        references dds.dm_restaurants(id),
    active_from   timestamp                not null,
    active_to     timestamp                not null
);