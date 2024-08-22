CREATE TABLE IF NOT EXISTS stg.ordersystem_users
(
    id           serial
        primary key,
    object_id    varchar   not null
        constraint ordersystem_users_object_id_uindex
            unique,
    object_value text      not null,
    update_ts    timestamp not null
);