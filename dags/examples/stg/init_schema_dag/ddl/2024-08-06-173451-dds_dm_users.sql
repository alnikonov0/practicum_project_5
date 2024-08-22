CREATE TABLE IF NOT EXISTS dds.dm_users
(
    id         serial
        primary key,
    user_id    varchar,
    user_name  varchar,
    user_login varchar
);
