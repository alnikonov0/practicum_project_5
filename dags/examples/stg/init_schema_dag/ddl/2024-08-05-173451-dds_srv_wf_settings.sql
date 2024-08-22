CREATE TABLE IF NOT EXISTS dds.srv_wf_settings
(
    id                serial
        primary key,
    workflow_key      varchar not null unique,
    workflow_settings json    not null
);