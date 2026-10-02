-- Send status per (sale_date, revision). The only tables the producer owns in the branch database.
-- SQL Server variant of db/migration/postgresql.
create table sync_log (
    sale_date   date              not null,
    revision    int               not null,
    status      varchar(10)       not null check (status in ('SENT', 'FAILED')),
    attempts    int               not null,
    last_error  varchar(1000),
    event_id    varchar(36),
    sent_at     datetimeoffset(6),
    updated_at  datetimeoffset(6) not null,
    constraint pk_sync_log primary key (sale_date, revision)
);
