-- Send status per (sale_date, revision). The only tables the producer owns in the branch database.
-- MySQL variant of db/migration/postgresql. Times are DATETIME(6) in the connection time zone: the JDBC URL must set
-- connectionTimeZone=Asia/Bangkok (README), like the back-office's own times.
create table sync_log (
    sale_date   date          not null,
    revision    int           not null,
    status      varchar(10)   not null check (status in ('SENT', 'FAILED')),
    attempts    int           not null,
    last_error  varchar(1000),
    event_id    varchar(36),
    sent_at     datetime(6),
    updated_at  datetime(6)   not null,
    primary key (sale_date, revision)
);
