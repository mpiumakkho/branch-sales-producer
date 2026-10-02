-- History of send attempts: one row per attempt, never updated or deleted by the producer (MySQL variant).
create table sync_attempt (
    id           bigint        not null auto_increment primary key,
    sale_date    date          not null,
    revision     int           not null,
    attempted_at datetime(6)   not null,
    result       varchar(10)   not null check (result in ('SENT', 'FAILED')),
    error        varchar(1000),
    event_id     varchar(36),
    index ix_sync_attempt_day (sale_date, revision)
);
