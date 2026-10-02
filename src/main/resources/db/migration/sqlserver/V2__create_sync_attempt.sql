-- History of send attempts: one row per attempt, never updated or deleted by the producer (SQL Server variant).
create table sync_attempt (
    id           bigint            identity(1, 1) primary key,
    sale_date    date              not null,
    revision     int               not null,
    attempted_at datetimeoffset(6) not null,
    result       varchar(10)       not null check (result in ('SENT', 'FAILED')),
    error        varchar(1000),
    event_id     varchar(36)
);

create index ix_sync_attempt_day on sync_attempt (sale_date, revision);
