-- History of send attempts: one row per attempt, never updated or deleted by the producer.
-- sync_log keeps the current status per (sale_date, revision); this table keeps every attempt that led to it.
-- No automatic clean-up yet: while HQ is unreachable, each pending day adds one row per send round (hourly).
create table sync_attempt (
    id           bigint        generated always as identity primary key,
    sale_date    date          not null,
    revision     integer       not null,
    attempted_at timestamptz   not null,
    result       varchar(10)   not null check (result in ('SENT', 'FAILED')),
    error        varchar(1000),
    -- message written for this attempt; null when the data could not be written as a message
    event_id     varchar(36)
);

create index ix_sync_attempt_day on sync_attempt (sale_date, revision);
