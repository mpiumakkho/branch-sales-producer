-- Send status per (sale_date, revision). The only table the producer owns in the branch database.
-- A revision is pending while daily_sales says CONFIRMED and there is no SENT row for it.
create table sync_log (
    sale_date   date          not null,
    revision    integer       not null,
    status      varchar(10)   not null check (status in ('SENT', 'FAILED')),
    attempts    integer       not null,
    last_error  varchar(1000),
    -- eventId of the last message sent for this revision, for tracing it in Kafka and at HQ
    -- (HQ keeps the eventId of the first message it stored for a revision, so after a resend the two can differ)
    event_id    varchar(36),
    sent_at     timestamptz,
    updated_at  timestamptz   not null,
    primary key (sale_date, revision)
);
