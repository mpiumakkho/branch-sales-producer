-- Simulated tables of the branch back-office system (SQL Server variant of backoffice/postgresql/schema.sql).
-- In a real branch these tables already exist and belong to the back-office system; the producer only reads them.

-- The producer reads with SNAPSHOT isolation, so that it never blocks or deadlocks the back-office's writes.
-- A DBA enables it once per database; it does not change how other transactions behave. (Already on in master.)
-- One statement (no ";" inside), because the variable must be in the same batch as its use.
declare @enable_snapshot nvarchar(300) = N'alter database ' + quotename(db_name()) + N' set allow_snapshot_isolation on'
if (select snapshot_isolation_state from sys.databases where name = db_name()) = 0
    exec (@enable_snapshot);

create table daily_sales (
    sale_date    date              primary key,
    branch_code  varchar(10)       not null,
    status       varchar(10)       not null check (status in ('DRAFT', 'CONFIRMED')),
    revision     int               not null default 0,
    confirmed_at datetimeoffset(6)
);

create table daily_sales_line (
    sale_date     date          not null references daily_sales (sale_date),
    category_code varchar(20)   not null,
    amount        decimal(12,2) not null,
    quantity      int           not null,
    primary key (sale_date, category_code)
);

create table daily_return (
    return_date  date              primary key,
    branch_code  varchar(10)       not null,
    status       varchar(10)       not null check (status in ('DRAFT', 'CONFIRMED')),
    revision     int               not null default 0,
    confirmed_at datetimeoffset(6)
);

create table daily_return_line (
    return_date   date          not null references daily_return (return_date),
    category_code varchar(20)   not null,
    amount        decimal(12,2) not null,
    quantity      int           not null,
    primary key (return_date, category_code)
);

create table pos_shift (
    business_date     date              not null,
    terminal_id       varchar(20)       not null,
    shift_no          int               not null,
    branch_code       varchar(10)       not null,
    cashier_id        varchar(30),
    status            varchar(10)       not null check (status in ('OPEN', 'CLOSED')),
    revision          int               not null default 0,
    opened_at         datetimeoffset(6) not null,
    closed_at         datetimeoffset(6),
    transaction_count int               not null default 0,
    cash_expected     decimal(12,2),
    cash_counted      decimal(12,2),
    primary key (business_date, terminal_id, shift_no)
);

create table pos_shift_tender (
    business_date date          not null,
    terminal_id   varchar(20)   not null,
    shift_no      int           not null,
    tender_code   varchar(20)   not null,
    amount        decimal(12,2) not null,
    quantity      int           not null,
    primary key (business_date, terminal_id, shift_no, tender_code),
    foreign key (business_date, terminal_id, shift_no) references pos_shift (business_date, terminal_id, shift_no)
);
