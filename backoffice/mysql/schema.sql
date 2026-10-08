-- Simulated tables of the branch back-office system (MySQL variant of backoffice/postgresql/schema.sql).
-- In a real branch these tables already exist and belong to the back-office system; the producer only reads them.
-- confirmed_at is local time (Asia/Bangkok) without a zone, as many back-office systems store it. The producer's JDBC
-- URL sets connectionTimeZone=Asia/Bangkok so that it is read as Bangkok time.
create table daily_sales (
    sale_date    date          primary key,
    branch_code  varchar(10)   not null,
    status       varchar(10)   not null check (status in ('DRAFT', 'CONFIRMED')),
    revision     int           not null default 0,
    confirmed_at datetime(6)
);

create table daily_sales_line (
    sale_date     date          not null,
    category_code varchar(20)   not null,
    amount        decimal(12,2) not null,
    quantity      int           not null,
    primary key (sale_date, category_code),
    foreign key (sale_date) references daily_sales (sale_date)
);

create table daily_return (
    return_date  date          primary key,
    branch_code  varchar(10)   not null,
    status       varchar(10)   not null check (status in ('DRAFT', 'CONFIRMED')),
    revision     int           not null default 0,
    confirmed_at datetime(6)
);

create table daily_return_line (
    return_date   date          not null,
    category_code varchar(20)   not null,
    amount        decimal(12,2) not null,
    quantity      int           not null,
    primary key (return_date, category_code),
    foreign key (return_date) references daily_return (return_date)
);

-- opened_at and closed_at are local time (Asia/Bangkok) without a zone, like confirmed_at.
create table pos_shift (
    business_date     date          not null,
    terminal_id       varchar(20)   not null,
    shift_no          int           not null,
    branch_code       varchar(10)   not null,
    cashier_id        varchar(30),
    status            varchar(10)   not null check (status in ('OPEN', 'CLOSED')),
    revision          int           not null default 0,
    opened_at         datetime(6)   not null,
    closed_at         datetime(6),
    transaction_count int           not null default 0,
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
