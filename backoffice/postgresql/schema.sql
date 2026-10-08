-- Simulated tables of the branch back-office system (PostgreSQL variant).
-- In a real branch these tables already exist and belong to the back-office system.
-- The producer only reads them; it never writes to the branch database. Its send state is in the branch MongoDB.

-- One row per business day. The manager enters lines (DRAFT), then confirms (CONFIRMED, revision + 1).
-- Editing after confirmation sets the status back to DRAFT; confirming again increases the revision.
create table daily_sales (
    sale_date    date        primary key,
    branch_code  varchar(10) not null,
    status       varchar(10) not null check (status in ('DRAFT', 'CONFIRMED')),
    revision     integer     not null default 0,
    confirmed_at timestamptz
);

-- category_code is the back-office's own code; the producer maps it to the HQ code through configuration.
create table daily_sales_line (
    sale_date     date          not null references daily_sales (sale_date),
    category_code varchar(20)   not null,
    amount        numeric(12,2) not null,
    quantity      integer       not null,
    primary key (sale_date, category_code)
);

-- Returns and voids of one business day, entered and confirmed like the sales. HQ stores them after that day's sales.
create table daily_return (
    return_date  date        primary key,
    branch_code  varchar(10) not null,
    status       varchar(10) not null check (status in ('DRAFT', 'CONFIRMED')),
    revision     integer     not null default 0,
    confirmed_at timestamptz
);

create table daily_return_line (
    return_date   date          not null references daily_return (return_date),
    category_code varchar(20)   not null,
    amount        numeric(12,2) not null,
    quantity      integer       not null,
    primary key (return_date, category_code)
);

-- One row per POS terminal shift. The POS opens it (OPEN) and closes it with the Z-report (CLOSED, revision + 1).
-- Reopening sets OPEN again; closing again increases the revision. Only CLOSED shifts are sent.
-- business_date is the business day the shift was opened under: a night shift keeps it after midnight.
create table pos_shift (
    business_date     date          not null,
    terminal_id       varchar(20)   not null,
    shift_no          integer       not null,
    branch_code       varchar(10)   not null,
    cashier_id        varchar(30),
    status            varchar(10)   not null check (status in ('OPEN', 'CLOSED')),
    revision          integer       not null default 0,
    opened_at         timestamptz   not null,
    closed_at         timestamptz,
    transaction_count integer       not null default 0,
    cash_expected     numeric(12,2),
    cash_counted      numeric(12,2),
    primary key (business_date, terminal_id, shift_no)
);

-- tender_code is the POS's own code; the producer maps it to the HQ tender type through configuration.
create table pos_shift_tender (
    business_date date          not null,
    terminal_id   varchar(20)   not null,
    shift_no      integer       not null,
    tender_code   varchar(20)   not null,
    amount        numeric(12,2) not null,
    quantity      integer       not null,
    primary key (business_date, terminal_id, shift_no, tender_code),
    foreign key (business_date, terminal_id, shift_no) references pos_shift (business_date, terminal_id, shift_no)
);
