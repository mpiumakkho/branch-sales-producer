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
