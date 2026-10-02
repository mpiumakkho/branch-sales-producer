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
