-- Manager enters and confirms 2026-10-03
insert into daily_sales (sale_date, branch_code, status, revision) values ('2026-10-03', 'BR0002', 'DRAFT', 0);
insert into daily_sales_line values
    ('2026-10-03', 'C01', 3100.00, 78),
    ('2026-10-03', 'C03', 2050.00, 92);
update daily_sales set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where sale_date = '2026-10-03';
