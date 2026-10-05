-- Manager enters and confirms 2026-10-02, including C99, which has no HQ mapping in demo-branches/BR0002/branch.yaml
insert into daily_sales (sale_date, branch_code, status, revision) values ('2026-10-02', 'BR0002', 'DRAFT', 0);
insert into daily_sales_line values
    ('2026-10-02', 'C01', 3050.00, 76),
    ('2026-10-02', 'C03', 1980.00, 90),
    ('2026-10-02', 'C99',  500.00,  5);
update daily_sales set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where sale_date = '2026-10-02';
