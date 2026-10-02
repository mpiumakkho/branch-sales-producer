-- Manager enters and confirms 2026-10-01. C01 and C02 both map to BEVERAGE (see demo/BR0002/branch.yaml).
insert into daily_sales (sale_date, branch_code, status, revision) values ('2026-10-01', 'BR0002', 'DRAFT', 0);
insert into daily_sales_line values
    ('2026-10-01', 'C01', 3200.00, 80),
    ('2026-10-01', 'C02', 1450.00, 52),
    ('2026-10-01', 'C03', 2100.00, 95),
    ('2026-10-01', 'C05',  980.00, 30);
update daily_sales set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where sale_date = '2026-10-01';
