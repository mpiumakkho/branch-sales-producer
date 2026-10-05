-- Manager enters and confirms 2026-10-02
insert into daily_sales (sale_date, branch_code, status, revision) values ('2026-10-02', 'BR0001', 'DRAFT', 0);
insert into daily_sales_line values
    ('2026-10-02', 'BEV', 17650.00, 398),
    ('2026-10-02', 'SNK', 11900.00, 380),
    ('2026-10-02', 'RTE',  8800.00, 147);
update daily_sales set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where sale_date = '2026-10-02';
