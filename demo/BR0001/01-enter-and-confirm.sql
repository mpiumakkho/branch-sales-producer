-- Manager enters 2026-10-01 (DRAFT), then confirms it: revision 1
insert into daily_sales (sale_date, branch_code, status, revision) values ('2026-10-01', 'BR0001', 'DRAFT', 0);
insert into daily_sales_line values
    ('2026-10-01', 'BEV', 18200.00, 410),
    ('2026-10-01', 'SNK', 12050.00, 395),
    ('2026-10-01', 'RTE',  9120.50, 152),
    ('2026-10-01', 'HH',   2500.00,  37);
update daily_sales set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where sale_date = '2026-10-01';
