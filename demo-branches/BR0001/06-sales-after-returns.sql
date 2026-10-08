-- The sales of 2026-10-05 are confirmed after its returns (05-returns.sql): HQ stores them and replays the waiting
-- returns by itself
insert into daily_sales (sale_date, branch_code, status, revision) values ('2026-10-05', 'BR0001', 'DRAFT', 0);
insert into daily_sales_line values
    ('2026-10-05', 'BEV', 16400.00, 371),
    ('2026-10-05', 'SNK', 10980.00, 352),
    ('2026-10-05', 'RTE',  8210.00, 139);
update daily_sales set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where sale_date = '2026-10-05';
