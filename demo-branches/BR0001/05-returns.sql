-- Returns of two days. 2026-10-01: its sales are at HQ already (01-enter-and-confirm.sql), so HQ stores the returns
-- right away. 2026-10-05: no sales of that day yet, so HQ holds the returns (PARENT_MISSING) until 06-sales-after-returns.sql
insert into daily_return (return_date, branch_code, status, revision) values ('2026-10-01', 'BR0001', 'DRAFT', 0);
insert into daily_return_line values
    ('2026-10-01', 'BEV', 120.00, 3),
    ('2026-10-01', 'HH',  230.00, 2);
update daily_return set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where return_date = '2026-10-01';

insert into daily_return (return_date, branch_code, status, revision) values ('2026-10-05', 'BR0001', 'DRAFT', 0);
insert into daily_return_line values
    ('2026-10-05', 'SNK', 45.00, 1);
update daily_return set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where return_date = '2026-10-05';
