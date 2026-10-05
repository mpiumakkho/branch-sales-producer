-- Manager enters and confirms 2026-10-03, including gift cards (GC -> GIFT_CARD, a category HQ does not have yet)
insert into daily_sales (sale_date, branch_code, status, revision) values ('2026-10-03', 'BR0001', 'DRAFT', 0);
insert into daily_sales_line values
    ('2026-10-03', 'BEV', 17900.00, 402),
    ('2026-10-03', 'SNK', 12200.00, 388),
    ('2026-10-03', 'GC',   3000.00,   6);
update daily_sales set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where sale_date = '2026-10-03';
