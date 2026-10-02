-- Manager finds a mistake in 2026-10-01 after it was sent: edit (back to DRAFT), then confirm again: revision 2
update daily_sales set status = 'DRAFT' where sale_date = '2026-10-01';
update daily_sales_line set amount = 18700.00, quantity = 422 where sale_date = '2026-10-01' and category_code = 'BEV';
update daily_sales set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where sale_date = '2026-10-01';
