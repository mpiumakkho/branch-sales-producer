-- POS01 shift 1 of 2026-10-04 is closed before the manager confirms the day's sales (not entered at all here).
-- HQ stores the shift anyway: a shift close has no parent rule, so no PARENT_MISSING (see hq-reconciliation.sql in the
-- consumer repo's demo folder for the difference between shifts and the day's sales).
insert into pos_shift (business_date, terminal_id, shift_no, branch_code, cashier_id, status, revision, opened_at)
values ('2026-10-04', 'POS01', 1, 'BR0002', 'K07', 'OPEN', 0, '2026-10-04 06:30:00+07');
insert into pos_shift_tender values
    ('2026-10-04', 'POS01', 1, 'T1', 2100.00, 55),
    ('2026-10-04', 'POS01', 1, 'T2', 1380.00, 12),
    ('2026-10-04', 'POS01', 1, 'T3',  640.00,  9);
update pos_shift set status = 'CLOSED', revision = revision + 1, closed_at = now(), transaction_count = 76,
                     cash_expected = 2100.00, cash_counted = 2100.00
 where business_date = '2026-10-04' and terminal_id = 'POS01' and shift_no = 1;
