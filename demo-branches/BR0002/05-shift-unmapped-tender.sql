-- POS01 shift 2 of 2026-10-04 took gift vouchers (GV), which have no HQ mapping in demo-branches/BR0002/branch.yaml.
-- The producer does not send the shift: FAILED at the branch with "no HQ tender mapping for local tender [GV]".
insert into pos_shift (business_date, terminal_id, shift_no, branch_code, cashier_id, status, revision, opened_at)
values ('2026-10-04', 'POS01', 2, 'BR0002', 'K08', 'OPEN', 0, '2026-10-04 14:30:00+07');
insert into pos_shift_tender values
    ('2026-10-04', 'POS01', 2, 'T1', 1850.00, 47),
    ('2026-10-04', 'POS01', 2, 'GV',  300.00,  3);
update pos_shift set status = 'CLOSED', revision = revision + 1, closed_at = now(), transaction_count = 50,
                     cash_expected = 1850.00, cash_counted = 1850.00
 where business_date = '2026-10-04' and terminal_id = 'POS01' and shift_no = 2;
