-- Three POS shifts of 2026-10-01: POS01 shifts 1 and 2, POS02 shift 1. The POS opens each shift (OPEN), records the
-- tenders, and closes it with the Z-report (CLOSED, revision 1, closing time, counted cash). Only CLOSED shifts are sent.
insert into pos_shift (business_date, terminal_id, shift_no, branch_code, cashier_id, status, revision, opened_at)
values ('2026-10-01', 'POS01', 1, 'BR0001', 'C101', 'OPEN', 0, '2026-10-01 07:00:00+07'),
       ('2026-10-01', 'POS01', 2, 'BR0001', 'C102', 'OPEN', 0, '2026-10-01 15:05:00+07'),
       ('2026-10-01', 'POS02', 1, 'BR0001', 'C103', 'OPEN', 0, '2026-10-01 07:00:00+07');
insert into pos_shift_tender values
    ('2026-10-01', 'POS01', 1, 'CSH', 9120.00, 140),
    ('2026-10-01', 'POS01', 1, 'CRD', 6230.00,  48),
    ('2026-10-01', 'POS01', 1, 'QR',  3100.00,  24),
    ('2026-10-01', 'POS01', 2, 'CSH', 7400.00, 118),
    ('2026-10-01', 'POS01', 2, 'CRD', 5120.00,  41),
    ('2026-10-01', 'POS01', 2, 'DBT',  980.00,   9),
    ('2026-10-01', 'POS02', 1, 'CSH', 4300.00,  96),
    ('2026-10-01', 'POS02', 1, 'QR',  2650.00,  31);
-- POS01 shift 1: 20.00 short in the drawer (cash expected 9120.00, counted 9100.00)
update pos_shift set status = 'CLOSED', revision = revision + 1, closed_at = now(), transaction_count = 212,
                     cash_expected = 9120.00, cash_counted = 9100.00
 where business_date = '2026-10-01' and terminal_id = 'POS01' and shift_no = 1;
update pos_shift set status = 'CLOSED', revision = revision + 1, closed_at = now(), transaction_count = 168,
                     cash_expected = 7400.00, cash_counted = 7400.00
 where business_date = '2026-10-01' and terminal_id = 'POS01' and shift_no = 2;
update pos_shift set status = 'CLOSED', revision = revision + 1, closed_at = now(), transaction_count = 127,
                     cash_expected = 4300.00, cash_counted = 4310.00
 where business_date = '2026-10-01' and terminal_id = 'POS02' and shift_no = 1;
