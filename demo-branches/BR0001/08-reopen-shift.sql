-- The supervisor reopens POS01 shift 1 of 2026-10-01 (back to OPEN) and recounts the drawer: the 20.00 was there.
-- Closing it again sends revision 2 of the same shift (same shiftNo); HQ answers UPDATED.
update pos_shift set status = 'OPEN' where business_date = '2026-10-01' and terminal_id = 'POS01' and shift_no = 1;
update pos_shift set status = 'CLOSED', revision = revision + 1, closed_at = now(), cash_counted = 9120.00
 where business_date = '2026-10-01' and terminal_id = 'POS01' and shift_no = 1;
