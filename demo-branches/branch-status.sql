-- Demo: each record in the back-office: days of sales and returns, POS shifts (send state: demo-branches/sync-state.sh)
select 'SALES' as record, sale_date::text as record_key, status, revision,
       to_char(confirmed_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS') as confirmed_at_bkk
  from daily_sales
union all
select 'RETURN', return_date::text, status, revision,
       to_char(confirmed_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS')
  from daily_return
union all
select 'SHIFT', business_date || ' ' || terminal_id || '#' || shift_no, status, revision,
       to_char(closed_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS')
  from pos_shift
 order by 2, 1;
