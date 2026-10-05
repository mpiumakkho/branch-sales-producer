-- Demo: each day in the back-office (send state: demo-branches/sync-state.sh)
select sale_date, status, revision,
       to_char(confirmed_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS') as confirmed_at_bkk
  from daily_sales
 order by sale_date;
