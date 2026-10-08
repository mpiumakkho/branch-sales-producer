-- Demo: each day in the back-office, sales and returns (send state: demo-branches/sync-state.sh)
select 'SALES' as record, sale_date as business_date, status, revision,
       to_char(confirmed_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS') as confirmed_at_bkk
  from daily_sales
union all
select 'RETURN', return_date, status, revision,
       to_char(confirmed_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS')
  from daily_return
 order by 2, 1;
