-- Demo: each day in the back-office and its send status in sync_log
select d.sale_date, d.status, d.revision,
       s.status as sync_status, s.attempts, s.last_error,
       to_char(s.sent_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS') as sent_at_bkk
  from daily_sales d
  left join sync_log s on s.sale_date = d.sale_date and s.revision = d.revision
 order by d.sale_date;
