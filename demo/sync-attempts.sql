-- Demo: every send attempt (sync_attempt), oldest first. sync_log only keeps the latest status per revision.
select sale_date, revision,
       to_char(attempted_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS') as attempted_at_bkk,
       result, event_id, error
  from sync_attempt
 order by id;
