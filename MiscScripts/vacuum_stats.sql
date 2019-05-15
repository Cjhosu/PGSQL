SELECT sat.relname
       ,cla.relname AS is_toast_for
       ,n_dead_tup
       ,last_autovacuum
  FROM pg_stat_all_tables sat
  LEFT JOIN pg_class cla
    ON sat.relid = cla.reltoastrelid
 WHERE schemaname in ('public', 'pg_toast')
   AND n_dead_tup > 0
 ORDER BY n_dead_tup DESC;
