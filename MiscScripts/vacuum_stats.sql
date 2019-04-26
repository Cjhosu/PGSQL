SELECT relname, n_dead_tup, last_autovacuum
  FROM pg_stat_all_tables
 WHERE schemaname = 'public'
 ORDER BY n_dead_tup DESC;
