
SELECT relname
       ,seq_scan AS sequence_scan
       ,seq_tup_read AS tuples_read
       ,seq_tup_read / seq_scan AS tuples_read_per_seq_scan
       ,idx_scan AS index_scans
FROM pg_stat_user_tables
WHERE seq_scan > 0
ORDER BY seq_tup_read DESC;
