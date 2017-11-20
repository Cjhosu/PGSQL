CREATE OR REPLACE FUNCTION fn_archive_table_data(tablename text, archive_mode text)
RETURNS void AS $$
DECLARE
  row_num BIGINT;
  batch_num INTEGER;
BEGIN
  IF $2 = 'date'
    THEN
    BEGIN
      CREATE TABLE IF NOT EXISTS arch_record_logging(
        id BIGSERIAL PRIMARY KEY,
        source_id BIGINT,
        source_table CHARACTER VARYING(62),
        batch_id INTEGER
        );
      CREATE TABLE IF NOT EXISTS arch_batch_logging(
        id SERIAL PRIMARY KEY,
        archived_started_at TIMESTAMPTZ,
        archived_finished_at TIMESTAMPTZ
        );
      CREATE TABLE IF NOT EXISTS arch_current_batch(
        id BIGINT PRIMARY KEY,
        batch_id INTEGER
        );
      row_num := (SELECT 1);
      WHILE row_num > 0
      LOOP
        BEGIN
          INSERT INTO arch_batch_logging (archived_started_at) VALUES (now());
          EXECUTE 'INSERT INTO arch_current_batch
            SELECT id from public.'||$1||'
            WHERE delete_on <= now()
            ORDER BY delete_on
            LIMIT 5000;';
          EXECUTE 'INSERT INTO archive.'||$1||'
            SELECT p.* FROM public.'||$1||' p
            JOIN arch_current_batch acb ON p.id = acb.id;';
          EXECUTE 'DELETE FROM public.'||$1||'
            WHERE id IN (SELECT id FROM arch_current_batch);';
          IF (Select count(*) from arch_current_batch) > 0
            THEN
            EXECUTE 'INSERT INTO arch_record_logging (source_id, source_table, batch_id)
              SELECT id, '''||$1||''', (SELECT max(id) FROM arch_batch_logging) FROM arch_current_batch;';
          END IF;
          TRUNCATE TABLE arch_current_batch;
          EXECUTE 'SELECT count(*) FROM public.'||$1||' WHERE delete_on <= now();' INTO row_num;
          INSERT INTO arch_batch_logging (archived_finished_at) VALUES (now());
        END;
      END LOOP;
    END;
  END IF;
END;
$$
language plpgsql;
