CREATE OR REPLACE FUNCTION fn_archive_table_data(tablename text)
RETURNS void AS $$

  row_num BIGINT;
    BEGIN
      CREATE TABLE IF NOT EXISTS archive.record_logging(
        id BIGSERIAL PRIMARY KEY,
        source_id BIGINT,
        source_table CHARACTER VARYING(62),
        batch_id INTEGER
        );
      CREATE TABLE IF NOT EXISTS archive.batch_logging(
        id SERIAL PRIMARY KEY,
        archived_started_at TIMESTAMPTZ,
        archived_finished_at TIMESTAMPTZ
        );
      CREATE TABLE IF NOT EXISTS archive.current_batch(
        id BIGINT PRIMARY KEY,
        batch_id INTEGER
        );
      row_num := (SELECT 1);
      WHILE row_num > 0
      LOOP
        BEGIN
          INSERT INTO archive.batch_logging (archived_started_at) VALUES (now());
          EXECUTE 'INSERT INTO archive.current_batch
            SELECT id from public.'||$1||'
            WHERE archive_after <= now()
            ORDER BY archive_after
            LIMIT 5000;';
          EXECUTE 'INSERT INTO archive.'||$1||'
            SELECT p.* FROM public.'||$1||' p
            JOIN archive.current_batch acb ON p.id = acb.id;';
          EXECUTE 'DELETE FROM public.'||$1||'
            WHERE id IN (SELECT id FROM archive.current_batch);';
          IF (Select count(*) from archive.current_batch) > 0
            THEN
            EXECUTE 'INSERT INTO archive.record_logging (source_id, source_table, batch_id)
              SELECT id, '''||$1||''', (SELECT max(id) FROM archive.batch_logging) FROM archive.current_batch;';
          END IF;
          TRUNCATE TABLE archive.current_batch;
          EXECUTE 'SELECT count(*) FROM public.'||$1||' WHERE archive_after <= now();' INTO row_num;
          INSERT INTO archive.batch_logging (archived_finished_at) VALUES (now());
        END;
      END LOOP;
    END;
  END IF;
END;
$$
language plpgsql;
