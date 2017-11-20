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
id SERIAL PRIMARY KEY
,source_id INTEGER
,source_table CHARACTER VARYING(62)
);

CREATE TABLE IF NOT EXISTS arch_batch_logging(
id SERIAL PRIMARY KEY
,archived_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS arch_current_batch(
id BIGINT PRIMARY KEY
,batch_id INTEGER
);

WHILE (Select COUNT(*) FROM public.tablename WHERE delete_on <= now()) > 0
LOOP
BEGIN

EXECUTE 'INSERT INTO arch_current_batch
SELECT id from public.'||$1||' where delete_on <= now(()
 ORDER BY delete_on
 LIMIT 1000;';

EXECUTE 'INSERT INTO archive.'||$1|| '
SELECT * From public.'||$1||' t JOIN arch_current_batch acb ON t.id = acb.id;';

EXECUTE 'DELETE FROM public.'||$1|| '
Where id in (Select id from  arch_current_batch);';

TRUNCATE TABLE arch_current_batch;
END;
END LOOP;
END;
END IF;
END;

$$
language plpgsql;
