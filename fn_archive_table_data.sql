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
,batch_id INTEGER
);

CREATE TABLE IF NOT EXISTS arch_batch_logging(
id SERIAL PRIMARY KEY
,archived_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS arch_current_batch(
id BIGINT PRIMARY KEY
,batch_id INTEGER
);

row_num := (SELECT 1);

WHILE row_num  > 0
LOOP
BEGIN

INSERT INTO arch_batch_logging (archived_at) VALUES (now());

EXECUTE 'INSERT INTO arch_current_batch
SELECT id from public.'||$1||' where delete_on <= now()
 ORDER BY delete_on
 LIMIT 1000;';

EXECUTE 'INSERT INTO archive.'||$1|| '
SELECT t.* From public.'||$1||' t JOIN arch_current_batch acb ON t.id = acb.id;';

EXECUTE 'DELETE FROM public.'||$1|| '
Where id in (Select id from  arch_current_batch);';

IF (Select count(*) from arch_current_batch) > 0
THEN
EXECUTE 'INSERT INTO arch_record_logging (source_id, source_table, batch_id)
SELECT (Select id from arch_current_batch),'''||$1||''',(Select max(id) from arch_batch_logging);';
END IF;

TRUNCATE TABLE arch_current_batch;

EXECUTE 'Select count(*) from public.'||$1||' WHERE delete_on <= now();' INTO row_num;

END;
END LOOP;
END;
END IF;
END;

$$
language plpgsql;
