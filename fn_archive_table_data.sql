CREATE OR REPLACE FUNCTION fn_archive_table_data(tablename text, archive_mode text)
RETURNS void AS $$

BEGIN
IF $2 = 'date'
THEN
BEGIN

EXECUTE 'INSERT INTO archive.'||$1|| '
SELECT * From public.'||$1||' where delete_on <= now();';

EXECUTE 'DELETE FROM public.'||$1|| '
Where id in (Select id from  archive.'||$1||');';
END;
END IF;
END;

$$
language plpgsql;
