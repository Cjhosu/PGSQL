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



CREATE OR REPLACE FUNCTION find_and_archive()
RETURNS VOID AS $$

DECLARE
        archive_table TEXT;
        sql TEXT;
BEGIN
-- Create a table that holds the things we can delete

DROP TABLE IF EXISTS arch_can_delete;

CREATE TABLE arch_can_delete(
id        SERIAL PRIMARY KEY
,tablename TEXT
);

WITH init_fk AS (
SELECT tc.constraint_name
       ,ccu.table_name AS foreign_table_name
       ,ccu.column_name AS foreign_column_name
       ,tc.table_name
       ,kcu.column_name
  FROM  information_schema.table_constraints AS tc
  JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
  JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
 WHERE constraint_type = 'FOREIGN KEY'
)

, all_tab AS (
SELECT pt.tablename 
  FROM pg_tables pt 
 WHERE schemaname = 'public'
)

INSERT INTO arch_can_delete (tablename)
SELECT t.tablename
  FROM all_tab t
  LEFT JOIN init_fk fk
    ON t.tablename = fk.foreign_table_name
 WHERE fk.foreign_table_name is null and t.tablename not like 'arch%';

archive_table := (SELECT c.tablename from arch_can_delete c where c.tablename='pharmacy_notes');

EXECUTE 'Select fn_archive_table_data('''||archive_table||''',''date'');';
END;

$$

LANGUAGE plpgsql;
