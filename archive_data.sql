CREATE OR REPLACE FUNCTION find_and_archive()
RETURNS VOID AS $$

DECLARE
        archive_table TEXT;
        rec_id INTEGER;
        sql TEXT;
BEGIN
-- Create a table that holds the things we can delete

DROP TABLE IF EXISTS arch_can_delete;

CREATE TABLE arch_can_delete(
id        SERIAL PRIMARY KEY
,tablename TEXT
,is_done BOOLEAN DEFAULT FALSE
);

--id the tables that are not refrenced by a fk (it is safe to delete from these now)

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

-- Get the tables we can delte from pass them to the archiving function
BEGIN
WHILE (Select count(*) FROM arch_can_delete WHERE is_done='f') > 0
LOOP
BEGIN
rec_id := (Select min(id) FROM arch_can_delete c WHERE is_done = 'f');
archive_table := (SELECT c.tablename from arch_can_delete c where id = rec_id);

EXECUTE 'SELECT fn_archive_table_data('''||archive_table||''',''date'');';

-- Mark the record we just did
UPDATE arch_can_delete SET is_done = 't' WHERE id = rec_id;
END;
END LOOP;
END;

END;

$$

LANGUAGE plpgsql;
