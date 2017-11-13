CREATE OR REPLACE FUNCTION find_and_archive()
RETURNS VOID AS $$

DECLARE
        archive_table TEXT;
        rec_id INTEGER;
        sql TEXT;
BEGIN

-- Create a table that holds the things we want to delete

DROP TABLE IF EXISTS arch_all_tables;

CREATE TABLE arch_all_tables(
id        SERIAL PRIMARY KEY
,tablename TEXT
);

-- seed it
INSERT INTO arch_all_tables (tablename)
SELECT table_name
  FROM information_schema.columns
 WHERE column_name = 'delete_on' and table_schema = 'public';

-- create a table that contains the fk relationships of anything that should be deleted

DROP TABLE IF EXISTS arch_fk_constraints;

CREATE TABLE arch_fk_constraints(
id SERIAL PRIMARY KEY
,to_archive text
,remove_first text
);

-- seed it

INSERT INTO arch_fk_constraints (to_archive, remove_first)
SELECT ccu.table_name AS foreign_table_name
       ,tc.table_name
FROM
    information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY' and ccu.table_name in (
SELECT tablename
  FROM arch_all_tables
);

-- Create a table that holds the things we can delete now because they are not referenced  by a fk

DROP TABLE IF EXISTS arch_can_delete;

CREATE TABLE arch_can_delete(
id        SERIAL PRIMARY KEY
,tablename TEXT
,is_done BOOLEAN DEFAULT FALSE
);

-- seed it

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

INSERT INTO arch_can_delete (tablename)
SELECT t.tablename
  FROM arch_all_tables t
  LEFT JOIN init_fk fk
    ON t.tablename = fk.foreign_table_name
 WHERE fk.foreign_table_name is null and t.tablename not like 'arch%';

-- Get the tables we can delte from pass them to the archiving function
BEGIN
WHILE (SELECT COUNT(*) FROM arch_all_tables) > 0
LOOP
BEGIN

WHILE (Select count(*) FROM arch_can_delete WHERE is_done='f') > 0
LOOP
BEGIN

rec_id := (Select min(id) FROM arch_can_delete c WHERE is_done = 'f');
archive_table := (SELECT c.tablename from arch_can_delete c where id = rec_id);

EXECUTE 'SELECT fn_archive_table_data('''||archive_table||''',''date'');';

-- Mark the record we just did
UPDATE arch_can_delete SET is_done = 't' WHERE id = rec_id;

DELETE FROM arch_fk_constraints WHERE remove_first = archive_table;
DELETE FROM arch_all_tables WHERE tablename = archive_table;

END;
END LOOP;

IF (Select count(*) from arch_all_tables) >0
THEN

INSERT INTO arch_can_delete (tablename, is_done)
SELECT (SELECT a.tablename
FROM arch_all_tables a
LEFT JOIN arch_fk_constraints fk
ON a.tablename = fk.to_archive
LEFT JOIN arch_can_delete d
ON a.tablename = d.tablename
WHERE fk.to_archive is null and d.tablename is null), 'f';

END IF;

END;
END LOOP;

END;
END;

$$ LANGUAGE plpgsql;
