CREATE OR REPLACE FUNCTION fn_find_and_archive()
  RETURNS VOID AS $$
  DECLARE
    archive_table TEXT;
    rec_id INTEGER;
    sql TEXT;
  BEGIN
  -- Create a table that holds the things we want to delete
  DROP TABLE IF EXISTS archive.all_tables;
  CREATE TABLE archive.all_tables(
    id SERIAL PRIMARY KEY,
    tablename TEXT
  );
  -- seed it
  INSERT INTO archive.all_tables (tablename)
    SELECT table_name
    FROM information_schema.columns
    WHERE column_name = 'archive_after'
      AND table_schema = 'public';
  -- create a table that contains the fk relationships of anything that should be deleted
  DROP TABLE IF EXISTS archive.fk_constraints;
  CREATE TABLE archive.fk_constraints(
    id SERIAL PRIMARY KEY,
    to_archive text,
    remove_first text
  );
  -- seed it
  INSERT INTO archive.fk_constraints (to_archive, remove_first)
    SELECT ccu.table_name AS foreign_table_name, tc.table_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
    WHERE constraint_type = 'FOREIGN KEY'
      AND ccu.table_name IN (
        SELECT tablename
        FROM archive.all_tables
      );
  -- Create a table that holds the things we can delete now because they are not referenced  by a fk
  DROP TABLE IF EXISTS archive.can_delete;
  CREATE TABLE archive.can_delete(
    id SERIAL PRIMARY KEY,
    tablename TEXT,
    is_done BOOLEAN DEFAULT FALSE
  );
  -- seed it
  WITH init_fk AS (
    SELECT tc.constraint_name,
      ccu.table_name AS foreign_table_name,
      ccu.column_name AS foreign_column_name,
      tc.table_name,
      kcu.column_name
    FROM  information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
    WHERE constraint_type = 'FOREIGN KEY'
  )
  INSERT INTO archive.can_delete (tablename)
    SELECT t.tablename
    FROM archive.all_tables t
    LEFT JOIN init_fk fk
      ON t.tablename = fk.foreign_table_name
    WHERE fk.foreign_table_name IS NULL;
  -- Get the tables we can delte from pass them to the archiving function

IF (SELECT COUNT(*) 
      FROM (SELECT DISTINCT
                   remove_first
              FROM archive.fk_constraints fk
              LEFT JOIN archive.all_tables a
                ON fk.remove_first = a.tablename
             WHERE a.tablename is null) as T) > 0
THEN RAISE NOTICE 'One of the tables you are attempting to archive has a constraint that is not being archived';
RETURN;
END IF;
  -- This block is error handling for when a parent is being archived but one of it's children is not

  BEGIN
    WHILE (SELECT COUNT(*) FROM archive.all_tables) > 0
    LOOP
      BEGIN
      WHILE (SELECT count(*) FROM archive.can_delete WHERE is_done='f') > 0
      LOOP
        BEGIN
        rec_id := (SELECT min(id) FROM archive.can_delete c WHERE is_done = 'f');
        archive_table := (SELECT c.tablename FROM archive.can_delete c WHERE id = rec_id);
        EXECUTE 'SELECT fn_archive_data('''||archive_table||''');';
         -- Mark the record we just did
        UPDATE archive.can_delete SET is_done = 't' WHERE id = rec_id;
        DELETE FROM archive.fk_constraints WHERE remove_first = archive_table;
        DELETE FROM archive.all_tables WHERE tablename = archive_table;
        END;
      END LOOP;
      IF (SELECT count(*) FROM archive.all_tables) > 0
        THEN
        INSERT INTO archive.can_delete (tablename, is_done)
          SELECT (SELECT a.tablename
            FROM archive.all_tables a
            LEFT JOIN archive.fk_constraints fk
              ON a.tablename = fk.to_archive
            LEFT JOIN archive.can_delete d
              ON a.tablename = d.tablename
            WHERE fk.to_archive IS NULL
              AND d.tablename IS NULL), 'f';
      END IF;
      END;
    END LOOP;
  END;
END;
$$ LANGUAGE plpgsql;
