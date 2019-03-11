CREATE OR REPLACE FUNCTION fn_varchar_six_audit()
RETURNS TABLE
(db_name varchar(50), table_name varchar(50), column_name varchar(50))
AS
$$
DECLARE cur_db text;
DECLARE sql text;
DECLARE curs CURSOR FOR SELECT dbs.database_name FROM dbs;
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS dbs (database_name varchar(50));
    DELETE FROM dbs;

    INSERT INTO dbs
    SELECT datname 
      FROM pg_database 
     WHERE datname NOT IN ('template1', 'template0' , 'postgres') ;

    DROP TABLE IF EXISTS result_table;
    OPEN curs;
    LOOP
        FETCH curs INTO cur_db;
        EXIT WHEN NOT FOUND;
        CREATE TEMP TABLE IF NOT EXISTS result_table
        (db_name varchar(50), table_name varchar(50), column_name varchar(50));


        sql = 'INSERT INTO result_table
               SELECT * FROM  dblink(''dbname=' || cur_db || ''',''Select table_catalog, table_name, column_name From information_schema.columns where data_type = ''''character varying'''' and (character_maximum_length = 6 or character_maximum_length = 8)'') as T2 (table_catalog varchar(50), table_name varchar(50), column_name varchar(50));';

        EXECUTE sql;

    END LOOP;

    CLOSE curs;

    RETURN QUERY SELECT * FROM result_table;

END;
$$
language plpgsql;
