CREATE OR REPLACE FUNCTION generate_create_table_statement(p_table_name varchar)
  RETURNS text AS
$BODY$
DECLARE
    v_table_ddl   text;
    column_record record;
BEGIN
    FOR column_record IN 
        SELECT 
            b.nspname as schema_name,
            b.relname as table_name,
            a.attname as column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type,
            CASE WHEN 
                (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                 FROM pg_catalog.pg_attrdef d
                 WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) IS NOT NULL THEN
                'DEFAULT '|| (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                              FROM pg_catalog.pg_attrdef d
                              WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
            ELSE
                ''
            END as column_default_value,
            CASE WHEN a.attnotnull = true THEN 
                'NOT NULL'
            ELSE
                'NULL'
            END as column_not_null,
            a.attnum as attnum,
            e.max_attnum as max_attnum
        FROM 
            pg_catalog.pg_attribute a
            INNER JOIN 
             (SELECT c.oid,
                n.nspname,
                c.relname
              FROM pg_catalog.pg_class c
                   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relname ~ ('^('||p_table_name||')$')
                AND pg_catalog.pg_table_is_visible(c.oid)
              ORDER BY 2, 3) b
            ON a.attrelid = b.oid
            INNER JOIN 
             (SELECT 
                  a.attrelid,
                  max(a.attnum) as max_attnum
              FROM pg_catalog.pg_attribute a
              WHERE a.attnum > 0 
                AND NOT a.attisdropped
              GROUP BY a.attrelid) e
            ON a.attrelid=e.attrelid
        WHERE a.attnum > 0 
          AND NOT a.attisdropped
        ORDER BY a.attnum
    LOOP
        IF column_record.attnum = 1 THEN
            v_table_ddl:='CREATE TABLE archive.'||column_record.table_name||' (';
        ELSE
            v_table_ddl:=v_table_ddl||',';
        END IF;

        IF column_record.attnum <= column_record.max_attnum THEN
            v_table_ddl:=v_table_ddl||
                     '  '||column_record.column_name||' '||column_record.column_type||' '||column_record.column_default_value||' '||column_record.column_not_null;
        END IF;
    END LOOP;

    v_table_ddl:=v_table_ddl||');';
    RETURN v_table_ddl;
END;
$BODY$
  LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION script_archive_tables()
RETURNS text AS
$$
DECLARE var text;
        var2 text;
        schema_dif record;
BEGIN
WITH missing_tbl AS(
SELECT p.table_name FROM information_schema.tables p
  LEFT JOIN information_schema.tables a
    ON p.table_name = a.table_name
   AND p.table_schema = 'public'
   AND a.table_schema = 'archive'
 WHERE a.table_name is null  and p.table_schema = 'public' limit 1)
SELECT * from missing_tbl INTO schema_dif;
   var := 'SELECT generate_create_table_statement ('''||schema_dif.table_name ||''');';
BEGIN
 EXECUTE var into var2;
Return var2;
END;
END ;
$$
Language plpgsql;


CREATE OR REPLACE FUNCTION script_archive_columns()
RETURNS text AS
$$
DECLARE var text;
        schema_dif record;
BEGIN
WITH  missing_clm as(
SELECT p.table_name
       ,p.column_name
       , p.data_type
  FROM information_schema.columns p
  LEFT JOIN information_schema.columns a
    ON p.column_name = a.column_name
   AND p.table_name = a.table_name
   AND a.table_schema = 'archive'
   AND p.table_schema = 'public'
 WHERE a.table_schema is null and p.table_schema = 'public' limit 1)
Select * From missing_clm INTO schema_dif;
var := 'ALTER TABLE archive.'||schema_dif.table_name ||' ADD COLUMN '|| schema_dif.column_name ||' ' || schema_dif.data_type ||';';
RETURN var;
END;
$$
Language plpgsql;

CREATE OR REPLACE FUNCTION script_data_type_diffs()
RETURNS TEXT AS
$$
DECLARE var text;
        up_rec record;
BEGIN
WITH type_dif AS(
SELECT p.table_name
       ,p.column_name
       ,p.data_type AS public_type
       ,a.data_type AS archive_type
       ,p.character_maximum_length public_length
       ,a.character_maximum_length archive_length
  FROM information_schema.columns p
  JOIN information_schema.columns a
    ON p.table_name = a.table_name
   AND p.column_name = a.column_name
   AND p.table_schema = 'public'
   AND a.table_schema = 'archive'
 WHERE p.data_type <> a.data_type
    OR (p.data_type = a.data_type
   AND p.character_maximum_length > a.character_maximum_length)
 LIMIT 1)
SELECT * from type_dif INTO up_rec;
IF up_rec.public_type = 'character varying'
THEN
var := 'ALTER TABLE archive.'||up_rec.table_name ||' ALTER COLUMN ' || up_rec.column_name ||' TYPE '|| up_rec.public_type ||'('|| up_rec.public_length ||');';
ELSE
var := 'ALTER TABLE archive.'||up_rec.table_name ||' ALTER COLUMN ' || up_rec.column_name ||' TYPE '|| up_rec.public_type ;
END IF;
RETURN var;
END;
$$
Language plpgsql;

CREATE OR REPLACE FUNCTION create_archive_tables()
RETURNS void AS
$$
DECLARE out_var text;
BEGIN
WHILE 1=1
LOOP
BEGIN
 EXECUTE 'SELECT script_archive_tables();' INTO out_var;
EXECUTE out_var;
EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'All tables created';
RETURN;
END;
END LOOP;
END;
$$
Language plpgsql;

CREATE OR REPLACE FUNCTION create_archive_columns()
RETURNS void AS
$$
DECLARE out_var text;
BEGIN
WHILE 1=1
LOOP
BEGIN
 EXECUTE 'SELECT script_archive_columns();' INTO out_var;
EXECUTE out_var;
EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'All columns added';
RETURN;
END;
END LOOP;
END;
$$
Language plpgsql;

CREATE OR REPLACE FUNCTION run_archive_scripts()
RETURNS void AS
$$
BEGIN
EXECUTE 'SELECT create_archive_tables();';
EXECUTE 'SELECT create_archive_columns();';
END;
$$
Language plpgsql;
