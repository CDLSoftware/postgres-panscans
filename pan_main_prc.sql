-- Auth: Daniel Walker/Lauro Ojeda
-- Desc: Pan scan main procedure
-- Usage: call pan_scan_prc({tables_list}, {schema_name}, {progress_type});
-- Last revision: 2023-04-14 Lauro Ojeda (Loads of bugfixes to make it simpler and stable)

CREATE OR REPLACE PROCEDURE pan_scan_prc(
    param_tables text[] default '{}',
    param_schemas text[] default '{public}',
    progress text default 'all', -- 'tables','hits','all'
    param_verbose text default 'N'
)
AS $$
DECLARE
  store_table varchar(100) := 'pan_results';
  progress_table varchar(100) := 'pan_progress';
  search_re text := '([3-6]\d{3})((-\d{4}){3}|(\s\d{4}){3}|\d{12})';
  query text;
  qry text;
  schemaname text;
  tablename text;
  columnname text;
  pan text;
  columnvalue_original text;
  rowctid tid;
  pk_column varchar(100);
  pk_value varchar(100);
  col_size int;
  classtype varchar(100);
  cnt int := 0;
  verbosity varchar(5) := upper(param_verbose);
BEGIN
  qry := format('INSERT INTO %s (id, createdatetime, tablename, value, row_id) values (default, now(), ''%s'', ''%s'', null)', progress_table, tablename, 'Pan Scan progress');
  EXECUTE qry;
  commit;

  FOR schemaname,tablename IN
              (SELECT table_schema, table_name
              FROM information_schema.tables t
              WHERE (t.table_name=ANY(param_tables) OR param_tables='{}') AND t.table_schema=ANY(param_schemas) AND t.table_type='BASE TABLE' AND t.table_name not in (select p.tablename from pan_progress p where value='COMPLETED') AND t.table_name not in (select table_name from pan_exceptions where column_name='*') and t.table_name not in ('pan_results','pan_progress','pan_exceptions')
              ORDER BY 2)
  LOOP
    -- Inserting on table to indicate the task has begun
    qry := format('INSERT INTO %s (id, createdatetime, tablename, value) values (default, now(), ''%s'', ''%s'')', progress_table, tablename, 'INITIALIZING');
    EXECUTE qry;
    commit;

    query := format('SELECT ctid FROM %I.%I', schemaname, tablename);
    FOR rowctid IN EXECUTE query
    LOOP
      cnt := cnt + 1;
      if (cnt = 50000) then
        --update row with current ctid scan is on
        qry := format('UPDATE %s SET row_id=''%s'', tablename=''%s'' where value=''%s''', progress_table, rowctid, tablename, 'Pan Scan progress');
        EXECUTE qry;
        raise info '%', format('%s -> PAN Still running', to_char(now(), 'YYYY-MM-DD HH24:MI:SS'));
        cnt := 0;
        commit;
      end if;

      -- Looks only on columns larger than 16 digits as PANs has minimum of 16 digits
      FOR columnname IN
                  (SELECT c.column_name
                    FROM information_schema.columns c
                    WHERE c.table_name = tablename
                        AND c.table_schema = schemaname
                        AND c.column_name not in (select p.column_name from pan_exceptions p where p.table_name=c.table_name) and c.column_name not in (select p.column_name from pan_exceptions p where table_name='*')
                        AND ( character_maximum_length >= 16 OR numeric_precision >= 16 OR data_type in ('bigint', 'character', 'char', 'varchar', 'json', 'numeric', 'real', 'text', 'xml') )
                    EXCEPT
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tco
                        JOIN information_schema.key_column_usage kcu ON (kcu.constraint_name = tco.constraint_name
                                                                        AND kcu.constraint_schema = tco.constraint_schema
                                                                        AND kcu.constraint_name = tco.constraint_name )
                    WHERE
                      tco.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
                      AND kcu.table_schema = schemaname
                      AND kcu.table_name = tablename
                    ORDER BY 1
                   )
      LOOP
	    query := format('SELECT regexp_replace(%I, ''[^0-9]'', '''', ''g'') FROM %I.%I WHERE length(%I)>=16 AND cast(%I as text) ~ %L AND ctid=%L', columnname, schemaname, tablename, columnname, columnname, search_re, rowctid);
        EXECUTE query INTO pan;

        col_size := char_length(pan)::int;
        IF (col_size = 16) THEN
            pk_value := '';
            pk_column := '';
            classtype := 'POSSIBLE POSITIVE';

            -- Getting the PK value to add on the results table
            query := format('SELECT
                              CASE
                              WHEN cnt.cnt_col = 0 THEN ''no-pk''
                              ELSE (
                                SELECT kcu.column_name
                                FROM information_schema.table_constraints tco
                                  JOIN information_schema.key_column_usage kcu ON (kcu.constraint_name = tco.constraint_name AND kcu.constraint_schema = tco.constraint_schema AND kcu.constraint_name = tco.constraint_name )
                                WHERE
                                    tco.constraint_type = ''PRIMARY KEY'' AND kcu.table_schema = ''%s'' AND kcu.table_name = ''%s''
                                ORDER BY kcu.position_in_unique_constraint
                                LIMIT 1
                              ) END AS column_pk
                            FROM (
                              SELECT COUNT(*) cnt_col
                              FROM information_schema.table_constraints
                              WHERE constraint_type = ''PRIMARY KEY'' AND constraint_schema = ''%s'' AND table_name = ''%s''
                            ) AS cnt', schemaname, tablename, schemaname, tablename);
            EXECUTE query INTO pk_column;

            IF (pk_column = 'no-pk') THEN
                pk_value := 'no-pk';
            ELSE
                query := format('SELECT %s FROM %s.%s WHERE ctid=''%s''', pk_column, schemaname, tablename, rowctid);
                EXECUTE query INTO pk_value;
            END IF;

            -- Limiting the size of the the row to be inserted on the table
            -- The quoting removal below looks weird, but works
            query := format('SELECT replace(left(%s,30), E''\'''', '''') FROM %s.%s WHERE ctid=''%s''', columnname, schemaname, tablename, rowctid);
            EXECUTE query INTO columnvalue_original;

            query := format('SELECT %s FROM %s.%s WHERE ctid=''''%s'''';', columnname, schemaname, tablename, rowctid);
            --raise info '%s', query;
            qry := format('INSERT INTO %s (id, createdatetime, schemaname, tablename, value, pan, row_id, classtype, columnname, pk_column, pk_value, finding_sql) values (default, ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'')', store_table, now(), schemaname, tablename, columnvalue_original, pan, rowctid, classtype, columnname, pk_column, pk_value, query);
            EXECUTE qry;

            IF (verbosity = 'Y' OR verbosity = 'YES') THEN
                raise notice '%', format('Table/column: %s.%s(%s) | Class: %s | row-ctid: %s | Value: %s(%I chr) | Query: %s', schemaname, tablename, columnname, classtype, rowctid, pan, col_size, query);
            END IF;
        ELSE
            -- FALSE POSITIVE as over then 16 digits
            NULL;
        END IF;
      END LOOP; -- for columnname
    END LOOP; -- for rowctid
    qry := format('INSERT INTO %s (id, createdatetime, tablename, value, row_id) values (default, now(), ''%s'', ''%s'', null)', progress_table, tablename, 'COMPLETED');
    EXECUTE qry;
  END LOOP; -- for table

  COMMIT;
END;
$$ language plpgsql;