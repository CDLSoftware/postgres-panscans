-- Auth: Daniel Walker/Lauro Ojeda
-- Desc: Pan scan main procedure (Recommended to be run as DBADMIN)
-- Usage: call pan_scan_prc({tables_list}, {schema_name}, {progress_type});

CREATE OR REPLACE PROCEDURE pan_scan_prc(
    param_tables text[] default '{}',
    param_schemas text[] default '{public}',
    progress text default null -- 'tables','hits','all'
)
AS $$
DECLARE
  query text;
  qry text;
  search_re text := '([3-6]\d{3})((-\d{4}){3}|(\s\d{4}){3}|\d{12})';
  schemaname text;
  tablename text;
  columnname text;
  columnvalue text;
  rowctid tid;
  pk_column varchar(100);
  pk_value varchar(100);
  stripped_str text;
  char_stripped text;
  value_stripped text;
  col_size int;
  unq_digs int;
  store_table varchar(100) := 'pan_results';
  progress_table varchar(100) := 'pan_progress';
  classtype varchar(100);
  cnt int := 0;
  auto_fix int := 0;

BEGIN
  qry := format('INSERT INTO %s (id, createdatetime, tablename, value, row_id) values (default, now(), ''%s'', ''%s'', null)', progress_table, tablename, 'Pan Scan progress');
  EXECUTE qry;
  commit;

  FOR schemaname,tablename IN
              (SELECT table_schema, table_name
              FROM information_schema.tables t
              WHERE (t.table_name=ANY(param_tables) OR param_tables='{}') AND t.table_schema=ANY(param_schemas) AND t.table_type='BASE TABLE' and (t.table_name not in (select p.tablename from pan_progress p where value='COMPLETED')) and (t.table_name not in (select table_name from pan_exceptions where column_name='*'))
              ORDER BY 2)
  LOOP
    -- Inserting on table to indicate the task has begun
    qry := format('INSERT INTO %s (id, createdatetime, tablename, value) values (default, now(), ''%s'', ''%s'')', progress_table, tablename, 'INITIALIZING');
    EXECUTE qry;
    commit;

    IF (progress in ('tables','all')) THEN
      raise info '%', format('Searching globally in table: %I.%I', schemaname, tablename);
    END IF;

    query := format('SELECT ctid FROM %I.%I AS t WHERE cast(t.* as text) ~ %L', schemaname, tablename, search_re);
    FOR rowctid IN EXECUTE query
    LOOP
      cnt := cnt + 1;
      if (cnt = 50000) then
        --update row with current ctid scan is on
        qry := format('UPDATE %s SET row_id=''%s'', tablename=''%s'' where value=''%s''', progress_table, rowctid, tablename, 'Pan Scan progress');
        EXECUTE qry;
        raise info '%', format('%s -> PAN Still running', now());
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
	    query := format('SELECT %I FROM %I.%I WHERE cast(%I as text) ~ %L AND ctid=%L', columnname, schemaname, tablename, columnname, search_re, rowctid);
        EXECUTE query INTO columnvalue;

        IF (columnvalue IS NOT NULL) THEN
          IF (progress in ('hits', 'all')) THEN
            -- raise info '%', format('Found in %I.%I.%I at ctid %s, value: ''%s''', schemaname, tablename, columnname, rowctid, columnvalue);
            -- strip out chars to filter the numbers better
            col_size := char_length(columnvalue)::int;
            IF (col_size >= 16) THEN
                --stripped_str := replace(replace(replace(columnvalue,' ',''),'-',''),'_','');
                stripped_str :=  regexp_replace(columnvalue, '[ -_]', '', 'g');
                char_stripped := regexp_replace(stripped_str,'[^0-9]+', '', 'g');
                IF (char_length(char_stripped)>16) THEN
                  classtype := 'FALSE POSITIVE';
                  --raise info '%', format('%s -> ctid %s = ORIG. VAL: %s(%I chr)| STRIP VAL: %s(%I chr)', classtype, rowctid, columnvalue, col_size, stripped_str, char_length(char_stripped));
                ELSE
                  -- Adding an extra layer of checking against the POSSIBLE POSITIVE to remove some types of FALSE POSITIVES
                  -- If 4 unique digits or less, then classed as FALSE POSITIVE to avoid the below situations
                  -- 1234-1234-1234-1234, 9999-9999-9999-9999, 4444-5555-6666-7777, 1111-2222-1111-2222, 5555555555554444, 5105105105105100, 4111111111111111
                  query := format('select length(string_agg(ccard,''''))::int unq_digs from (select distinct(regexp_split_to_table(''%s'', '''')) ccard order by ccard ) x', char_stripped);
                  EXECUTE query INTO unq_digs;

                  if (unq_digs <= 4) then
                    classtype := 'FALSE POSITIVE';
                  else
                    -- Clearing values before continuing
                    pk_value := '';
                    pk_column := '';

                    classtype := 'POSSIBLE POSITIVE';
                    --raise notice '%', format('%s -> ctid %s = ORIG. VAL: %s(%I chr)| STRIP VAL: %s(%I chr)', classtype, rowctid, columnvalue, col_size, stripped_str, char_length(char_stripped));

                    -- Getting the PK value to add on the results table
                    query := format('SELECT kcu.column_name
                        FROM information_schema.table_constraints tco
                        JOIN information_schema.key_column_usage kcu ON (kcu.constraint_name = tco.constraint_name
                                                    AND kcu.constraint_schema = tco.constraint_schema
                                                    AND kcu.constraint_name = tco.constraint_name )
                        WHERE
                        tco.constraint_type = ''PRIMARY KEY''
                        AND kcu.table_schema = ''%s''
                        AND kcu.table_name = ''%s''
                        ORDER BY kcu.position_in_unique_constraint
                        limit 1', schemaname, tablename);
                    EXECUTE query INTO pk_column;

                    query := format('SELECT %s FROM %s.%s WHERE ctid=''%s''', pk_column, schemaname, tablename, rowctid);
                    EXECUTE query INTO pk_value;

                    -- Limiting the size of the the row to be inserted on the table
                    columnvalue := left(columnvalue, 50);
                    value_stripped := regexp_replace(columnvalue,'''', '', 'g');

                    qry := format('INSERT INTO %s (id, createdatetime, schemaname, tablename, value, row_id, classtype, columnname, pk_column, pk_value) values (default, ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'')', store_table, now(), schemaname, tablename, value_stripped, rowctid, classtype, columnname, pk_column, pk_value);
                    EXECUTE qry;

                  end if;
                END IF;
            ELSE
                classtype := 'UNKNOWN';
                raise notice '%', format('%s -> ctid %s = ORIG. VAL: %s(%I chr)', classtype, rowctid, columnvalue, col_size);
            END IF;
          END IF;
        END IF;
      END LOOP; -- for columnname
    END LOOP; -- for rowctid
    qry := format('INSERT INTO %s (id, createdatetime, tablename, value, row_id) values (default, now(), ''%s'', ''%s'', null)', progress_table, tablename, 'COMPLETED');
    EXECUTE qry;
  END LOOP; -- for table

  commit;
END;
$$ language plpgsql;