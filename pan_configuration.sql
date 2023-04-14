-- Auth: Daniel Walker/Lauro Ojeda
-- Desc: Creates the configuration tables
-- Usage: \i pan_configuration.sql

CREATE TABLE IF NOT EXISTS pan_progress (
    id serial NOT NULL primary key,
    createdatetime timestamp without time zone,
    tablename character varying(100),
    value character varying(100),
    row_id character varying(100)
);

CREATE TABLE IF NOT EXISTS pan_results (
    id serial NOT NULL primary key,
    createdatetime timestamp without time zone NOT NULL,
    schemaname character varying(100) NOT NULL,
    tablename character varying(100) NOT NULL,
    value text NOT NULL,
    pan text,
    row_id tid,
    classtype character varying(100),
    columnname character varying(100),
    pk_column varchar(100),
    pk_value varchar(100),
    finding_sql text
);

CREATE TABLE IF NOT EXISTS pan_exceptions (
    table_name character varying(100) NOT NULL,
    column_name character varying(100) NOT NULL
);

truncate pan_results restart identity;
truncate pan_progress restart identity;
truncate pan_exceptions;

-- SKIP SCANNING
-- List of tables and columns that don't require scanning.
-- table_name | column_name
-- -----------+-----------------
-- table      |    *       = Excludes table from scan.
-- table      |  column    = Excludes a specific column from a specific table
--  *         |  column    = Excludes column name from scan

-- Tables to skip totally
--insert into pan_exceptions (table_name, column_name) values ('table_name1', '*');
--insert into pan_exceptions (table_name, column_name) values ('table_name2', '*');
--insert into pan_exceptions (table_name, column_name) values ('table_name3', '*');

-- Column names to be skipped on any table
--insert into pan_exceptions (table_name, column_name) values ('*', 'column0');
--insert into pan_exceptions (table_name, column_name) values ('table_name4', 'column_readonly');

insert into pan_exceptions (table_name, column_name) values ('pan_results', '*');
insert into pan_exceptions (table_name, column_name) values ('pan_progress', '*');
insert into pan_exceptions (table_name, column_name) values ('pan_exceptions', '*');
