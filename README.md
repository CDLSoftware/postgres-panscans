**Description**
For compliance agreements for Data Protection Act and PCI-DSS one must not store any valid Permanent Account Numbers (PANs) or 16 digit card numbers within their data stores.

The provided set of scripts were created to find credit card numbers within a Postgres database. It will scan all tables in all columns within the configured schema/database and will output the exact rows which were found.

**Usage**
1) Configure the file pan_settings.conf with details of the database credentials, logfile location email address
2) Amend the file pan_configuration.sql in the section "SKIP SCANNING", for skipping scanning of tables and columns you are sure will never store PANs (such as primary key columns, date columns, etc)
3) Execute from shell (tested in Centos 7 and MacOS):
nohup sh pan_main.sh &

Once initiated the a set of 3 tables will be created to store the details required by this tool to function. Then main pan procedure is called (pan_main_prc.sql) which contains the regex function used to detect the 16 digit card numbers. More information regarding the regex function further down. 
When the tool has completed scanning, it will output a CSV file with the PAN rows found (this file will be located at LOGDIR, inside the pan_settings.conf). If an email is provided and the server is able to send emails, a message will be sent.

**Further details**
pan_results - All potential results are stored here then generated into a select statement and put into a CSV once the scan is complete.
pan_progress - Stores tables scanned and completed. Table is also used if PAN needs to be restarted. Procedure will skip any tables that have been marked as completed.  
pan_exceptions - The configuration file stores any tables/columns we don't want to scan in the form on an insert statement. Which are inserted into the exceptions table and excluded from the scan.  

**Example of tables and column names to skip**
> Please ensure to add the exception list at the end of file pan_configuration.sql BEFORE calling pan_main.sh <

-- List of tables and columns that don't require scanning.
-- table_name | column_name
-- -----------+-----------------
-- table      |    *       = Excludes table from scan.
-- table      |  column    = Excludes a specific column from a specific table
--  *         |  column    = Excludes column name from scan

-- Tables to skip totally
insert into pan_exceptions (table_name, column_name) values ('table_name1', '*');
insert into pan_exceptions (table_name, column_name) values ('table_name2', '*');
insert into pan_exceptions (table_name, column_name) values ('table_name3', '*');

-- Column names to be skipped on any table
insert into pan_exceptions (table_name, column_name) values ('*', 'column0');
insert into pan_exceptions (table_name, column_name) values ('table_name4', 'column_readonly');
