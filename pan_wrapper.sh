#!/bin/bash
#Auth: Daniel Walker
#pan_wrapper.sh

#variables
export ENDPOINT=$1
export DBNAME=$2
export SCHEMA=$3
export RESTARTING=$4
export MASTERUSER=$5
export MASTERPASS=$6
export VERBOSITY=$7

export PGPASSWORD=$MASTERPASS
export LOG=pan_running_for_${SCHEMA}.log
export DATE=$(date +%Y%m%d%H%M)
> $LOG
#Creating pan scan main package
echo "Stage 1 - Creating main procedure" | tee -a $LOG
psql -h $ENDPOINT --username $MASTERUSER $DBNAME << EOF
\i pan_main_prc.sql
EOF

#check that determines if configuration tables need creating, truncating or pan is restarting
echo "Stage 2 - Pan configuration" | tee -a $LOG
if [ "$RESTARTING" = "N" ];
then
  echo "Restarting was set to NO! If incorrect cancel now as progress table will be wiped. Sleeping for 5 minutes..." | tee -a $LOG
  sleep 300;

  psql -h $ENDPOINT --username $MASTERUSER $DBNAME << EOF
  \i pan_configuration.sql
EOF
else
  echo "PAN Restarting, checking configuration tables exist." | tee -a $LOG
  #check if rquired config tables exits
  PAN_PROGRESS=`psql -h $ENDPOINT -t --username $MASTERUSER $DBNAME -c "select count(*) from information_schema.tables where table_name='pan_progress';"`
  PAN_RESULTS=`psql -h $ENDPOINT -t --username $MASTERUSER $DBNAME -c "select count(*) from information_schema.tables where table_name='pan_results';"`
  PAN_CONFIG=$((PAN_RESULTS+PAN_PROGRESS))

  if [ "$PAN_CONFIG" -lt 2 ];
  then
  echo "##### ERROR - Required configuration tables do NOT exist! Please investigate. #####" | tee -a $LOG
  else
  echo "All neccessary PAN configuration tables exist. PAN will resume from where it was last stopped." | tee -a $LOG
fi
fi



echo "Stage 3 - Starting PAN" | tee -a $LOG
psql -h $ENDPOINT --username $MASTERUSER $DBNAME << EOF
call pan_scan_prc('{}','{$SCHEMA}', 'hits', '${VERBOSITY}');
EOF


echo "Stage 4 - PAN run completed" | tee -a $LOG
#Spool results to a CSV file
CSV="${SCHEMA}_pan_results_${DATE}.csv"
psql -t -h $ENDPOINT --username $MASTERUSER $DBNAME << EOF
\o $CSV
select 'select ' || columnname || ' from ' || schemaname || '.' || tablename || ' where ctid=''' || row_id || ''';' from pan_results;
\o
truncate table pan_results;
EOF
echo "Results spooled to $CSV" | tee -a $LOG
