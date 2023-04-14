#!/bin/bash

# Auth: Daniel Walker/Lauro Ojeda
# Desc: Deploys and executes the function to search for PANs (
# Usage: ./pan_main.sh

if [ -f pan_settings.conf ]; then
  source pan_settings.conf
else
  echo "$(date) Cannot continue without the config file"
  exit 1
fi

export DATE=$(date +%Y%m%d)
export LOGFILE=${LOGDIR}/pan_running_for_${DATABASE_HOST}_${DATE}.log
export PGPASSWORD=${DATABASE_PASSWORD}

echo "$(date) Logfile for this run - ${LOGFILE}"

if ! [[ "${RESTARTING}" == "Y" || "${RESTARTING}" == "N" ]]; then
    echo "$(date) ERROR - Restarting variable needs to be capital Y/N"
fi

# Check if directory exists else, create it
if [ ! -d "${LOGDIR}" ]; then
    mkdir ${LOGDIR}
    echo "$(date) Created logfile directory ${LOGDIR}"
fi

# Creating pan scan main package
echo "$(date) Loading PAN configuration into the database" | tee -a ${LOGFILE}
psql -h ${DATABASE_HOST} -U ${DATABASE_USER} -d ${DATABASE_NAME} -f pan_main_prc.sql >>${LOGFILE}

# Check that determines if configuration tables need creating, truncating or pan is restarting
if [ "$RESTARTING" = "N" ]; then
    echo "$(date) Restarting has been set to NO. Please cancel now if incorrect. Sleeping for 2 minutes..." | tee -a ${LOGFILE}
    sleep 120

    echo "$(date) Creating PAN scan configuration tables" | tee -a ${LOGFILE}
    psql -q -h ${DATABASE_HOST} -U ${DATABASE_USER} -d ${DATABASE_NAME} -f pan_configuration.sql >>${LOGFILE}
else
    echo "$(date) Restarting PAN scan, checking configuration tables exist" | tee -a ${LOGFILE}

    # Check if required config tables exits
    PAN_CONFIG=$(psql -t -h ${DATABASE_HOST} -U ${DATABASE_USER} -d ${DATABASE_NAME} -c "select count(*) from information_schema.tables where table_name in ('pan_progress', 'pan_results', 'pan_exceptions');")

    if [ "$PAN_CONFIG" -lt 3 ]; then
        echo "$(date) ERROR - Required configuration tables for PAN to restart do NOT exist!" | tee -a ${LOGFILE}
    else
        echo "$(date) All necessary PAN configuration tables exist, pan resuming" | tee -a ${LOGFILE}
    fi
fi

echo "$(date) PAN run starting..." | tee -a ${LOGFILE}
echo "$(date) Check table pan_progress for updates on progress" | tee -a ${LOGFILE}

# Running Pan
psql -q -h ${DATABASE_HOST} -U ${DATABASE_USER} -d ${DATABASE_NAME} -c "call pan_scan_prc('{}','{${DATABASE_SCHEMA}}', 'hits', '${VERBOSITY}');" >> ${LOGFILE}

echo "$(date) PAN run complete, checking results" | tee -a ${LOGFILE}

# Spool results to a txt file
RESULTS=$(psql -q -t -h ${DATABASE_HOST} -U ${DATABASE_USER} -d ${DATABASE_NAME} -c "select count(*) from pan_results;")

if [ "$RESULTS" -lt 1 ]; then
    echo "$(date) OK - No results found" | tee -a ${LOGFILE}
else
    CSV="${LOGDIR}/${DATABASE_NAME}-pan_results-${DATE}.csv"
    echo "table_column,pan_found,pk_id,query" > ${CSV}
    psql -q -t -h ${DATABASE_HOST} -U ${DATABASE_USER} -d ${DATABASE_NAME} -c "\copy (SELECT schemaname||'.'||tablename||'('||columnname||')' as table_column, left(value, 80) as pan_found, 'PK: '||pk_column||'('||pk_value||')' as pk_id, 'select '||columnname||' from '||schemaname||'.'||tablename||' where '||pk_column||'='||pk_value||';' as query FROM pan_results ORDER BY id) to '${CSV}' csv header;"

    # Email notification
    echo -e "Please verify the attached CSV file for details of PANs found." | mailx -s "PANs detected for ${DATABASE_HOST}" -a ${CSV} ${EMAIL} | tee -a ${LOGFILE}
fi

# Error checking
chk_run=$(grep -iEc "FATAL|ERROR" ${LOGFILE})

if [ ${chk_run} -gt 1 ]; then
    echo "$(date) ERROR - PAN run has errors. Check ${LOGFILE}" | tee -a ${LOGFILE}
    exit 1
else
    echo "$(date) OK - PAN run successful" | tee -a ${LOGFILE}
fi