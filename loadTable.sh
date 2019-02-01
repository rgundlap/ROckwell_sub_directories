#!/bin/bash

##################################################################################################################################################
## Name: loadTable.sh
## Purpose: Load specified table in a specified DB given as arguments.
## Author: Vikash/Anika
## Arguments: #1. Database name (string) #2. Table name (string)
## Created Date: 2016-10-12
## Modified Date: 2016-10-12
## Modification Description: Naming conventions and re-structuring of the script.
## Modified Date: 2017-10-24
## Modified By: hravi
## Modification Description: Added logic to refresh the table in impala
## Modification Date: 2017-12-14
## Modified By : dshah5
## Modification Desc: Modified script to handle failure of Impala commands and display correct messages while script run
###################################################################################################################################################

echo "Start Time of $0 "`date`

## Checking number of argument passed to script.
if [ $# -ne 2 ]
then
    echo "[Error_1000 in $0]: Invalid arguments, Expecting 2 arguments <db_name> <table_name>"
    echo "Expected format is $0 <db_name> <table_name>"
    echo -n "$0 finished with error at "
    date
    echo "============================================================================="
    exit 1000
else
  ## Setting variables by arguments
  DB_NAME=$1
  DB_NAME=`echo "$DB_NAME" | tr '[:upper:]' '[:lower:]'`
  SCRIPT_LOC="${BICOE_BASE}/dml/${HIVE_ENV}${DB_NAME}"
  TABLE=$2
  TABLE=`echo "$TABLE" | tr '[:upper:]' '[:lower:]'`

  ## Checking script location for received as argument. 
  if [ ! -d ${SCRIPT_LOC} ] ; then
    echo "[Error_1010 in $0]: Directory ${SCRIPT_LOC} for DB ${DB_NAME} not found !"
    echo "Expected format is $0 <db_name> <table_name>"
    echo -n "$0 finished with error at "
    date
    echo "============================================================================="
    exit 1010
  fi

  ## Checking script for table received as argument. 
  if [ ! -f ${SCRIPT_LOC}/${TABLE}.ctl ] ; then
    echo "[Error_1020 in $0]: File ${SCRIPT_LOC}/${TABLE}.ctl for table ${TABLE} not found !"
    echo "Expected format is $0 <db_name> <table_name>"
    echo -n "$0 finished with error at "
    date
    echo "============================================================================="
    exit 1020
  fi
fi

script="${SCRIPT_LOC}/${TABLE}.ctl"

echo " "
start_time=`date +%s`
echo -n "START TIME of LOADING ${HIVE_ENV}${DB_NAME}.${TABLE} is: "
date
echo " "

beeline -u ${HIVE_URL} ${HIVE_USER} ${HIVE_CONF} --hivevar env=${HIVE_ENV} -f ${script}
RC=$?
if [ $RC -ne 0 ]
then
     echo "[Error_1050 in $0]: Beeline command for ${script} failed with Return_Code: $RC !"
     echo "[COMMAND]: beeline -u ${HIVE_URL} ${HIVE_USER} ${HIVE_CONF} --hivevar env=${HIVE_ENV} -f ${script}"
     end_time=`date +%s`
     echo -n "END TIME of LOADING ${HIVE_ENV}${DB_NAME}.${TABLE} is: "
     date
     diff=$(echo "scale=2; $((end_time/60)) - $((start_time/60))" | bc)
     #diff=$(($((end_time/60)) - $((start_time/60))))
     echo "TOTAL TIME taken by $0 for ${HIVE_ENV}${DB_NAME}.${TABLE} is : ${diff} Minutes"
     echo "------------------------------------------------------------------------------"
     exit 1050
fi
echo "${DB_NAME}.${TABLE} Loaded Successfully"
end_time=`date +%s`
echo -n "END TIME of LOADING ${DB_NAME}.${TABLE} is: "
date
#diff=$(echo "scale=2; $((end_time/60)) - $((start_time/60))" | bc)
diff=$(($((end_time/60)) - $((start_time/60))))
echo "TOTAL TIME taken to load ${HIVE_ENV}${DB_NAME}.${TABLE} is : ${diff} Minutes"
echo "------------------------------------------------------------------------------"

echo "End Time for $0 "`date`


impala-shell ${IMPALA_URL} -q "invalidate metadata ${HIVE_ENV}${DB_NAME}.\`${TABLE}\`;"
RC=$?
if [[ $RC -ne 0 &&  ${DB_NAME} != "outbound" ]]
then
     echo "[Error_1090 in $0]: invalidate metadata command failed for ${HIVE_ENV}${DB_NAME}.${TABLE} with Return_Code: $RC !"

     exit 1090
fi

echo "------------------------------------------------------------------------------"


impala-shell ${IMPALA_URL} -q "refresh ${HIVE_ENV}${DB_NAME}.\`${TABLE}\`;"

RC=$?

if [[ $RC -ne 0 && ${DB_NAME} != "outbound" ]]
then
     echo "[Error_1100 in $0]: impala refresh command failed for ${HIVE_ENV}${DB_NAME}.${TABLE} with Return_Code: $RC !"

     exit 1100
fi
 
echo "------------------------------------------------------------------------------"



exit 0

