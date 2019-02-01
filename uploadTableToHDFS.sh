#!/bin/bash

##################################################################################################################################################
## Name: uploadTableToHDFS.sh
## Purpose: This script uploads the data from local to hdfs for a specific table 
## Author: Vikash/Anika
## Arguments: #1. Database name (string) #2. Table name (string)
## Created Date: 2016-10-12
## Modified Date: 2016-10-12
## Modification Description: Naming conventions and re-structuring of the script.
## Modified Date: 2017-10-24
## Modified By: hravi
## Modification Description: Added logic to refresh the table in impala
###################################################################################################################################################

echo "Start Time for $0 "`date` 

## Checking number of argument passed to script.
if [ $# -ne 2 ] ; 
then
    echo "[Error_1000 in $0]: Invalid arguments, should be 2 <db-name> <table-name>"
    echo -n "$0 finished with error at "
    date
    echo "============================================================================="
    exit 1000
else
  ## Setting variables by arguments
  DB_NAME=$1
  DB_NAME=`echo "$DB_NAME" | tr '[:upper:]' '[:lower:]'`
  DATA_LOC="${BICOE_HOME}/data/${BICOE_ENV}/inbound/${HIVE_ENV}${DB_NAME}"
  TABLE_NAME=$2
  TABLE_NAME=`echo "$TABLE_NAME" | tr '[:upper:]' '[:lower:]'`

  ## Checking data directory for received DB as argument
  if [ ! -d ${DATA_LOC} ] ; then
     echo "[Error_1010 in $0]: Directory ${DATA_LOC} not found !"
     echo -n "$0 finished with error at "
     date
     echo "============================================================================="
     exit 1010
  fi

    ## Checking data directory for received table as argument
    if [ ! -d ${DATA_LOC}/${TABLE_NAME} ] ; then
       echo "[Error_1010 in $0]: Directory ${DATA_LOC}/${TABLE_NAME} not found !"
       echo -n "$0 finished with error at "
       date
       echo "============================================================================="
       exit 1010
    fi

    HDFS_RAW_DB="/user/hive/warehouse/rockwell/enterprise/raw/${HIVE_ENV}${DB_NAME}.db"
fi
echo " "
start_time=`date +%s`
echo -n "START TIME of UPLOADING ${HIVE_ENV}${DB_NAME}.${TABLE_NAME} is: "
date
echo "Source: ${DATA_LOC}/${TABLE_NAME} "
echo "Destination: ${HDFS_RAW_DB}/${TABLE_NAME} "
echo " "

count=`hdfs dfs -ls ${HDFS_RAW_DB}/${TABLE_NAME}/ | wc -l`;
RC=$?
if [ $RC -ne 0 ]
then
   echo "[Error_1042 in $0]: hdfs list command failed for ${HDFS_RAW_DB}/${TABLE_NAME} with Return_Code: $RC !"
   echo "[COMMAND]: hdfs dfs -ls ${HDFS_RAW_DB}/${TABLE_NAME}/*"
   end_time=`date +%s`
   echo -n "END TIME of UPLOADING ${DB_NAME}.${TABLE_NAME} is: "
   date
   diff=$(echo "scale=2; $((end_time/60)) - $((start_time/60))" | bc)
   #diff=$(($((end_time/60)) - $((start_time/60))))
   echo "TOTAL TIME taken to upload ${HIVE_ENV}${DB_NAME}.${TABLE_NAME} is : ${diff} Minutes"
   echo "------------------------------------------------------------------------------"
   exit 1042
fi

## Checking if files exists in hdfs for db and table
if [ ${count} -eq 0 ]; then 
   echo "${HDFS_RAW_DB}/${TABLE_NAME}/ is empty in HDFS."
else 
   ## Removing existing files from hdfs
   echo "Removing existing files from HDFS ${HDFS_RAW_DB}/${TABLE_NAME}/"
   hdfs dfs -rm -r -skipTrash ${HDFS_RAW_DB}/${TABLE_NAME}/*
   RC=$?
   if [ $RC -ne 0 ]
   then
      echo "[Error_1041 in $0]: hdfs remove command failed for ${HDFS_RAW_DB}/${TABLE_NAME} with Return_Code: $RC !"
      echo "[COMMAND]: hdfs dfs -rm -r -skipTrash ${HDFS_RAW_DB}/${TABLE_NAME}/*"
      end_time=`date +%s`
      echo -n "END TIME of UPLOADING ${DB_NAME}.${TABLE_NAME} is: "
      date
      #diff=$(echo "scale=2; $((end_time/60)) - $((start_time/60))" | bc)
      diff=$(($((end_time/60)) - $((start_time/60))))
      echo "TOTAL TIME taken to upload ${HIVE_ENV}${DB_NAME}.${TABLE_NAME} is : ${diff} Minutes"
      echo "------------------------------------------------------------------------------"
      exit 1041
   fi
fi

## Uploading data files in to HDFS.
filePath=${DATA_LOC}/${TABLE_NAME}
echo "Loading ${DB_NAME}.${TABLE_NAME} from ${filePath}"
hdfs dfs -put ${filePath}/* ${HDFS_RAW_DB}/${TABLE_NAME}/.
RC=$?
if [ $RC -ne 0 ]
then
     echo "[Error_1040 in $0]: hdfs put command failed for ${HIVE_ENV}${DB_NAME}.${TABLE_NAME} with Return_Code: $RC !"
     echo "[COMMAND]: hdfs dfs -put ${filePath}/* ${HDFS_RAW_DB}/${TABLE_NAME}/."
     end_time=`date +%s`
     echo -n "END TIME of UPLOADING ${DB_NAME}.${TABLE_NAME} is: "
     date
     diff=$(echo "scale=2; $((end_time/60)) - $((start_time/60))" | bc)
     #diff=$(($((end_time/60)) - $((start_time/60))))
     echo "TOTAL TIME taken by $0 for ${HIVE_ENV}${DB_NAME}.${TABLE_NAME} is : ${diff} Minutes"
     echo "------------------------------------------------------------------------------"
     exit 1040
fi     
echo "${HIVE_ENV}${DB_NAME}.${TABLE_NAME} Loaded Successfully"
end_time=`date +%s`
echo -n "END TIME of UPLOADING ${HIVE_ENV}${DB_NAME}.${TABLE_NAME} is: "
date
#diff=$(echo "scale=2; $((end_time/60)) - $((start_time/60))" | bc)
diff=$(($((end_time/60)) - $((start_time/60))))
echo "TOTAL TIME taken to upload ${HIVE_ENV}${DB_NAME}.${TABLE_NAME} is : ${diff} Minutes"
echo "------------------------------------------------------------------------------"

echo -n "End Time for $0 "
date

impala-shell ${IMPALA_URL} -q "invalidate metadata ${HIVE_ENV}${DB_NAME}.\`${TABLE_NAME}\`;"
RC=$?
if [ $RC -ne 0 ]
then
     echo "[Error_1090 in $0]: invalidate metadata command failed for ${HIVE_ENV}${DB_NAME}.${TABLE_NAME} with Return_Code: $RC !"

     exit 1090
fi
echo "${HIVE_ENV}${DB_NAME}.${TABLE_NAME} metadata refresh completed in impala"
echo "------------------------------------------------------------------------------"

impala-shell ${IMPALA_URL} -q "refresh ${HIVE_ENV}${DB_NAME}.\`${TABLE_NAME}\`;"
RC=$?
if [ $RC -ne 0 ]
then
     echo "[Error_1100 in $0]: impala refresh command failed for ${HIVE_ENV}${DB_NAME}.${TABLE_NAME} with Return_Code: $RC !"

     exit 1100
fi
 echo "${HIVE_ENV}${DB_NAME}.${TABLE_NAME} refresh completed in impala"
echo "------------------------------------------------------------------------------"
exit 0

