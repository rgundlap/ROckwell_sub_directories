#!/bin/bash

##################################################################################################################################################
## Name: uploadToHDFS.sh
## Purpose: Execute unzip and upload script for specified tables in a specified DB given as arguments.
## Author: Vikash/Anika
## Arguments: #1. Database name (string) #2. List of tables (file)
## Created Date: 2016-10-12
## Modified Date: 2016-10-12
## Modification Description: Naming conventions and re-structuring of the script.
## Modified Date: 2016-10-20
## Modified By: vpareek
## Modification Description: Adding more configurable options to execute script, like (--skipFailure, --help).
###################################################################################################################################################

echo "Start Time of $0 "`date`
EXT_STS=0
HLT=1

## Function to print help.
## Name: showHelp
## Purpose: to show usage of the script and format to execute
## Parameters: None
## return Value: Exit code
showHelp () {
   SCRIPT=`basename "$0"`
   echo " Usage: "
   echo -e "\t ${SCRIPT} executes unzip and upload script for specified tables(as -t|--tableList) in a specified DB(as -d|--database)."
   echo -e "\t Format to run ${SCRIPT} is: "
   echo -e "\t ./${SCRIPT} -d <DB_NAME> -l <TABLE_LIST> [--skipFailure]"
   echo -e "\t Another way is:"
   echo -e "\t ./${SCRIPT} --database <DB_NAME> --tableList <TABLE_LIST> [--skipFailure]"
   echo -e "\t For usage: "
   echo -e "\t ./${SCRIPT} --help"
   echo ""
   echo " Available options are: "
   echo -e "\t -d|--database : Name of database for which tables need to be created."
   echo -e "\t -l|--tableList : List of tables need to be created for specified database as -d|--database."
   echo -e "\t --skipFailure : To skip failure and continue processing tables from the list, for e.g. If any raw table creation failed --skipFailure aviod exiting from this scripti and continue with next table in the list. Without this option script will exit from point of failure of table creation."
   echo -e "\t -h|--help : show help for ${SCRIPT}."
}

## Checking number of argument passed to script.
if [ ! $# -gt 0 ]
then
   EXT_STS=1000
   echo "[Error_${EXT_STS} in $0]: No arguments provided."
   showHelp
   echo "============================================================================="
   exit ${EXT_STS}
else
   ## iterating all the argumnets and setting variables
   while [[ $# -ge 1 ]]
   do
   option="$1"
   case $option in
       -d|--database)
       DB_NAME=$2
       DB_NAME=`echo "$DB_NAME" | tr '[:upper:]' '[:lower:]'`
       shift ## shift argument sequence
       ;;
       -l|--tableList)
       TABLE_LIST=$2
       shift ## shift argument sequence
       ;;
       --skipFailure)
       HLT=0
       shift ## shift argument sequence
       ;;
       -h|--help)
       showHelp
       exit 0;
       ;;
       *)
       EXT_STS=1001
       echo "[Error_${EXT_STS} in $0]: Invalid arguments."
       showHelp
       echo "============================================================================="
       exit ${EXT_STS}; 
       ;;
   esac
   shift ## shift argument sequence or value
   done
fi
   
## Checking arguments
if [ ! ${DB_NAME} ] || [ ! ${TABLE_LIST} ]
then
   EXT_STS=1002
   echo "[Error_${EXT_STS} in $0]: Database and table list must be privided for creating table."
   echo "try --help option"
   exit ${EXT_STS}
else  
   DATA_LOC=${BICOE_HOME}/data/${BICOE_ENV}/inbound/${HIVE_ENV}${DB_NAME}
   ## Checking script location for received DB.
   if [ ! -d ${DATA_LOC} ] ; then
      EXT_STS=1010
      echo "[Error_${EXT_STS} in $0]: Directory ${DATA_LOC} not found !"
      echo -n "$0 finished with error at "
      date
      echo "============================================================================="
      exit ${EXT_STS}
   fi

   ## Checking list of tables, received as argument.
   if [ ! -f ${TABLE_LIST} ] ; then
      EXT_STS=1020
      echo "[Error_${EXT_STS} in $0]: File ${TABLE_LIST} not found !"
      echo -n "$0 finished with error at "
      date
      echo "============================================================================="
      exit ${EXT_STS}
   fi
fi

echo "Unzipping and loading tables from ${DATA_LOC} to HDFS: /user/hive/warehouse/rockwell/enterprise/raw/${HIVE_ENV}${DB_NAME}"
# export HADOOP_CLIENT_OPTS="-Djline.terminal=jline.UnsupportedTerminal"
## Executing unzip and upload script for each table in the specified list of tables as argument.
for tablename in `cat ${TABLE_LIST}`
do
    if [ ! -d ${DATA_LOC}/${tablename} ] ; then
       EXT_STS=1010
       echo "[Error_${EXT_STS} in $0]: Directory ${DATA_LOC}/${tablename} not found !"
       if [ ${HLT} -eq 1 ]; then
          echo "--skipFailure is disabled, Exiting."
          echo "------------------------------------------------------------------------------"
          exit ${EXT_STS}
       fi
       echo "--skipFailure is enabled, Continue...."
       echo "-------------------------------------------------------------------------------"
    else
        ## Extracting data files
        ## START TIME
        ./unzipFilesInDir.sh "${DATA_LOC}/${tablename}"
        RC=$?
        if [ $RC -ne 0 ]; then
           EXT_STS=${RC}
           echo "[Error_${EXT_STS} in $0]: Script unzipFilesInDir.sh failed for ${DATA_LOC}/${tablename} with Return_Code: $RC !"
           if [ ${HLT} -eq 1 ]; then
              echo "--skipFailure is disabled, Exiting."
              echo "------------------------------------------------------------------------------"
              exit ${EXT_STS}
           fi
           echo "--skipFailure is enabled, Continue...."
           echo "-------------------------------------------------------------------------------"
        else
           ## Start uploading extracted file/s to HDFS
           ./uploadTableToHDFS.sh ${DB_NAME} ${tablename}
           RC=$?
           if [ $RC -ne 0 ]; then
              EXT_STS=${RC}
              echo "[Error_${EXT_STS} in $0]: Script uploadTableToHDFS.sh failed for ${DB_NAME}.${tablename} with Return_Code: $RC !"
              if [ ${HLT} -eq 1 ]; then
                 echo "--skipFailure is disabled, Exiting."
                 echo "------------------------------------------------------------------------------"
                 exit ${EXT_STS}
              fi
              echo "--skipFailure is enabled, Continue...."
              echo "-------------------------------------------------------------------------------"
           fi
        fi
    fi
done

echo "End Time for $0 "`date` 
echo "============================================================================="
exit ${EXT_STS}
