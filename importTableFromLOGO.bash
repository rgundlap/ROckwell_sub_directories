#!/bin/bash

##################################################################################################################################################
## Name: importTableFromLOGO.sh
## Purpose: This script imports data from LOGO TIGER database MSSQL server to hdfs for a specific table based on the object definition in the source.
## Author: proppik and dshah5
## Arguments: #1. Database name (string) #2. Table name (string)
## Created Date: 2018-07-09
## Description: Changed sqoop query to include extract_dttm . Uncommented the ##checking script location & ##checking script for table block.
###################################################################################################################################################


## Initial job specific environmental variables

case "${HIVE_ENV}" in 
'p_' )
    LOGO_SRV="jdbc:sqlserver://usmkedb154.ra-int.com:1433;database=tiger"
    LOGO_ACCT=Logo
    LOGO_ALIAS=${HIVE_ENV}logo.password.alias
    ;;
'q_')
    LOGO_SRV="jdbc:sqlserver://usmkedb154.ra-int.com:1433;database=tiger"
    LOGO_ACCT=Logo
    LOGO_ALIAS=p_logo.password.alias 
    ;;
'd_')
    LOGO_SRV="jdbc:sqlserver://usmkedb153.ra-int.com:1433;database=tiger"
    LOGO_ACCT=Logo
    LOGO_ALIAS=q_logo.password.alias 
    ;;
*)
   echo "[Error]: HIVE_ENV is not set or has invalid value"
esac

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
query=`cat "${script}"`
target_dir=/user/hive/warehouse/rockwell/enterprise/raw/${HIVE_ENV}${DB_NAME}.db/${TABLE}
##target_dir=/user/proppik/${TABLE}

echo " "
start_time=`date +%s`
echo -n "START TIME of IMPORTING ${HIVE_ENV}${DB_NAME}.${TABLE} is: "
date
echo " "

## chech hdfs target-dir
hadoop fs -test -d ${target_dir}
RC=$?
if [ $RC -eq 0 ]; then
    echo "Removing Target Dir ${target_dir} from HDFS"
    ## Remove old directory for the table in hdfs
    hadoop fs -rm -r -skipTrash ${target_dir}
    RC=$?
    if [ $RC -ne 0 ]
    then
        echo "[Error_1041 in $0]: hdfs remove command failed for ${target_dir} with Return_Code: $RC !"
        echo "[COMMAND]: hdfs dfs -rm -r -skipTrash ${target_dir}"
        end_time=`date +%s`
        echo -n "END TIME of IMPORTING ${HIVE_ENV}${DB_NAME}.${TABLE} is: "
        date
        diff=$(echo "scale=2; $((end_time/60)) - $((start_time/60))" | bc)
        #diff=$(($((end_time/60)) - $((start_time/60))))
        echo "TOTAL TIME taken by $0 for ${HIVE_ENV}${DB_NAME}.${TABLE} is : ${diff} Minutes"
        echo "------------------------------------------------------------------------------"
        exit 1041
    fi
else
    echo "Nothing to remove. Target Dir ${target_dir} not found in HDFS"
fi

sqoop import -Dhadoop.security.credential.provider.path=${JCEKS_PROVIDER} \
--connect "${LOGO_SRV}" \
--username "${LOGO_ACCT}" \
--password-alias "${LOGO_ALIAS}" \
--query "${query}" \
--fields-terminated-by '\034' \
--hive-delims-replacement ' ' \
--hive-import \
--map-column-hive extract_dttm=timestamp \
--hive-table ${TABLE} \
--hive-database ${HIVE_ENV}${DB_NAME} \
--num-mappers 1 \
--target-dir "${target_dir}" \
--null-string '\\N' \
--null-non-string '\\N';


RC=$?
if [ $RC -ne 0 ]
then
     echo "[Error_1050 in $0]: Sqoop import command for ${script} failed with Return_Code: $RC !"
     end_time=`date +%s`
     echo -n "END TIME of IMPORTING ${HIVE_ENV}${DB_NAME}.${TABLE} is: "
     date
     diff=$(echo "scale=2; $((end_time/60)) - $((start_time/60))" | bc)
     #diff=$(($((end_time/60)) - $((start_time/60))))
     echo "TOTAL TIME taken by $0 for ${HIVE_ENV}${DB_NAME}.${TABLE} is : ${diff} Minutes"
     echo "------------------------------------------------------------------------------"
     exit 1050
fi
echo "${DB_NAME}.${TABLE} Imported Successfully"
end_time=`date +%s`
echo -n "END TIME of IMPORTING ${DB_NAME}.${TABLE} is: "
date
#diff=$(echo "scale=2; $((end_time/60)) - $((start_time/60))" | bc)
diff=$(($((end_time/60)) - $((start_time/60))))
echo "TOTAL TIME taken to import ${HIVE_ENV}${DB_NAME}.${TABLE} is : ${diff} Minutes"
echo "------------------------------------------------------------------------------"

echo "End Time for $0 "`date`

impala-shell ${IMPALA_URL} -q "invalidate metadata ${HIVE_ENV}${DB_NAME}.\`${TABLE}\`;"
RC=$?
if [ $RC -ne 0 ]
then
     echo "[Error_1090 in $0]: invalidate metadata command failed for ${HIVE_ENV}${DB_NAME}.${TABLE} with Return_Code: $RC !"

     exit 1090
fi
echo "${HIVE_ENV}${DB_NAME}.${TABLE} metadata refresh completed in impala"
echo "------------------------------------------------------------------------------"

impala-shell ${IMPALA_URL} -q "refresh ${HIVE_ENV}${DB_NAME}.\`${TABLE}\`;"
RC=$?
if [ $RC -ne 0 ]
then
     echo "[Error_1100 in $0]: impala refresh command failed for ${HIVE_ENV}${DB_NAME}.${TABLE} with Return_Code: $RC !"

     exit 1100
fi
 echo "${HIVE_ENV}${DB_NAME}.${TABLE} refresh completed in impala"
echo "------------------------------------------------------------------------------"
exit 0

