-------------------- HEADER ----------------------
-- Name                : zsd_burelation_parq
-- Purpose             : To load table zsd_burelation_parq in parquet format for raw layer in database sap_ecc.db
-- Author              : dshah5
-- Creation Date       : 2017-06-22
-- Modification Date   : 2017-06-22
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

DROP TABLE IF EXISTS ${env}sap_ecc.zsd_burelation_tmp PURGE;
CREATE TABLE IF NOT EXISTS ${env}sap_ecc.zsd_burelation_tmp
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '28'
     STORED AS PARQUET
        LOCATION '/user/hive/warehouse/rockwell/enterprise/raw/${env}sap_ecc.db/zsd_burelation_tmp/'
    TBLPROPERTIES("SKIP.HEADER.LINE.COUNT"="0")
AS
 SELECT T1.*, current_timestamp as BICOE_LOAD_DTTM FROM ${env}sap_ecc.zsd_burelation T1;

LOAD DATA INPATH '/user/hive/warehouse/rockwell/enterprise/raw/${env}sap_ecc.db/zsd_burelation_tmp/' OVERWRITE INTO TABLE ${env}sap_ecc.zsd_burelation_parq;

DROP TABLE IF EXISTS ${env}sap_ecc.zsd_burelation_tmp PURGE;

ANALYZE TABLE ${env}sap_ecc.zsd_burelation_parq COMPUTE STATISTICS;

