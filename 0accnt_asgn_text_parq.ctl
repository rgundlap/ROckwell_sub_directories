-------------------- HEADER ----------------------
-- Name                : 0accnt_asgn_text_parq.ctl
-- Purpose             : To create table 0accnt_asgn_text_parq in parquet format for raw layer in database sap_ecc.db
-- Description         : Customer Account Assignment Group
-- Author              : gravi2
-- Creation Date       : 2018 MAY 25

-------------------- HEADER ----------------------

DROP TABLE IF EXISTS ${env}sap_ecc.0accnt_asgn_text_parq PURGE;
CREATE EXTERNAL TABLE IF NOT EXISTS ${env}sap_ecc.0accnt_asgn_text_parq LIKE ${env}sap_ecc.0accnt_asgn_text
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '28'
     STORED AS PARQUET
        LOCATION '/user/hive/warehouse/rockwell/enterprise/raw/${env}sap_ecc.db/0accnt_asgn_text_parq/'
    TBLPROPERTIES("SKIP.HEADER.LINE.COUNT"="0");
ALTER TABLE ${env}sap_ecc.0accnt_asgn_text_parq ADD COLUMNS (BICOE_LOAD_DTTM TIMESTAMP);