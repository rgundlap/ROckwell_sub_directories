-------------------- HEADER ----------------------
-- Name                : 0accnt_asgn_text.ctl
-- Purpose             : To create table 0accnt_asgn_text in text format for raw layer in database sap_ecc.db
-- Description         : Customer Account Assignment Group
-- Author              : gravi2
-- Creation Date       : 2018 may 25

-------------------- HEADER ----------------------

DROP TABLE IF EXISTS ${env}SAP_ECC.0accnt_asgn_text PURGE;
CREATE EXTERNAL TABLE IF NOT EXISTS ${env}SAP_ECC.0accnt_asgn_text
(
SPRAS	 STRING,
KTGRD	 STRING,
VTEXT	 STRING,
ODQ_CHANGEMODE	 STRING,
ODQ_ENTITYCNTR	 DECIMAL (19,0 ),
EXTRACT_DTTM	 TIMESTAMP
)
	COMMENT 'Customer Account Assignment Group'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '28'
     STORED AS TEXTFILE
        LOCATION '/user/hive/warehouse/rockwell/enterprise/raw/${env}sap_ecc.db/0accnt_asgn_text/'
    TBLPROPERTIES("SKIP.HEADER.LINE.COUNT"="0");