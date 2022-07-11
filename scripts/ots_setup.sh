#!/bin/bash

# Sourcing the parameter file
. /home/saif/cohort_ff11/env/prj.prm

# creating log file name 
LOG_DIR=/home/saif/cohort_ff11/logs
FILE_NAME=`basename $0`
DT=`date '+%Y%m%d_%H:%M:%S'`
LOG_FILE_NAME=${LOG_DIR}/${FILE_NAME}_${DT}.log

# connecting to mysql and creating tables
mysql -u${USER} -p${PASS} -e "
			      create database ${DB};
			      use ${DB};
			      
			      create table ${TABLE}(
			      custid integer(10),
			      username varchar(30),
			      quote_count varchar(30),
			      ip varchar(30),
			      entry_time varchar(30),
			      prp_1 varchar(30),	
			      prp_2 varchar(30),
			      prp_3 varchar(30),
			      ms varchar(30),
			      http_type varchar(30),
			      purchase_category varchar(30),
			      total_count varchar(30),
			      purchase_sub_category varchar(30),
			      http_info varchar(30),
			      status_code integer(10),
			      last_modified datetime
			      );
			      
			      create table recon(
			      custid integer(10),
			      username varchar(30),
			      quote_count varchar(30),
			      ip varchar(30),
			      entry_time varchar(30),
			      prp_1 varchar(30),	
			      prp_2 varchar(30),
			      prp_3 varchar(30),
			      ms varchar(30),
			      http_type varchar(30),
			      purchase_category varchar(30),
			      total_count varchar(30),
			      purchase_sub_category varchar(30),
			      http_info varchar(30),
			      status_code integer(10),
			      last_modified datetime
			      );"
			      

# validation if the mysql command has execcuted successfully.			
CHECK=$?
if [ ${CHECK} -eq 0 ]
then 
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Tables have been created in SQL" >> ${LOG_FILE_NAME}
else
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Table creation UNSUCCESSFUL" >> ${LOG_FILE_NAME}
fi


# creating staging table and external table in hive
hive -e "
	create database project01;
	
	use project01;
	
	create table sample_data_stg
	(
	custid int,
	username string,
	quote_count string,
	ip string,
	entry_time string,
	prp_1 string,
	prp_2 string,
	prp_3 string,
	ms string,
	http_type string,
	purchase_category string,
	total_count string,
	purchase_sub_category string,
	http_info string,
	status_code int,
	last_modified timestamp
	)
	row format delimited fields terminated by ',';
	
	set hive.exec.dynamic.partition.mode=nonstrict;
	set hive.exec.dynamic.partition=true;
	
	create external table sample_data_part
	(
	custid int,
	username string,
	quote_count string,
	ip string,
	entry_time string,
	prp_1 string,
	prp_2 string,
	prp_3 string,
	ms string,
	http_type string,
	purchase_category string,
	total_count string,
	purchase_sub_category string,
	http_info string,
	status_code int,
	day int,
	last_modified timestamp
	)
	partitioned by (year string, month string)
	row format delimited fields terminated by ',';
	
	
	create table sample_data_recon
	(
	custid int,
	username string,
	quote_count string,
	ip string,
	entry_time string,
	prp_1 string,
	prp_2 string,
	prp_3 string,
	ms string,
	http_type string,
	purchase_category string,
	total_count string,
	purchase_sub_category string,
	http_info string,
	status_code int,
	last_modified timestamp
	)
	row format delimited fields terminated by ',';
	"


# validation if the hive tables have been created.			
CHECK=$?
if [ ${CHECK} -eq 0 ]
then 
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Tables have been created successfully in HIVE" >> ${LOG_FILE_NAME}
else
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Table creationvin HIVE UNSUCCESSFUL" >> ${LOG_FILE_NAME}
fi

# Creating a incremental last modified sqoop job
sqoop job --create ${JOB_NAME} -- import --connect jdbc:mysql://${HOST}:${PORT}/${DB}?useSSL=False \
--username ${USER} --password-file ${PASS_FILE} \
--query 'select * from data where $CONDITIONS' \
--split-by custid \
--incremental lastmodified \
--check-column last_modified \
--last-value '1900-01-01 00:00:00' \
--merge-key custid \
--target-dir ${OP_DIR}${TABLE}


# validation if the sqoop job has been created or not
CHECK=$?
if [ ${CHECK} -eq 0 ]
then 
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Sqoop job created successfully" >> ${LOG_FILE_NAME}
else
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Sqoop job has not been created" >> ${LOG_FILE_NAME}
fi


