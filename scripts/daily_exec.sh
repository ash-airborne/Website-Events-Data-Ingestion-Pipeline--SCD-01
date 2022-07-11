#!/bin/bash

# creating log file name 
LOG_DIR=/home/saif/cohort_ff11/logs
FILE_NAME=`basename $0`
DT=`date '+%Y%m%d_%H:%M:%S'`
LOG_FILE_NAME=${LOG_DIR}/${FILE_NAME}_${DT}.log

# reading file name from cla
FL_NAME=$1

# validating if any arguments ahave been passed from CLA
CHECK=$#
if [ ${CHECK} -eq 0 ]
then 
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} No arguments passed" >> ${LOG_FILE_NAME}
	echo "${TMST} Execute script like: sh $0 'file_name'" >> ${LOG_FILE_NAME}
	exit 1
else
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} DATA INGESTION INTO MYSQL TABLE ${TABLE} UNSUCCESSFUL" >> ${LOG_FILE_NAME}
fi

# sourcing the parameter file
. /home/saif/cohort_ff11/env/prj.prm

# loading the data into mysql db
mysql --local-infile=1 -u${USER} -p${PASS} -e " 
						set global local_infile=1;
						
						use ${DB};
						
						truncate table ${TABLE};
						
						LOAD DATA LOCAL INFILE '/home/saif/cohort_ff11/datasets/${FL_NAME}'
						INTO TABLE ${TABLE}
						FIELDS TERMINATED BY ',';
						
						update data set last_modified=now();"


# validating if the data has been loaded in the SQL
CHECK=$?
if [ ${CHECK} -eq 0 ]
then 
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} DATA LOADED SUCCESSFULLY INTO MYSQL TABLE ${TABLE}" >> ${LOG_FILE_NAME}
else
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} DATA INGESTION INTO MYSQL TABLE ${TABLE} UNSUCCESSFUL" >> ${LOG_FILE_NAME}
fi

# copying the dataset to archive folder
cp /home/saif/cohort_ff11/datasets/${FL_NAME} /home/saif/cohort_ff11/archive/

# validating if the data has been copied 				
CHECK=$?
if [ ${CHECK} -eq 0 ]
then 
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Dataset has been copied to archive" >> ${LOG_FILE_NAME}
else
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Datset copying to archive failed" >> ${LOG_FILE_NAME}
fi

# executing the sqoop job
sqoop job --exec ${JOB_NAME}

# validating if the sqoop job has been executed successfully or not
CHECK=$?
if [ ${CHECK} -eq 0 ]
then 
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} DATA Ingestion to hadoop successfull" >> ${LOG_FILE_NAME}
else
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} DATA INGESTION to hadoop unsuccessful" >> ${LOG_FILE_NAME}
fi





# creating a copy of ingested data in user's hive warehouse directory
hdfs dfs -cp ${OP_DIR}${TABLE} ${HIVE_DIR}${DB}.db/${TABLE}

# validating if the data has been copied from hdfs to user hive warehouse directory
CHECK=$?
if [ ${CHECK} -eq 0 ]
then 
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Copying to hive warehouse successful" >> ${LOG_FILE_NAME}
else
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Copying to hive warehouse unsuccessful" >> ${LOG_FILE_NAME}
fi

# setting dynamic partition properties for creation of partitions
hive -e " 
	  use ${DB};
	  set hive.exec.dynamic.partition.mode=nonstrict;
	  set hive.exec.dynamic.partition=true; 
	  
	  load data inpath '${HIVE_DIR}${DB}.db/${TABLE}' overwrite into table sample_data_stg;"
	  
VAR=`hive -e "use ${DB}; select max(last_modified) from sample_data_stg;"`

hive -e "
	  use ${DB};
	  
	  set hive.exec.dynamic.partition.mode=nonstrict;
	  set hive.exec.dynamic.partition=true; 
	  
	  insert overwrite table sample_data_part partition (year, month) select custid, username , quote_count, ip, entry_time, prp_1, prp_2, 
          prp_3, ms, http_type, purchase_category, total_count, purchase_sub_category, http_info, status_code, last_modified, 
          day(from_unixtime(unix_timestamp(entry_time, 'dd/MMM/yyyy:HH:mm:ss'))) as day,
          cast(year(from_unixtime(unix_timestamp(entry_time, 'dd/MMM/yyyy:HH:mm:ss'))) as string) as year,
	  cast(month(from_unixtime(unix_timestamp(entry_time, 'dd/MMM/yyyy:HH:mm:ss'))) as string) as month from sample_data_stg; 
	  
	  insert overwrite table sample_data_recon select * from sample_data_stg where unix_timestamp(last_modified) >= unix_timestamp('${VAR}');
	  "
	  
# validating if the data has been inserted into hive tables	  
CHECK=$?
if [ ${CHECK} -eq 0 ]
then 
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Data inserted into hive tables successfully" >> ${LOG_FILE_NAME}
else
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Data insertion into hive tables unsuccessful" >> ${LOG_FILE_NAME}
fi

# truncating the old records
mysql -u${USER} -p${PASS} -e "use ${DB};  
			      truncate table recon;"
			      
# validating if the recon table has been truncated	  
CHECK=$?
if [ ${CHECK} -eq 0 ]
then 
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Table Truncated" >> ${LOG_FILE_NAME}
else
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Table not Truncated" >> ${LOG_FILE_NAME}
fi



#exporting recon data to recon table in mysql
sqoop export --connect jdbc:mysql://${HOST}:${PORT}/${DB}?useSSL=False \
--username ${USER} --password-file ${PASS_FILE} \
--table ${REC} \
--export-dir /user/hive/warehouse/project01.db/sample_data_recon

# validating if the data has been inserted into recon table  
CHECK=$?
if [ ${CHECK} -eq 0 ]
then 
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Data inserted into ${REC} table successfully" >> ${LOG_FILE_NAME}
else
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Data insertion into ${REC} table unsuccessful" >> ${LOG_FILE_NAME}
fi

# validating data reconciliation
COUNT1=`mysql -u${USER} -p${PASS} -e "use ${DB};  select count(*) from data"`
ABC=`echo ${COUNT1} | cut -f 2 -d ' '`
COUNT2=`mysql -u${USER} -p${PASS} -e "use ${DB};  select count(*) from recon;"`
DEF=`echo ${COUNT2} | cut -f 2 -d ' '`

if [ ${ABC} -eq ${DEF} ]
then
	
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	rm -r /home/saif/cohort_ff11/sqoop/*.java
	echo "${TMST} - Removed Java File  Successfully" >> ${LOG_FILE_NAME}
	echo "${TMST} Data Reconciliation Successful" >> ${LOG_FILE_NAME}
else
	TMST=`date '+%Y-%m-%d %H:%M:%S'`
	echo "${TMST} Data Reconciliation unsuccessful" >> ${LOG_FILE_NAME}
fi	

