# Website-Events-Data-Ingestion-Pipeline--SCD-01
**Data Ingestion and Storage Pipeline** implementing *SCD-01* for data incoming from *'Hits' and 'Events'* of a website. The project was aimed to function as an *'On-Premises' Data Pipeline*. 

Technologies that were used to develop this pipeline are **Hadoop**, **Sqoop**, **Hive**, **MySQL** and **Unix** ***Shell Scripting***.

# Pipeline Workflow:
![Project Workflow](https://user-images.githubusercontent.com/55886870/178208866-46e28cd0-7dd7-419d-8c42-6f8f752f9c24.png)


# Summary
This is an automated pipeline utilising *Distributed processing* and *Distributed storage* designed for ingestion of data (***Hits and Events Data*** from a website) also designed for optimization of stored data in a data warehouse for quicker analysis and query execution. The implementation is based on *SCD-01*, in a gist which means that it does not preserve previous data instead updates it in case some changes are encountered and inserts the new data.

***The technologies that were used are mentioned below along with the description of how they were leveraged in order to solve the problem at hand.***

## Hadoop
*Hadoop* is an open-source framework for *distributed storage* and *distributed processing* using clusters of commodity hardware for processing data in large amounts. *Hadoop* consists of two main components **HDFS** and **MapReduce**, **HDFS** is the distributed file system for hadoop cluster and **MapReduce** is the processing Engine for *Hadoop* prioritising **Data locality**.

*"It was only sensible to use hadoop for this project as there is no better on premises distributed storage and processing option out there, and by looking at the amount of data to be processed anything other than hadoop did not make sense."*

## Sqoop
*Sqoop* is a data ingestion tool between **RDBMS**'s and *Hadoop Distributed File System*, It can *import* data from any **RDBMS to HDFS**, it can *incrementally append* or *incrementally load* data into **HDFS**, and can also *export* data from **HDFS to RDBMS**. Under the hood, *Sqoop* runs a **MapReduce** job so naturally it comes under the *Hadoop* family.

*"According to the requirement presented, the data was to be first made available in a MySQL database and then it had to be ingested into HDFS so that's where sqoop came into picture for the project."*

## Hive
*Hive* is an in-house *data warehouse* and *data summarization* tool in the *Hadoop* ecosystem. It is used to run querries and analysing large datasets. It has *built-in optimization* capabilities like **Partitions**, **Bucketing** and supports optimized file formats like **Parquet**, **Avro** and **ORC** to increase the query performance and since it runs on top of *Hadoop* the integration is seamless and works perfectly for the project. 

*"This is where we created partitions for optimization and implemented SCD-01 for this project."*

## MySQL
*MySQL* is a conventional **RDBMS**. It is an **OLTP** platform for querying data. As per the requirements, the data was to be loaded from the *edge node* into a specified *MySQL* DB and also had to be replicated for *data reconciliation*, ie; *checking the integrity of the data*.

*"We created two tables one for loading the data coming in daily in csv format and one for data reconciliation."*

## Unix Shell Scripting
Now to automate the *data ingestion pipeline* we used *shell scripting*. We could have used an *orchestration tool* like ***Airflow*** but for the scope of this project as there were only a few commands that needed to be executed on a daily basis, so *shell scripting* was enough to do the job. 

*"There were Two shell scripts used in total, the first one named 'ots_setup.sh' was used to create all the infrastructure to handle the data, like MySQL table creation, Hive managed table creation, sqoop job creation and reconciliation table creation, the second shell script named 'daily_exec.sh' was used to perform all the tasks for the pipeline like sqoop incremental imports, loading data from HDFS into hive, and checking for data integrity."*




