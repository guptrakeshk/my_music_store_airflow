## Data Engineering Project Using Airflow, AWS Redshift and AWS S3


### A hands-on, Project-based instruction for data engineering ETL using Apache Airflow and AWS Redshift

This is a collection of resources for data engineering ETL for a fictious music company.

Email: gupt.rakeshk@gmail.com

This project walks through end-to-end data engineering pipeline steps that are needed in a typical project.
Access event log data from AWS S3 to dynamically read data , create a staging database tables in the AWS REdshift and ETL pipeline using Airflow for a music streaming app. 
Airflow DAG application reads songs and events log data from AWS S3. Create AWS Redshift staging tables from S3 data logs. After processing and transforming songs and events log data, application stores processed data into facts and dimension table following STAR schema model. 

#### Pre-Requisites
- Ensure AWS IAM user is created with read access to S3 as well as necessary access for AWS EMR.
- AWS Redshift cluster is up, configured and running .
- AWS IAM user has right security policy set-up to access and perform ETL on AWS Redshift.
- Ensure IAM credentials is tested for right permission to access S3 for reading data.
- For security of confidential information, credentials details must be stored in Airflow Connections store.
- For better network efficiency, ensure Redshift cluster is created in the same region where S3 bucket holding events log and songs meta data is located .

### Steps involved are :
- Copy data that are stored in AWS S3. 
- Apply processing, transformation and subsequent writing to Redshift dimension and fact table.
- Once data is cleansed, transformed and loaded into Redshift, it is ready to asnwer queries for analytics & data science discovery.


#### Building ETL Pipeline steps:

- Airflow DAG tasks accesses S3 bucket to read events log and songs data.
- Airflow task automatically iterates through each event file from the event_log location to extract events data and copy into a staging table.
- Extract users, artists, songs, time and songplays activity data from events and songs staging data after processing and transformation.
- Write users, time, songs, artists dimention table and songplays fact table and store in the AWS Redshift.
- Once data is cleansed, transformed and loaded in Redshift, it is ready to asnwer specific queries for analytics.


#### How to execute programs :

Here are helpful steps in executing Spark program.

1. From project workspace click 'Access Airflow' to launch Airflow UI.
2. Create connections for AWS_Credentials and Redshift from the Airflow UI. Go to Admin --> Connections
3. Go to 'DAGs' in the UI. 
4. Turn 'on' *create_etl_schema* DAG and 'Trigger Dag' button. 
5. Verify all tasks are completely successfully.
5. Turn 'on' *etl_with_airflow* DAG and Trigger Dag' button.
6. Verify all tasks are completed successfully

Note: *create_etl_schema* DAG must run first in the sequence to create ETL schema in Redshift.



### Project Dataset 

#### Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

Song data location : s3://udacity-dend/song_data

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```
{"artist_id":"ARJNIUY12298900C91","artist_latitude":null,"artist_location":"","artist_longitude":null,"artist_name":"Adelitas Way","duration":213.9424,"num_songs":1,"song_id":"SOBLFFE12AF72AA5BA","title":"Scream","year":2009}
```

#### Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

Log data location : s3://udacity-dend/log_data
Log data json path location: s3://udacity-dend/log_json_path.json

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

And below is an example of what a event log looks like.
```
{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}
```

Song data schema that is used to enforce schema while reading data from songs data log

```
song_data_schema = StructType([
                StructField("song_id", StringType()),
                StructField("title", StringType(), True),
                StructField("artist_id", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("duration", DoubleType(), True),
                StructField("artist_name", StringType(), True),
                StructField("artist_location", StringType(), True),
                StructField("artist_latitude", DoubleType(), True),
                StructField("artist_longitude", DoubleType(), True),
                StructField("num_songs", IntegerType(), True)
])
```

### Project artifacts and DAG

- `*create_etl_schema*` : Airflow DAG to build Redshift schema for ETL pipeline. 
- `*etl_with_airflow*` : Airflow DAG to build an ETL pipeline that involves extracting source data from S3, stage into Redshift, process staging data and create dimension and fact tables using STAR model for analysis.
