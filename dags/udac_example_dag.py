from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner' : 'rakeshg',
    'start_date' : datetime(2021, 6, 27, 0, 0, 0, 0),
    'end_date' : datetime(2021, 7, 4, 0, 0, 0, 0),
    'depends_on_past': False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'catchup' : False,
    'email' : ['gupt.rakeshk@gmail.com'],
    'email_on_retry' : False,
}


dag = DAG('etl_with_airflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    table="staging_events",
    region="us-west-2",
    extra_params="JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    table="staging_songs",
    region="us-west-2",
    extra_params="JSON 'auto' compupdate off"
)

# Fact tables are usually so massive that they should only allow append type functionality.
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql= f""" 
        INSERT INTO public.songplays (playid, start_time, userid, level, songid, artistid, 
                            sessionid, location, user_agent) 
        {SqlQueries.songplay_table_insert} """
    
)

# Dimension loads are often done with the truncate-insert pattern where the target table 
# is emptied before the load. 'delete_and_load' parameter controls this behavior.
load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    delete_and_load = True,
    table = "public.users",
    sql= f""" 
        INSERT INTO public.users (userid,first_name,last_name, gender, level) 
        {SqlQueries.users_table_insert}"""
)

# Dimension loads are often done with the truncate-insert pattern where the target table 
# is emptied before the load. 'delete_and_load' parameter controls this behavior.
load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    delete_and_load = True,
    table = "public.songs",
    sql= f""" 
        INSERT INTO public.songs (songid, title, artistid, year, duration)
        {SqlQueries.songs_table_insert} """
)

# Dimension loads are often done with the truncate-insert pattern where the target table 
# is emptied before the load. 'delete_and_load' parameter controls this behavior.
load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    delete_and_load = True,
    table = "public.artists",
    sql= f"""
        INSERT INTO public.artists (artistid, name, location, lattitude, longitude)
        {SqlQueries.artists_table_insert} """
)

# Dimension loads are often done with the truncate-insert pattern where the target table 
# is emptied before the load. 'delete_and_load' parameter controls this behavior.
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    delete_and_load = True,
    table = "public.time",
    sql = f""" 
        INSERT INTO public.time (start_time, hour, day, week, month, year, weekday)
        {SqlQueries.time_table_insert} """
)


# data quality operator, which is used to run checks on the data itself
# Checks for whether table has records after processing and load. Expected result is more than 0 records.
# Checks for NULL into table's NOT NULLABLE columns. Expected results is 0 NULL records
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    #table_dict = {"users": ["userid"], "songs": ["songid"], "artists":["artistid"], "time":["start_time"]}
    data_checks = [
        {"table":"songs",'test_sql': "SELECT COUNT(*) FROM songs", "expected_result": 0, "comparison": '>'},
        {"table":"users",'test_sql': "SELECT COUNT(*) FROM users", "expected_result": 0, "comparison": '>'},
        {"table":"artists",'test_sql': "SELECT COUNT(*) FROM artists", "expected_result": 0, "comparison": '>'},
        {"table":"time",'test_sql': "SELECT COUNT(*) FROM time", "expected_result": 0, "comparison": '>'},
        {"table":"songs",'test_sql': "SELECT COUNT(*) FROM songs WHERE  (songid) IS NULL", "expected_result": 0, "comparison": None},
        {"table":"users",'test_sql': "SELECT COUNT(*) FROM users WHERE  (userid) IS NULL", "expected_result": 0, "comparison": None},
        {"table":"artists",'test_sql': "SELECT COUNT(*) FROM artists WHERE  (artistid) IS NULL", "expected_result": 0, "comparison": None},
        {"table":"time",'test_sql': "SELECT COUNT(*) FROM time WHERE  (start_time) IS NULL ", "expected_result": 0, "comparison": None},
    ]
)


# Create DAG for the proper workflow.

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_users_dimension_table, load_songs_dimension_table, load_artists_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator

