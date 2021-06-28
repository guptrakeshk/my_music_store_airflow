from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'rakeshg',
    #'start_date': datetime(2019, 1, 12),
    'email_on_retry': False,
    'retries': 2,
    'retries_delay': timedelta(minutes=7),
    'start_date': datetime(2021, 6, 4, 0, 0, 0, 0),
    'end_date': datetime(2021, 7, 4, 0, 0, 0, 0)
}


dag = DAG('etl_with_airflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
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

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql= SqlQueries.songplay_table_insert
    
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql= f""" 
        DELETE FROM public.users;
        INSERT INTO public.users (userid,first_name,last_name, gender, level) 
        {SqlQueries.users_table_insert}"""
)


load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql= f""" 
        DELETE FROM public.songs;
        INSERT INTO public.songs (songid, title, artistid, year, duration)
        {SqlQueries.songs_table_insert} """
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql= f"""
        DELETE FROM public.artists;
        INSERT INTO public.artists (artistid, name, location, lattitude, longitude)
        {SqlQueries.artists_table_insert} """
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql = f""" 
        DELETE FROM public.time;
        INSERT INTO public.time (start_time, hour, day, week, month, year, weekday)
        {SqlQueries.time_table_insert} """
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    #table="users",
    #columns_list = ["userid"]
    table_dict = {"users": ["userid"], "songs": ["songid"], "artists":["artistid"], "time":["start_time"]}
)


# Create DAG for the proper workflow.

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_users_dimension_table
load_songplays_table >> load_songs_dimension_table
load_songplays_table >> load_artists_dimension_table
load_songplays_table >> load_time_dimension_table

load_users_dimension_table >> run_quality_checks
load_songs_dimension_table >> run_quality_checks
load_artists_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

