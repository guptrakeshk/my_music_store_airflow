from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
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

dag = DAG (
    'create_etl_schema',
    description = 'Dag to create schema for ETL using Airflow',
    default_args= default_args,
    max_active_runs= 1,
    schedule_interval= '@hourly'
)

start_operator = DummyOperator(task_id = 'Begin_execution', dag=dag)

end_operator = DummyOperator(task_id = 'End_execution' , dag=dag)

drop_etl_schema = PostgresOperator(
    task_id = 'Drop_etl_schema',
    dag = dag,
    postgres_conn_id = 'redshift',
    sql = SqlQueries.drop_schema_tables
)

create_staging_schema = PostgresOperator(
    task_id = 'Create_staging_tables',
    dag = dag,
    postgres_conn_id = 'redshift',
    sql = SqlQueries.create_staging_tables
)

create_dim_schema = PostgresOperator(
    task_id = 'Create_dim_tables',
    dag=dag,
    postgres_conn_id = 'redshift',
    sql = SqlQueries.create_dim_tables
)

create_fact_schema = PostgresOperator(
    task_id = 'Create_fact_tables',
    dag=dag,
    postgres_conn_id = 'redshift',
    sql = SqlQueries.create_fact_tables
)


# Create DAG flow

start_operator >> drop_etl_schema >> [create_staging_schema, create_dim_schema]

create_dim_schema >> create_fact_schema >> end_operator
create_staging_schema >> end_operator
