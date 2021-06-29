from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = "",
                 delete_and_load = False,
                 table = "",
                 sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.delete_and_load = delete_and_load
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info(" LoadDimensionOperator is initiating SQL command to load dimension data \
                        It may take multiple seconds to finish ")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Generate dynamic formatted SQL
        formatted_sql = self.sql
        # Check whether table data needs to be deleted first
        if self.delete_and_load :
            formatted_sql = f"""
                    DELETE FROM {self.table};
                    {self.sql} """
        redshift.run(formatted_sql)
        self.log.info(" LoadDimensionOperator has finished loading dimension data ")
        