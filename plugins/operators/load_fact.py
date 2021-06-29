from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = "",
                 sql = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql


    def execute(self, context):
        self.log.info(" LoadFactOperator is initiating SQL command to load facts. It may take multiple seconds to finish.")
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        redshift.run(self.sql)
        self.log.info(" LoadFactOperator has finished loading facts.")
        