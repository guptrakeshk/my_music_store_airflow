from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {}
    
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 table="",
                 region="",
                 extra_params="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.region = region
        self.extra_params = extra_params
        

    def execute(self, context):
        self.log.info(" StageToRedshiftOperator has started COPY command. It may take multiple mins to finish.")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()        
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        
        s3_path = "s3://{}/{}".format(self.s3_bucket,rendered_key)
        
        # Format the SQL command
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.extra_params
        )
        self.log.info("COPY command has been initiated now. We will inform once command is finished\
                        Thanks for your patience")
        redshift.run(formatted_sql)
        
        self.log.info(" COPY command has finished successfully. ")
