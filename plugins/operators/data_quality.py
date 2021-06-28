from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 #table="",
                 #columns_list=[],
                 table_dict = {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        #self.table = table
        #self.columns_list = columns_list
        self.table_dict = table_dict

    '''
    def check_records_found(self, redshift_hook):
        records = redshift_hook.get_records(f" SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f" Data quality check failed. {self.table} returned no records")
        
        num_records = records[0][0]
        if num_records < 1 :
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
            
        self.log.info(f"Data load quality passed. Table {self.table} returned {num_records} rows")
        
    def check_for_null(self, redshift_hook, not_null_columns_list):
        """
            Function checks data quality by ensuring any NOT NULL fields do not have any NULL value
        """
        if not_null_columns_list :
            formatted_tuple = tuple(not_null_columns_list)
            records = redshift_hook.get_records(f" SELECT COUNT(*) FROM {self.table} WHERE  {formatted_tuple} IS NULL ")

            num_records = records[0][0]
            if num_records > 0 :
                raise ValueError(f"Data quality check failed. Not Null columns contained null values")

            self.log.info(f"Null Data check passed. There are not NULL values into NOT_NULL columns")
    '''    
    def check_records_found(self, redshift_hook, table):
        """
            Function to check if there are more than zero records in the table after processing
        """
        records = redshift_hook.get_records(f" SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f" Data quality check failed. {table} returned no records")
        
        num_records = records[0][0]
        if num_records < 1 :
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            
        self.log.info(f"Data load quality passed. Table {table} returned {num_records} rows")
    
    
    def check_for_null(self, redshift_hook, table, not_null_columns_list):
        """
            Function checks data quality by ensuring any NOT NULL fields do not have any NULL value
        """
        if not_null_columns_list :
            formatted_tuple = ",".join(not_null_columns_list)
            records = redshift_hook.get_records(f" SELECT COUNT(*) FROM {table} WHERE  ({formatted_tuple}) IS NULL ")

            num_records = records[0][0]
            if num_records > 0 :
                raise ValueError(f"Data quality check failed. Not Null columns contained null values")

            self.log.info(f"Null Data check passed. There are not NULL values into NOT_NULL columns")
            
        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        for table, columns_list in self.table_dict.items() :
        
            # Check for records in the destination table after processing.
            self.check_records_found(redshift_hook, table)

            # Check for records if there are any NULL for not null columns.
            self.check_for_null(redshift_hook, table, columns_list)

        
        