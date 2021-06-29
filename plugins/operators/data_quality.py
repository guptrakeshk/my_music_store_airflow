from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import operator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 data_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.data_checks = data_checks

    
    def records_found_check(self, redshift_hook, table, sql, exp_result, comp_ops):
        """
            Function to check if there are more than zero records in the table after processing.
            The flexibility of records check is contextual to comparison operator.
            
            :param redshift_hook: A redshift hook object
            :param table : A redshift table that needs data check
            :param sql : SQL statement that needs to be executed to fetch the records
            :param exp_result: The exepected result that is needed for data quality check
            :param comp_ops : Comparison operator that will be used for assertion.
        """
        records = redshift_hook.get_records(sql)
        num_records = records[0][0]
        
        
        if comp_ops is None :
            if operator.gt( num_records, exp_result) :
                raise ValueError(f" Data load quality check failed. '{table}' table has {num_records} NULL records\
                for NOT NULLABLE fields")
            else :
                self.log.info(f"Data load quality check passed.'{table}' table has {num_records} NULL records\
                for NOT NULLABLE fields")
        elif comp_ops :
            if comp_ops == '>':
                if operator.gt( len(records), exp_result) or operator.gt( len(records[0]) , exp_result) :
                    self.log.info(f"Data load quality check passed. '{table}' table returned more than {exp_result} rows")
                else :
                    raise ValueError(f" Data load quality check failed. '{table}' table returned no records")
        else :
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f" Data quality check failed. Table '{table}' returned no records")
            self.log.info(f"Data load quality passed")
        
        
        
    def execute(self, context):
        self.log.info(" DataQualityOperator has started checking data integrity. It may take multiple seconds to finish.")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        for test_case in self.data_checks:
            # Fetch the items from check dictionary
            table = test_case.get('table')
            test_sql = test_case.get('test_sql')
            exp_result = test_case.get('expected_result')
            comparison_ops = test_case.get('comparison')
            self.log.info(f"Starting data checks now for table '{table}' ")
            
            self.records_found_check(redshift_hook, table, test_sql, exp_result, comparison_ops)
            
        
        self.log.info(" Data quality checks have finished now. ")
            