from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        xxx = 0
        self.dq_checks=[
                    {'check_sql':'SELECT COUNT(*) FROM users' , 'expected_result': 104},
                    {'check_sql': 'SELECT COUNT(*) FROM time', 'expected_result':6820 },
                    {'check_sql':'SELECT COUNT(*) FROM artists' , 'expected_result': 10025},
                    {'check_sql': 'SELECT COUNT(*) FROM songs', 'expected_result':14896 },
                    ]

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Starting quality check')
        
        for check in self.dq_checks:

            query = check['check_sql']
            records = redshift.get_records(query)
            self.log.info(records)
            if records[0][0] != check['expected_result']:
                self.log.error(f"Quality check failed for query: {query}")
                raise ValueError(f"Quality check failed for query: {query}")
            else:
                self.log.info(f"Quality check passed for query: {query}")