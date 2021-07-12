from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table = "",
                 bucket="",
                 iamrole="",
                 jsonpath='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.bucket = bucket
        self.iamrole = iamrole
        self.jsonpath = jsonpath
        
        self.query = f"""
                COPY {self.table}
                FROM '{self.bucket}'
                iam_role '{self.iamrole}'
                FORMAT AS json '{self.jsonpath}'
            """

    def execute(self, context):
        self.log.info(f'Staging {self.table}')
     
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Query {self.query}')
        redshift.run(self.query)
      





