from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id= "",
                 sql="",
                 table="",
                 delete=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.sql=sql
        self.table=table
        self.delete=delete

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(redshift)
        self.log.info(f'Inserting values into {self.table}')
        self.log.info(self.sql)
        if self.delete:
            redshift.run(f"DELETE FROM {self.table}")
        
        redshift.run(self.sql)
        self.log.info('Finished inserting')
