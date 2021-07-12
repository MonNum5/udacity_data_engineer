from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os

class CreateTablesOperator(BaseOperator):
    ui_color = '#358140'
    
    staging_events_table_drop = "DROP TABLE IF EXISTS public.staging_events"
    staging_songs_table_drop = "DROP TABLE IF EXISTS public.staging_songs"
    songplay_table_drop = "DROP TABLE IF EXISTS public.songplays"
    user_table_drop = "DROP TABLE IF EXISTS public.users"
    song_table_drop = "DROP TABLE IF EXISTS public.songs"
    artist_table_drop = "DROP TABLE IF EXISTS public.artists"
    time_table_drop = "DROP TABLE IF EXISTS public.time"

    drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,                             user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Drop tables")
        for sql in CreateTablesOperator.drop_table_queries:
            redshift.run(sql)
        
        self.log.info("Create tables")
        
        #read sql
        f = open("/home/workspace/airflow/create_tables.sql", "r")
        sql = f.read()
        redshift.run(sql)
        self.log.info("Tables created")
        





