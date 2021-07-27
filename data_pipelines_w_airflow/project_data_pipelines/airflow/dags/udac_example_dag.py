from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTablesOperator)
from helpers import SqlQueries

# Get IAM role from airflow variables
iamrole = Variable.get('redshift_iamrole')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 7, 12),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTablesOperator(task_id='Create_tables', dag=dag, redshift_conn_id = 'redshift')

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    table = "staging_events",
    bucket='s3://udacity-dend/log_data',
    iamrole=iamrole,
    jsonpath='s3://udacity-dend/log_json_path.json',
    delete_table=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table = "staging_songs",
    bucket='s3://udacity-dend/song_data',
    iamrole=iamrole,
    jsonpath='auto',
    delete_table=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    dag=dag,
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    dag=dag,
    sql=SqlQueries.user_table_insert,
    delete=True,
    table='users'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    dag=dag,
    sql=SqlQueries.song_table_insert,
    delete=True,
    table='songs'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    dag=dag,
    sql=SqlQueries.artist_table_insert,
    delete=True,
    table='artists'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    sql=SqlQueries.time_table_insert,
    dag=dag,
    delete=True,
    table='time'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables
create_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator




