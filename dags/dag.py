from datetime import datetime, timedelta
import os
import configparser
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimensions import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from dags.sql_queries import SqlQueries

# Read configuration file
config = configparser.ConfigParser()
config.read('dwh.cfg')

# AWS and Redshift configuration
aws_credentials_id = 'aws_credentials'
redshift_conn_id = 'redshift'
s3_bucket = config.get('S3', 'BUCKET_NAME')
s3_song_data_key = config.get('S3', 'SONG_DATA')
s3_log_data_key = config.get('S3', 'LOG_DATA')
s3_log_jsonpath_key = config.get('S3', 'LOG_JSONPATH')

# Default arguments for the DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2023, 5, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False
}

# Create the DAG
dag = DAG(
    'sparkify_data_pipeline',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)

# Dummy start operator
start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

# Create tables in Redshift
create_tables_sql = os.path.join(os.path.dirname(__file__), 'create_tables.sql')
with open(create_tables_sql, 'r') as f:
    create_tables_query = f.read()

create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    postgres_conn_id=redshift_conn_id,
    sql=create_tables_query
)

# Stage events data from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    aws_credentials_id=aws_credentials_id,
    table='staging_events',
    s3_path=f's3://{s3_bucket}/{s3_log_data_key}',
    json_path=f's3://{s3_bucket}/{s3_log_jsonpath_key}'
)

# Stage songs data from S3 to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    aws_credentials_id=aws_credentials_id,
    table='staging_songs',
    s3_path=f's3://{s3_bucket}/{s3_song_data_key}'
)

# Load songplays fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table='songplays',
    sql=SqlQueries.songplay_table_insert
)

# Load users dimension table
load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table='users',
    sql=SqlQueries.user_table_insert
)

# Load songs dimension table
load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table='songs',
    sql=SqlQueries.song_table_insert
)

# Load artists dimension table
load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table='artists',
    sql=SqlQueries.artist_table_insert
)

# Load time dimension table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table='time',
    sql=SqlQueries.time_table_insert
)

# Run data quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

# Dummy end operator
end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

# Define task dependencies
start_operator >> create_tables
create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_users_dimension_table, load_songs_dimension_table,
                         load_artists_dimension_table, load_time_dimension_table]
[load_users_dimension_table, load_songs_dimension_table,
 load_artists_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator