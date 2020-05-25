from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'lcf',
    'start_date': datetime(2020, 5, 15),
    'depends_on_past': False,
    'catchup': False
}

dag = DAG('scoreindicators_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="lcf-udacity-de-bucket",
    s3_key="data/",
    delimiter=","
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="lcf-udacity-de-bucket",
    s3_key="data/happiness/",
    delimiter=","
)

load_score_fact_table = LoadFactOperator(
    task_id='Load_score_fact_table',
    dag=dag,
    conn_id="redshift",
    table="fact_score",
    sql=SqlQueries.score_table_insert
)

load_country_dimension_table = LoadDimensionOperator(
    task_id='Load_country_dim_table',
    dag=dag,
    conn_id="redshift",
    table="dim_country",
    sql=SqlQueries.country_table_insert
)

load_indicator_dimension_table = LoadDimensionOperator(
    task_id='Load_indicator_dim_table',
    dag=dag,
    conn_id="redshift",
    table="dim_indicator",
    sql=SqlQueries.indicator_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id="redshift",
    table="dim_time",
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id="redshift",
    tables=["fact_score","dim_country","dim_indicator","dim_time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_score_table
load_score_table >> load_country_dimension_table >> run_quality_checks
load_score_table >> load_indicator_dimension_table >> run_quality_checks
load_score_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
