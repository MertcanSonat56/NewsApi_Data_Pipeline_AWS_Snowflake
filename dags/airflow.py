from datetime import datetime, timedelta
import logging
import airflow 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.bash import BashOperator
from news_etl import runner
from airflow.utils.dates import days_ago

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


args = {"owner": "Airflow", "start_date": days_ago(2)}

dag = DAG( dag_id="snowflake_automation_dag", default_args=args, schedule_interval=None)


with dag:

    extract_news_info = PythonOperator(
        task_id = 'extract_news_info',
        python_callable= runner,
        dag=dag,
    )

    move_file_to_s3 = BashOperator(
        task_id="move_file_to_s3",
        bash_command='aws s3 mv {{ ti.xcom_pull ("extract_news_info")}} s3://myirisseta',
    )

    snowflake_create_table = SnowflakeOperator(
        task_id = "snowflake_create_table",
        sql="""create table if not exists helloparquent using template(select 
        ARRAY_AGG(OBJECT_CONSTRUCT(*)) from TABLE(INFER_SCHEMA (LOCATION => '@ramu.PUBLIC.snow_simple', FILE_FORMAT=>'parquet_format')))
        """,
        snowflake_conn_id="snowflake_conn"
    )

    snowflake_copy = SnowflakeOperator(
        task_id = "snowflake_copy",
        sql="""copy into ramu.PUBLIC.helloparquet from @ramu.PUBLIC.snow_simple
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE FILE_FORMAT=parquet_format
        """,
        snowflake_conn_id="snowflake_conn"
    )


extract_news_info >> move_file_to_s3 >> snowflake_create_table >> snowflake_copy




















