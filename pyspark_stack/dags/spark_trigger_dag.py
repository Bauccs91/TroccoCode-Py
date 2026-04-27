from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}

with DAG('spark_wordcount_trigger',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['spark']) as dag:

    run_spark_job = BashOperator(
        task_id='submit_wordcount',
        bash_command="""
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/wordcount.py
"""
    )
