from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
import calendar

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 10),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def example_callable(example_string, **kwargs):
    logging.info("What is it? - "+example_string)
    execution_date = kwargs['execution_date']   # <----- using the context
    logging.info("execution date day of the week is "+calendar.day_name[execution_date.weekday()])


dag = DAG(dag_id='python_operator_example', default_args=default_args, schedule_interval=None, catchup=False)

python_op = PythonOperator(
    task_id='python_operator',
    provide_context=True,
    python_callable=example_callable,
    op_kwargs={'example_string': 'paramater I passed'},
    dag=dag)

python_op
