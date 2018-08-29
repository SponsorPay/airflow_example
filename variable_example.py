from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
from airflow.models import Variable

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


def print_var(var, **kwargs):
    logging.info("The var content is: "+var)


var = Variable.get("example")

dag = DAG(dag_id='variable_example', default_args=default_args, schedule_interval=None, catchup=False)

print_op = PythonOperator(
    task_id='python_operator',
    provide_context=True,
    python_callable=print_var,
    op_kwargs={'var': var},
    dag=dag)

print_op
