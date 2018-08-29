from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import logging

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


def test_minute(**kwargs):
    execution_date = kwargs['execution_date']
    execution_minute = execution_date.minute
    logging.info("minute value is: "+str(execution_minute))
    if(execution_minute % 2 == 0):
        return 'even_minute'
    else:
        return 'odd_minute'


dag = DAG(dag_id='branch_python_operator_example', default_args=default_args, schedule_interval=None, catchup=False)

branch_a = DummyOperator(task_id='even_minute', dag=dag)

branch_b = DummyOperator(task_id='odd_minute', dag=dag)

branch_python_op = BranchPythonOperator(
    task_id='branch',
    python_callable=test_minute,
    provide_context=True,
    dag=dag
)

branch_a << branch_python_op >> branch_b
