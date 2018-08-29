from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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


def push_to_xcom(**kwargs):
    kwargs['ti'].xcom_push(key='A', value='A value')
    kwargs['ti'].xcom_push(key='B', value='B value')
    return 'C value'


def pull_from_xcom(**kwargs):
    a_value = kwargs['ti'].xcom_pull(dag_id='xcom_push_example', task_ids='xcom_pusher', key='A')
    logging.info("A value: "+a_value)
    b_value = kwargs['ti'].xcom_pull(dag_id='xcom_push_example', task_ids='xcom_pusher', key='B')
    logging.info("B value: "+b_value)
    c_value = kwargs['ti'].xcom_pull(dag_id='xcom_push_example', task_ids='xcom_pusher')
    logging.info("C value: "+c_value)


push_dag = DAG(dag_id='xcom_push_example', default_args=default_args, schedule_interval=None, catchup=False)

pusher = PythonOperator(
    task_id='xcom_pusher',
    provide_context=True,
    python_callable=push_to_xcom,
    dag=push_dag)

pusher

pull_dag = DAG(dag_id='xcom_pull_example', default_args=default_args, schedule_interval=None, catchup=False)

puller = PythonOperator(
    task_id='xcom_puller',
    provide_context=True,
    python_callable=pull_from_xcom,
    dag=pull_dag)

puller
