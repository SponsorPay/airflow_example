from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

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

dag = DAG(dag_id='dag_example', default_args=default_args, schedule_interval=None, catchup=False)

op_1 = DummyOperator(task_id='op_1', dag=dag)
op_2 = DummyOperator(task_id='op_2', dag=dag)
op_3 = DummyOperator(task_id='op_3', dag=dag)
op_4 = DummyOperator(task_id='op_4', dag=dag)
op_5 = DummyOperator(task_id='op_5', dag=dag)

op_1 >> op_3 << op_2
op_4 << op_3 >> op_5

# the '>>' & '<<' defining the streamself.
# same as set_downstream() set_upstream() functions.
