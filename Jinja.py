from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

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

dag = DAG(dag_id='jinja_example', default_args=default_args, schedule_interval=None, catchup=False)

templated_cmd = """
    {% for key, value in params.items() %}
    echo "{{ key }}"
    echo "{{ value }}"
    {% endfor %}
"""

# the jinja in the bash command has access to the params.

op = BashOperator(
    task_id='print_params',
    bash_command=templated_cmd,
    params={'Hey': 'There', """I'm using""": 'Jinja'},
    dag=dag)
op
