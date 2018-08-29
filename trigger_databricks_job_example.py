from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.sensors import HttpSensor
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
import json
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


def extract_run_id(**kwargs):
    response_str = kwargs['ti'].xcom_pull(task_ids='http_post_to_databricks')
    return json.loads(response_str)['run_id']


def check_state(response):
    def is_failed(content):
        return content['state']['life_cycle_state'] in ('TERMINATED', 'SKIPPED', 'INTERNAL_ERROR')

    def is_success(content):
            return content['state'].get('result_state') == 'SUCCESS'

    content = json.loads(response.content)
    logging.info('Job state is: {0}'.format(content['state']))

    if is_success(content):
        return True
    elif is_failed(content):
        raise AirflowException('Job Failed, Response: {0}, Status Code: {1}'.format(content, response.status_code))
    else:
        return False


def extract_result(**kwargs):
    response_str = kwargs['ti'].xcom_pull(task_ids='http_get_to_databricks')
    return json.loads(response_str)['notebook_output'].get('result')


job_param_dict = {'job_id': '178', 'notebook_params': {'message': "Hey there mate"}}
timeout = 20 * 60

# notice the using of jinja template & XCom in the sensor and the following http operator.

dag = DAG(dag_id='trigger_databricks_job_example', default_args=default_args, schedule_interval=None, catchup=False)

trigger_job_http_op = SimpleHttpOperator(
    task_id='http_post_to_databricks',
    http_conn_id='databricks',
    endpoint='/api/2.0/jobs/run-now',
    method='POST',
    headers={'Content-Type': 'application/json'},
    data=json.dumps(job_param_dict),
    xcom_push=True,
    response_check=lambda response: response.json().get('run_id') is not None,
    dag=dag)

run_id_extractor = PythonOperator(
    task_id='extract_run_id',
    provide_context=True,
    python_callable=extract_run_id,
    dag=dag)

state_http_sensor = HttpSensor(
    task_id='sensor_job_state',
    http_conn_id='databricks',
    timeout=timeout,
    method='GET',
    endpoint='/api/2.0/jobs/runs/get',
    request_params={'run_id': """{{ ti.xcom_pull(task_ids='extract_run_id') }}"""},
    response_check=check_state,
    poke_interval=30,
    dag=dag)

fetch_result_http_op = SimpleHttpOperator(
    task_id='http_get_to_databricks',
    http_conn_id='databricks',
    method='GET',
    data={'run_id': """{{ ti.xcom_pull(task_ids='extract_run_id') }}"""},
    endpoint='/api/2.0/jobs/runs/get-output',
    xcom_push=True,
    response_check=lambda response: response.json()['metadata']['state'].get('result_state') == 'SUCCESS',
    dag=dag)

result_extractor = PythonOperator(
    task_id='extract_result',
    provide_context=True,
    python_callable=extract_result,
    dag=dag)

trigger_job_http_op >> run_id_extractor >> state_http_sensor >> fetch_result_http_op >> result_extractor
