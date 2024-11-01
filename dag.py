from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator

default_args = {
    'owner':'airflow',
    'start_date': datetime.today(),
    'depends_on_past':False,
    'email':['my@mail.com'], # I masked my email to push safly to remote repo
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

dag = DAG('extract_employee_data',
          default_args=default_args,
          description = 'Runs a python extract script',
          schedule_interval = '@daily',
          catchup = False)

with dag:
    run_script_task = BashOperator(
        task_id = 'extract_data',
        bash_command = 'python /home/airflow/gcs/dags/scripts/extract.py'
    )
    start_pipeline = CloudDataFusionStartPipelineOperator(
    location="us-west1",
    pipeline_name="try_etl",
    instance_name="datafusion-dev",
    pipeline_timeout=1000,
    task_id="start_datafusion_dev",
    )
    run_script_task>>start_pipeline