from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

load_dotenv()


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
        task_id = 'run_script',
        bash_command = 'python /home/airflow/gcs/dags/scripts/extract.py'
    )