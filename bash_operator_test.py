from builtins import range
from datetime import timedelta

import airflow
from airflow.models import DAG , Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


from airflow.operators.dummy_operator import DummyOperator
import pendulum

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='example_bash_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

task = BashOperator(
    task_id='task1',
    bash_command='echo "{{ task_instance_key_str }} Now {{ execution_date }}  Prev {{ var.value.last_execution_date_succes }} " && sleep 1',
    dag=dag,
    )

task2 = BashOperator(
    task_id='task2',
    bash_command='echo "{{ task_instance_key_str }} Now {{ execution_date }} Prec {{ var.value.last_execution_date_succes }} " && sleep 1',
    dag=dag,
)

def setEndTime():
    Variable.set("last_execution_date_succes", "{{ execution_date }}")

task3 = PythonOperator(task_id='add_columns_values',
                    python_callable=setEndTime,
                    dag=dag)

task >> task2 >> task3

if __name__ == "__main__":
    dag.cli()