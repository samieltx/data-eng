from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

from tienda_amiga.extract import get_news_from_api
from tienda_amiga.load import load_to_postgres
from tienda_amiga.transform_jct import transform

with DAG(    
        dag_id='logging_dag',
        start_date=datetime(2023, 5, 16),
        #schedule_interval=None, 
        schedule_interval='0 4 * * *'
    ) as dag:

    
    dummy_start_task = DummyOperator(task_id="dummy_start")

    def _print_execution_date(ds):
        print(f"The execution date of this flow is {ds}")

    print_dag = PythonOperator(
        task_id='print_task',
        python_callable=_print_execution_date,
        dag=dag,
    )

    dummy_end_task = DummyOperator(task_id="dummy_end")

    dummy_start_task >>  print_dag >> dummy_end_task