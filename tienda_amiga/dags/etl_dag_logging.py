from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

from tienda_amiga.extract import get_news_from_api
from tienda_amiga.load import load_to_postgres
from tienda_amiga.transform_jct import transform


def test_function(**context):
    print(f"Execution date is {context['ds']}")
    print(f"DAG ID is {context['dag'].dag_id}")        
    return context['ds']

with DAG(    
        dag_id='logging_dag',
        start_date=datetime(2023, 5, 16),
        #schedule_interval=None, 
        schedule_interval='0 4 * * *'
    ) as dag:

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=test_function,
        provide_context=True,        
        dag=dag
    )
    dummy_start_task = DummyOperator(task_id="dummy_start")

    def _print_execution_date(ds):
        print(f"The execution date of this flow is {ds}")

    print_dag = PythonOperator(
        task_id='print_task',
        python_callable=_print_execution_date,
        dag=dag,
    )

    dummy_end_task = DummyOperator(task_id="dummy_end")

    dummy_start_task >>  print_dag >> python_task >> dummy_end_task