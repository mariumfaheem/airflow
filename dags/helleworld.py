
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import variable

#Hadopp , s3 ,
# Dummy


def helloWorld():
    return "Hello World"

default_args = {
    "owner": "DataTeams",
    "depends_on_past": True,
    "start_date": days_ago(1),
    "provide_context": True,

    #Backfilling - > 2011, 2012 ,2013
    "retries": 3,
    "retry_delay": 60,
    "retry_exponential_backoff": True,
    "max_retry_delay": 3600

}

with DAG("PythonOperatorHelloWorld", default_args=default_args, schedule_interval='0 */2 * * *', concurrency = 5, max_active_runs = 1, catchup = False, tags=["test"]) as dag:

    task = PythonOperator(
        task_id="hello_world",
        python_callable=TrackRider, ##it take python function name
        provide_context=False) #provide_context =

    
    start = DummyOperator(
        task_id='start',
        dag=dag)


    end = DummyOperator(
        task_id='end',
        dag=dag
    )
    start >> task >> end





