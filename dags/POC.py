from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from convert_2_orc_poc import process_fun
from file_transfer import file_transfer
from download_file import download_file




yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('POC',default_args=default_args,schedule_interval='@once', template_searchpath=['/usr/local/airflow/store_file_psql'], catchup=True) as dag:

    t00=PythonOperator(task_id="download_file", python_callable=download_file)

    # t0=PythonOperator(task_id="move_file", python_callable=move_file)


    t1=BashOperator(task_id='check_file_exists', bash_command='shasum ~/store_file_psql/iris.csv', retries=1, retry_delay=timedelta(seconds=15))
    
    # t1 = FileSensor( task_id= "check_file_exists", poke_interval= 0, fs_conn_id= 'fs_default', filepath= '/home/janushee/airflow/store_files/iris.csv' ,soft_fail=True,timeout=5)

    t2 = PostgresOperator(
    task_id='drop_if_table_exist',
    postgres_conn_id= 'postgres_conn',
    sql= """
        DROP TABLE IF EXISTS iris
        """, 
    provide_context=True)

    t3 = PostgresOperator(
    task_id='create_table',
    postgres_conn_id= 'postgres_conn',
    sql= """
        CREATE TABLE IF NOT EXISTS iris(
        iris_sepal_length VARCHAR(5),
        iris_sepal_width VARCHAR(5),
        iris_petal_length VARCHAR(5),
        iris_petal_width VARCHAR(5),
        iris_variety VARCHAR(16))
        """, 
    provide_context=True)

    t4 = PostgresOperator(
    task_id='copy_data',
    postgres_conn_id= 'postgres_conn',
    sql= """COPY iris FROM '/store_file_psql/iris.csv' DELIMITER ',' CSV HEADER;""", 
    provide_context=True)

    t5 = PythonOperator(task_id='processing_through_python', python_callable=process_fun)
    t6 = PythonOperator(task_id="load_to_s3", python_callable=file_transfer)

    t00 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6
