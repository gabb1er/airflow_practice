import os
from datetime import datetime
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator

BASE_ENDPOINT = 'CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'


def convert_date(dt):
    return dt.strftime('%m-%d-%Y')


def download_data(**kwargs):
    exec_date = kwargs['templates_dict']['exec_date']
    conn = BaseHook.get_connection(kwargs['conn_id'])
    url = conn.host + kwargs['base_endpoint'] + exec_date + '.csv'
    logging.info(f"Sending GET request to {url}...")
    resp = requests.get(url)
    if resp.status_code == 200:
        save_path = f'/opt/airflow/dags/files/{exec_date}.csv'
        logging.info(f'Request successful. Saving data to {save_path}')
        with open(save_path, 'w') as f:
            f.write(resp.text)
        logging.info(f"Data saved successfully.")


default_args = {
    'owner': 'Airflow',
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'some-user@some-domain.com',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 60,  # seconds
    'start_date': datetime(2022, 4, 15),
    'end_date': datetime(2022, 4, 20)
}

with DAG(
    dag_id='sample_dag',
    default_args=default_args,
    schedule_interval='0 7 * * *',
    description='Даг для примера',
    max_active_runs=2,
    concurrency=4,
    user_defined_macros={
        'convert_date': convert_date
    }
) as main_dag:

    EXEC_DATE = '{{ convert_date(execution_date) }}'

    is_covid_data_availabe = HttpSensor(
        task_id='is_covid_data_availabe',
        http_conn_id='covid_api',
        method='GET',
        endpoint=f'{BASE_ENDPOINT}{EXEC_DATE}.csv',
        poke_interval=60,
        timeout=300,
        mode='reschedule',
        soft_fail=False
    )

    download_covid_data = PythonOperator(
        task_id='download_covid_data',
        python_callable=download_data,
        op_kwargs={
            'conn_id': 'covid_api',
            'base_endpoint': BASE_ENDPOINT,
        },
        templates_dict={
            'exec_date': EXEC_DATE
        }
    )

    # TODO: Let's do some coding here:
    """
    ЗАДАЧА:
        Написать bash-команды, чтобы сделать следующее:
            1) создать директорию в HDFS, если она не существует: /covid_data/csv
            2) Скопировать в эту директорию полученный CSV файл.
                save_path = f'/opt/airflow/dags/files/{exec_date}.csv'
            3) Удалить CSV файл на локальной машине.
    """
    move_to_hdfs = BashOperator(
        task_id='move_to_hdfs',
        bash_command="""
            hdfs dfs -mkdir -p /covid_data/csv
            hdfs dfs -copyFromLocal /opt/airflow/dags/files/{0}.csv /covid_data/csv
            rm /opt/airflow/dags/files/{0}.csv
        """.format(EXEC_DATE)
    )

    process_data = SparkSubmitOperator(
        task_id='process_data',
        application=os.path.join(os.path.dirname(__file__), 'scripts/sample_data_processing.py'),
        conn_id='spark_conn',
        name=f'{main_dag.dag_id}_process_data',
        application_args=[
            '--exec_date', EXEC_DATE
        ]
    )

    # TODO: Let's do some coding here:
    """
    ЗАДАЧА:
        Написать HQL скрипт для создания UNMANAGED таблицы covid_results в схеме default.
        Скрипт запускается каждый день, но создавать таблицу нужно только 1 раз.
        Добавить команду, чтобы "подхватить" новые данные. 
        Формат:         CSV (разделитель - запятая)
        Путь в HDFS:    /covid_data/results
        Поля таблицы:
            country_region STRING,
            total_confirmed INT,
            total_deaths INT,
            fatality_ratio DOUBLE,
            world_case_pct DOUBLE,
            world_death_pct DOUBLE
        Поле партиционирования: exec_date
    """
    load_to_hive = HiveOperator(
        task_id='load_to_hive',
        hive_cli_conn_id='hive_conn',
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS default.covid_results(
                country_region STRING,
                total_confirmed INT,
                total_deaths INT,
                fatality_ratio DOUBLE,
                world_case_pct DOUBLE,
                world_death_pct DOUBLE
            )
            PARTITIONED BY (exec_date STRING)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION '/covid_data/results';
            MSCK REPAIR TABLE default.covid_results;
        """
    )

    is_covid_data_availabe >> download_covid_data >> move_to_hdfs >> process_data >> load_to_hive
