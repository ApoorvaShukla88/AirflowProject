import jinja2
import pdfkit
import json
from zipfile import ZipFile
import sqlalchemy
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.papermill_operator import PapermillOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
import matplotlib.pyplot as plt
from datetime import date
import papermill as pm


def jupyter_output_report():
    pm.execute_notebook(
        '/Users/amishra/DEV/AirflowProject/YTAnalysis.ipynb',
        '/Users/amishra/DEV/AirflowProject/YTAnalysis-Out.ipynb',
        parameters={'root_path': '/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/'}
    )


default_args = {
    'owner': 'Apoorva',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': None,
}

dag = DAG(
    'Youtube_Report',
    default_args=default_args,
    schedule_interval=timedelta(days=30),
    description='Youtube Trending Videos Analysis',
)



t1 = PythonOperator(
    task_id='GenerateFullReport',
    python_callable=jupyter_output_report,
    dag=dag,
)

t2 = BashOperator(
    task_id='ConvertReporttoHTML',
    bash_command='export PATH="$PATH:/usr/local/texlive/2020/bin/x86_64-darwin" && jupyter nbconvert --to pdf --output-dir /Users/amishra/DEV/AirflowProject /Users/amishra/DEV/AirflowProject/YTAnalysis-Out.ipynb',
    dag=dag,
)

t1 >> t2