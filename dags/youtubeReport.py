from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import papermill as pm
import boto3


def jupyter_output_report():
    pm.execute_notebook(
        '/Users/amishra/DEV/AirflowProject/YTAnalysis.ipynb',
        '/Users/amishra/DEV/AirflowProject/YTAnalysis-Out.ipynb',
        parameters={'root_path': '/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/'}
    )


def upload_file_to_S3(filename, key, bucket_name):
    s3 = boto3.resource('s3')
    s3.Bucket(bucket_name).upload_file(filename, key)



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
    task_id='ConvertReportToPDF',
    bash_command='export PATH="$PATH:/usr/local/texlive/2020/bin/x86_64-darwin" && jupyter nbconvert --to pdf --output-dir /Users/amishra/DEV/AirflowProject /Users/amishra/DEV/AirflowProject/YTAnalysis-Out.ipynb',
    dag=dag,
)


t3 = PythonOperator(
    task_id='upload_to_S3',
    python_callable=upload_file_to_S3,
    op_kwargs={
        'filename': '/Users/amishra/DEV/AirflowProject/YTAnalysis-Out.pdf',
        'key': 'YTAnalysis-Out.pdf',
        'bucket_name': 'apoorva.first.boto.s3.bucket'},
    dag=dag
)




t1 >> t2 >> t3