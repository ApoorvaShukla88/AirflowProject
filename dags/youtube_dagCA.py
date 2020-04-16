import boto3
import jinja2
import pdfkit
import json
from zipfile import ZipFile
import sqlalchemy
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
import matplotlib.pyplot as plt
from datetime import date
import airflow.hooks.S3_hook

country = 'CA'
root_path = '/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/'
date_formatted = str(date.today().strftime("%Y%m%d"))

template_path = root_path + 'templates/'
df_template = 'myreport.html'
chart_template = 'report_chart.html'

raw_file_path = root_path + 'raw/'
raw_file = raw_file_path + country + 'videos.csv'
agg_file_path = root_path + 'agg/'
aggregated_file = agg_file_path + country + '_aggregated.csv'
category_file_path = root_path + 'category/'
category_file = category_file_path + country + '_category_formatted.csv'
report_path = root_path + 'report/'
report_file = report_path + country + date_formatted + "_youtube_analysis.png"
html_dir = root_path + 'html/'
html_file = html_dir + country + '.html'
pdf_dir = root_path + 'pdf/'
pdf_report_file = pdf_dir + country + date_formatted + "_youtube_analysis.pdf"


def unzip_files(zipped_file):
    with ZipFile(zipped_file, 'r') as zipObj:
        zipObj.extractall(raw_file_path)


def insert_raw_data(input_file, country_code):
    engine = sqlalchemy.create_engine('postgresql+psycopg2://amishra:pass@localhost:5432/airflow_backend')
    df = pd.read_csv(input_file, delimiter=',')

    with engine.connect() as conn, conn.begin():
        df.to_sql('raw_data_'+country_code, conn, if_exists='replace')


def category_data(input_file, output_file):
    with open(input_file, 'r') as f:
        data = json.load(f)

    category_list = list()
    for c in data['items']:
        category_list.append([c['id'], c['snippet']['title']])
    df11 = pd.DataFrame(category_list, columns=['category_id', 'category_title'])
    with open(output_file, 'w') as f:
        f.write(df11.to_csv())


def agg_data(raw_data_file, category_data_file, output_file):
    df = pd.read_csv(raw_data_file)
    months = {1: "January", 2: "February",
              3: "March", 4: "April", 5: "May",
              6: "June", 7: "July", 8: "August",
              9: "September", 10: "October",
              11: "November", 12: "December"}
    df['month'] = df.apply(lambda row: months[int(row.trending_date.split(".")[2])], axis=1)
    df11 = pd.read_csv(category_data_file)
    # df.groupby('category_id').count()
    df12 = df11.astype({'category_id': int})
    merged_inner = pd.merge(df, df12, on='category_id')
    df13 = merged_inner.groupby(['month', 'category_title']).count()
    with open(output_file, 'w') as f:
        f.write(df13.to_csv())


def save_report(input_file, output_file):
    df13 = pd.read_csv(input_file)
    df14 = df13.iloc[:, 0:3]
    df14.columns.str.strip()
    fig, ax = plt.subplots(figsize=(15, 7))
    df14.groupby(['month', 'category_title']).sum()['video_id'].unstack().plot(ax=ax)
    ax.set_xlabel('Month')
    ax.set_ylabel('Number of videos of particular category')
    fig.savefig(output_file, dpi=300, bbox_inches='tight')


def make_report(input_raw_file, input_agg_file, input_html_file, input_report_file, output_pdf_file):
    templateLoader = jinja2.FileSystemLoader(searchpath=template_path)
    templateEnv = jinja2.Environment(loader=templateLoader)

    df_template_obj = templateEnv.get_template(df_template)
    chart_template_obj = templateEnv.get_template(chart_template)

    df_raw = pd.read_csv(input_raw_file, delimiter=',').head(20)
    df_agg = pd.read_csv(input_agg_file, delimiter=',').head(20)

    output_raw = df_template_obj.render(df=df_raw, title="Sample Raw Data")
    output_agg = df_template_obj.render(df=df_agg, title='Sample Aggregated Data')
    output_chart = chart_template_obj.render(report_src="\'" + input_report_file + "\'")

    with open(input_html_file, 'w') as fh:
        fh.write("<head>")
        fh.write("</head>")
        fh.write("<body>")
        fh.write(output_raw)
        fh.write(output_agg)
        fh.write(output_chart)
        fh.write("</body>")

    pdfkit.from_file(input_html_file, output_pdf_file)


# def upload_file_to_S3(filename, key, bucket_name):
#     s3 = boto3.resource('s3')
#     s3.Bucket(bucket_name).upload_file(filename, key)


def upload_file_to_S3_with_hook(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('s3_connect')
    hook.load_file(filename, key, bucket_name)

default_args = {
    'owner': 'Apoorva',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': None,
}

dag = DAG(
    'Youtube_'+ country,
    default_args=default_args,
    schedule_interval=timedelta(days=30),
    description='Youtube Trending Videos Analysis',
)

t1 = BashOperator(
    task_id='Download-From-Source',
    bash_command='/Users/amishra/opt/anaconda3/bin/kaggle datasets download datasnaek/youtube-new -p ' + raw_file_path,
    dag=dag,
)


t2 = PythonOperator(
    task_id='Unzip-Downloaded-File',
    python_callable=unzip_files,
    op_kwargs={'zipped_file': raw_file_path + 'youtube-new.zip'},
    dag=dag,
)

t3 = PythonOperator(
    task_id='Persist-Data-In-DB',
    python_callable=insert_raw_data,
    op_kwargs={'input_file': raw_file,
               'country_code': country},
    dag=dag,
)

t4 = PythonOperator(
    task_id='Format-Category-Data',
    python_callable=category_data,
    op_kwargs={'input_file': raw_file_path + country +'_category_id.json' ,
               'output_file': category_file },
    dag=dag,
)

t5 = PythonOperator(
    task_id='Aggregate-Data',
    python_callable=agg_data,
    op_kwargs={'raw_data_file': raw_file,
               'category_data_file': category_file,
               'output_file': aggregated_file},
    dag=dag,
)

t6 = PythonOperator(
    task_id='Generate-Report',
    python_callable=save_report,
    op_kwargs={'input_file': aggregated_file,
               'output_file': report_file},
    dag=dag,
)

t7 = PythonOperator(
    task_id='Generate-Report-PDF',
    python_callable=make_report,
    op_kwargs={'input_raw_file': raw_file,
               'input_agg_file': aggregated_file,
               'input_html_file': html_file,
               'input_report_file': report_file,
               'output_pdf_file': pdf_report_file},
    dag=dag,
)

t8 = PythonOperator(
    task_id='upload_to_S3',
    python_callable=upload_file_to_S3_with_hook,
    op_kwargs={
        'filename': pdf_report_file,
        'key': 'YTAnalysisCA_hook-Out.pdf',
        'bucket_name': 'apoorva.first.boto.s3.bucket'},
    dag=dag
)



t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
