import argparse
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


path = '/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/'
raw_file = 'videos.csv'
country = 'IN'



def unzip_files(zipped_file):
    with ZipFile(zipped_file, 'r') as zipObj:
        zipObj.extractall(path)


def insert_raw_data(raw_data_file,country):
    engine = sqlalchemy.create_engine('postgresql+psycopg2://amishra:pass@localhost:5432/airflow_backend')
    # engine = sqlalchemy.create_engine('mysql+pymysql://root:zipcoder@localhost/testDB')
    df = pd.read_csv(raw_data_file, delimiter=',')

    with engine.connect() as conn, conn.begin():
        df.to_sql('raw_data_'+country, conn, if_exists='replace')


def category_data(category_file,country):
    data = None
    with open(category_file,'r') as f:
        data = json.load(f)

    category_list = list()
    for c in data['items']:
        category_list.append([c['id'], c['snippet']['title']])
    df11 = pd.DataFrame(category_list, columns=['category_id', 'category_title'])
    with open(path + 'dataclean/' + country + '_category_formatted.csv','w') as f:
        f.write(df11.to_csv())


def agg_data(raw_data_file, formatted_file, country):
    df = pd.read_csv(raw_data_file)
    months = {1: "January", 2: "February",
              3: "March", 4: "April", 5: "May",
              6: "June", 7: "July", 8: "August",
              9: "September", 10: "October",
              11: "November", 12: "December"}
    df['month'] = df.apply(lambda row: months[int(row.trending_date.split(".")[2])], axis=1)
    df11 = pd.read_csv(formatted_file)
    # df.groupby('category_id').count()
    df12 = df11.astype({'category_id': int})
    merged_inner = pd.merge(df, df12, on='category_id')
    df13 = merged_inner.groupby(['month', 'category_title']).count()
    with open(path + 'dataclean/' + country + '_aggregated.csv','w') as f:
        f.write(df13.to_csv())


def save_report(aggregated_file, report_dir, country):
    df13 = pd.read_csv(aggregated_file)
    df14 = df13.iloc[:, 0:3]
    df14.columns.str.strip()
    fig, ax = plt.subplots(figsize=(15, 7))
    df14.groupby(['month', 'category_title']).sum()['video_id'].unstack().plot(ax=ax)
    ax.set_xlabel('Month')
    ax.set_ylabel('Number of videos of particular category')
    report_name = report_dir + "/youtube_analysis_" + country + "_" + str(date.today().strftime("%Y%m%d"))
    report_for_2018 = fig.savefig(report_name, dpi=300, bbox_inches='tight')


default_args = {
    'owner': 'Apoorva',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
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
    task_id='download_from_source',
    bash_command='/Users/amishra/opt/anaconda3/bin/kaggle datasets download datasnaek/youtube-new -p /Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow',
    dag=dag,
)


t2 = PythonOperator(
    task_id='Unzip-downloaded-file',
    python_callable=unzip_files,
    op_kwargs={'zipped_file': path+'youtube-new.zip'},
    dag=dag,
)

t3 = PythonOperator(
    task_id='Persist_data_In_DB',
    python_callable=insert_raw_data,
    op_kwargs={'raw_data_file': path+country+raw_file,
               'country': country},
    dag=dag,
)

t4 = PythonOperator(
    task_id='CombiningDataForAnalysis',
    python_callable=category_data,
    op_kwargs={'category_file': path + country +'_category_id.json' ,
               'country': country },
    dag=dag,
)

t5 = PythonOperator(
    task_id='Aggregate_data',
    python_callable=agg_data,
    op_kwargs={'raw_data_file': path+ country + raw_file,'formatted_file':
        path +'dataclean/' + country + '_category_formatted.csv' ,
               'country': country },
    dag=dag,
)

t6 = PythonOperator(
    task_id='SaveReport',
    python_callable=save_report,
    op_kwargs={'aggregated_file': path + 'dataclean/' + country + '_aggregated.csv',
               'report_dir' : path + 'dataclean/',
               'country' : country },
    dag=dag,
)

t7 = DummyOperator(
    task_id='SendReportViaEmail',
    dag=dag,

)


t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
