import json
# import calendar
import sqlalchemy
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import matplotlib.pyplot as plt
from datetime import date
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def insert_raw_data():
    engine = sqlalchemy.create_engine('mysql+pymysql://root:zipcoder@localhost/testDB')
    df = pd.read_csv("/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/youtube-new/USvideos.csv",
                     delimiter=',')

    with engine.connect() as conn, conn.begin():
        df.to_sql('raw_data', conn, if_exists='replace')


def category_data():
    data = None
    with open('/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/youtube-new/US_category_id.json',
              'r') as f:
        data = json.load(f)

    category_list = list()
    for c in data['items']:
        category_list.append([c['id'], c['snippet']['title']])

    df11 = pd.DataFrame(category_list, columns=['category_id', 'category_title'])
    with open(
            '/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/dataclean/US_category_formatted.csv',
            'w') as f:
        f.write(df11.to_csv())


def agg_data():
    df = pd.read_csv('/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/youtube-new/USvideos.csv')
    months = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June", 7: "July", 8: "August",
              9: "September", 10: "October", 11: "November", 12: "December"}
    df['month'] = df.apply(lambda row: months[int(row.trending_date.split(".")[2])], axis=1)
    df11 = pd.read_csv(
        '/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/dataclean/US_category_formatted.csv')
    # df.groupby('category_id').count()
    df12 = df11.astype({'category_id': int})
    merged_inner = pd.merge(df, df12, on='category_id')
    df13 = merged_inner.groupby(['month', 'category_title']).count()
    with open(
            '/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/dataclean/USvideos_aggregated.csv',
            'w') as f:
        f.write(df13.to_csv())


def save_report():
    df13 = pd.read_csv(
        '/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/dataclean/USvideos_aggregated.csv',
        encoding='utf-8-sig')
    df14 = df13.iloc[:, 0:3]
    df14.columns.str.strip()
    fig, ax = plt.subplots(figsize=(15, 7))
    df14.groupby(['month', 'category_title']).sum()['video_id'].unstack().plot(ax=ax)
    ax.set_xlabel('Month')
    ax.set_ylabel('Number of videos of particular category')
    report_name = "/Users/amishra/DEV/AirflowProject/youtube_analysis_" + str(date.today().strftime("%Y%m%d"))
    report_for_2018 = fig.savefig(report_name, dpi=300, bbox_inches='tight')


# def send_report():
#     fromaddr = "apoorvashukla88@gmail.com"
#     toaddr = "apoorvashukla88@gmail.com"
#     msg = MIMEMultipart()
#     msg['From'] = fromaddr
#     msg['To'] = toaddr
#     msg['Subject'] = "Report :Youtube Trending videos wrt Month "
#     body = "PFA"
#     msg.attach(MIMEText(body, 'plain'))
#     report_name = "/Users/amishra/DEV/AirflowProject/youtube_analysis_" + str(date.today().strftime("%Y%m%d"))
#     filename = report_name
#     attachment = open(report_name, "rb")
#     s = smtplib.SMTP('smtp.gmail.com', 587)
#     s.starttls()
#     s.login(fromaddr, "Password_of_the_sender")
#     text = msg.as_string()
#     s.sendmail(fromaddr, toaddr, text)
#     s.quit()


def send_report():
    return "send report"


default_args = {
    'owner': 'Apoorva',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': None,
}

dag = DAG(
    'Youtube',
    default_args=default_args,
    schedule_interval=timedelta(days=30),
    description='Youtube Trending Videos Analysis',
)

t1 = BashOperator(
    task_id='download_from_source',
    bash_command='kaggle datasets download datasnaek/youtube-new -p /Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow',
    dag=dag,
)

t2 = BashOperator(
    task_id='unzip_downloaded_files',
    bash_command='unzip /Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/youtube-new.zip -d Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/youtube-new/',
    dag=dag,
)

t3 = PythonOperator(
    task_id='Persist_data_In_DB',
    python_callable=insert_raw_data,
    dag=dag,
)

t4 = PythonOperator(
    task_id='CombiningDataForAnalysis',
    python_callable=category_data,
    dag=dag,
)

t5 = PythonOperator(
    task_id='Aggregate_data',
    python_callable=agg_data,
    dag=dag,
)

t6 = PythonOperator(
    task_id='SaveReport',
    python_callable=save_report,
    dag=dag,
)

t7 = PythonOperator(
    task_id='SendReportViaEmail',
    python_callable=send_report,
    dag=dag,

)

#
# t4 = PythonOperator(
#     task_id='merge_data',
#     python_callable=category_data,
#     dag=dag,
# )


# t7 = PythonOperator(
#     task_id='SendReport',
#     python_callable=send_report,
#     dag=dag,
#
# )


t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
