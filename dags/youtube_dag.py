import json
# import calendar
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd



def insert_raw_data():
    engine = sqlalchemy.create_engine('mysql+pymysql://root:zipcoder@localhost/testDB')
    df = pd.read_csv("/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/youtube-new/USvideos.csv",
                     delimiter=',')


    with engine.connect() as conn, conn.begin():
        df.to_sql('info2', conn, if_exists='replace')



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
    months = {1:"January",2:"February",3:"March",4:"April",5:"May",6:"June",7:"July",8:"August",
              9:"September",10:"October",11:"November",12:"December"}
    df['month'] = df.apply(lambda row: months[int(row.trending_date.split(".")[2])], axis=1)
    df11 = pd.read_csv(
        '/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/dataclean/US_category_formatted.csv')
    #df.groupby('category_id').count()
    df12 = df11.astype({'category_id': int})
    merged_inner = pd.merge(df, df12, on='category_id')
    df13 = merged_inner.groupby(['month', 'category_title']).count()
    with open('/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/dataclean/USvideos_aggregated.csv',
              'w') as f:
        f.write(df13.to_csv())

#
# def inset_data():
#     # engine = sqlalchemy.create_engine('mysql+pymysql://root:zipcoder@localhost/Youtube.youtubeStats')
#     # df_sql = pd.read_sql_table('match_results', engine)
#     sql_config = {
#         'server': 'localhost',
#         'database': 'Youtube',
#         'username': 'root',
#         'password': 'zipcoder'
#     }
#     table_name = 'youtubeStats'
#     df = pd.DataFrame(np.random.randint(-100, 100, size=(100, 4)),
#                       columns=list('ABCD'))
#
#     cur = db.cursor()
#
#     for row in cur.fetchall():
#         print
#         row
#
#     db.close()




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

t3 = PythonOperator(
    task_id='Persist_data',
    python_callable=category_data,
    dag=dag,
)

t1 = BashOperator(
    task_id='download_from_source',
    bash_command='kaggle datasets download datasnaek/youtube-new -p /Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow',
    dag=dag,
)

t2 = BashOperator(
    task_id='unzip_downloaded_files',
    bash_command='unzip /Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/youtube-new.zip',
    dag=dag,
)

# t3 = PythonOperator(
#     task_id='ReadFile',
#     python_callable=read_data,
#     dag=dag,
# )

t4 = PythonOperator(
    task_id='CombiningData',
    python_callable=category_data,
    dag=dag,
)

t5 = PythonOperator(
    task_id='AggregatingData',
    python_callable=agg_data,
    dag=dag,
)






t1 >> t2 >> t3 >> t4 >> t5
