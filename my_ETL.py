from datetime import datetime, timedelta,date
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# данные изменены по правилам курса
connection = {'host': 'ссылка',
                      'database':'Название схемы',
                      'user':'логин', 
                      'password':'пароль'
                     }

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'dani-danilov',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2026, 4, 20),   # фиксированная дата в прошлом
}

schedule_interval = '0 12 * * *' # cron-выражение, также можно использовать '@daily', '@weekly', а также timedelta

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_danilov(): 

    @task()
    def extract_feed(): 
        # feed_actions для каждого юзера посчитаем число просмотров и лайков контента
        query = '''select sum(action = 'view') as views, sum(action ='like') as likes, user_id,gender,age,os,toDate(time) as event_date
                    from simulator_20260320.feed_actions 
                    where toDate(time) = yesterday()
                    group by event_date, gender,age,os,user_id
                    '''
        df_extract_feed = ph.read_clickhouse(query, connection=connection)
        return df_extract_feed
    @task()
    def extract_message(): 
        query = ''' with t_1 as (
            select user_id, count(user_id) as messages_sent, count(distinct(receiver_id)) as users_received,toDate(time) as event_date
            from simulator_20260320.message_actions 
            where toDate(time) = yesterday()
            group by event_date,user_id),
            t_2 as (
            select receiver_id, count(receiver_id) as messages_received ,count(distinct(user_id)) as users_sent,toDate(time) as event_date
            from simulator_20260320.message_actions 
            where toDate(time) = yesterday()
            group by event_date,receiver_id)
            select user_id, messages_sent, messages_received,users_received,users_sent,t_1.event_date
            from t_1 join t_2 on t_1.user_id = t_2.receiver_id''' 
        df_extract_message = ph.read_clickhouse(query, connection=connection)
        return df_extract_message
    @task
    def get_merged_df(df_extract_feed,df_extract_message):
        df_merged = pd.merge(df_extract_feed,df_extract_message,on=['user_id','event_date'],how='inner') 
        return df_merged
    @task
    def get_df_gender(df_merged): 
        df_gender = df_merged.groupby(['event_date','gender'],as_index=False)[['views','likes','messages_sent','messages_received','users_received','users_sent']].sum()
        df_gender['dimension'] = 'gender'
        df_gender.rename(columns={'gender':'dimension_value'},inplace=True)
        return df_gender
    @task
    def get_df_os(df_merged): 
        df_os = df_merged.groupby(['event_date','os'],as_index=False)[['views','likes','messages_sent','messages_received','users_received','users_sent']].sum()
        df_os['dimension'] = 'os'
        df_os.rename(columns={'os':'dimension_value'},inplace=True)
        return df_os
    @task
    def get_df_age(df_merged):
        df_age = df_merged.groupby(['event_date','age'],as_index=False)[['views','likes','messages_sent','messages_received','users_received','users_sent']].sum()
        df_age['dimension'] = 'age'
        df_age.rename(columns={'age':'dimension_value'},inplace=True)
        return df_age
    @task 
    def get_final_df(df_gender,df_os,df_age):
        df_final = pd.concat([df_gender,df_os,df_age])
        return df_final 
    @task 
    def load_to_clickhouse(df_final):
        CH_HOST = 'ссылка' # подключение к БД куда мы заливаем данные 
        CH_USER = 'логин'
        CH_PASS = 'пароль'

        # Порядок колонок
        expected_columns = [
            'event_date', 'dimension', 'dimension_value',
            'views', 'likes', 'messages_received', 'messages_sent',
            'users_received', 'users_sent'
        ]
        df_final = df_final[expected_columns]

        # 1. Создаём таблицу (если её нет)
        create_query = """
        CREATE TABLE IF NOT EXISTS test.danilov_airflow (
            event_date Date,
            dimension String,
            dimension_value String,
            views UInt64,
            likes UInt64,
            messages_received UInt64,
            messages_sent UInt64,
            users_received UInt64,
            users_sent UInt64
        ) ENGINE = MergeTree()
        ORDER BY (event_date, dimension, dimension_value)
        """
        r = requests.post(CH_HOST, data=create_query.encode('utf-8'),
                          auth=(CH_USER, CH_PASS), verify=False)
        r.raise_for_status()
        print('Таблица test.danilov_airflow готова')

        # 2. Вставляем данные
        tsv = df_final.to_csv(index=False, sep='\t', header=False)
        insert_query = f'INSERT INTO test.danilov_airflow FORMAT TSV'
        r = requests.post(CH_HOST, params={'query': insert_query},
                          data=tsv.encode('utf-8'),
                          auth=(CH_USER, CH_PASS), verify=False)
        r.raise_for_status()
        print(f'Успешно вставлено {len(df_final)} строк в test.danilov_airflow')
       
            
    df_extract_feed = extract_feed()
    df_extract_message = extract_message()
    df_merged = get_merged_df(df_extract_feed,df_extract_message)
    df_gender = get_df_gender(df_merged)
    df_os = get_df_os(df_merged)
    df_age = get_df_age(df_merged) 
    df_final = get_final_df(df_gender,df_os,df_age)
    load_to_clickhouse(df_final) 

dag_danilov_simulator = dag_danilov_simulator()