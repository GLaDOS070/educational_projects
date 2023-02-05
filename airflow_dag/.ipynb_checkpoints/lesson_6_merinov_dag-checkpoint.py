from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
import pandas as pd
import pandahouse as ph

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20221120',
                      'user':'USER', 
                      'password':'PASSWORD'
                     }
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'USER_TEST', 
                      'password':'PASSWORD_TEST'
                     }
default_args = {
    'owner': 'd-merinov-24',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 13),
    'schedule_interval': '0 12 * * *'
    }


@dag(default_args=default_args, catchup=False)
def lesson_6_etl_merinov():
    
    @task()
    def get_feed():
        query = '''
                    SELECT user_id as users,
                           toDate(time) as event_date,
                           countIf(action = 'like') as likes,
                           countIf(action = 'view') as views,
                           os, gender, age
                    FROM simulator_20221120.feed_actions 
                    WHERE toDate(time) = today() - 1
                    GROUP BY users, os, gender, age, event_date
                '''
        df_feed = ph.read_clickhouse(query=query, connection=connection)
        return df_feed
    @task()
    def get_messages():
            query = '''
                        with table1 as 
                            (SELECT user_id as users,
                                   toDate(time) as event_date,
                                   count(1) as messages_sent,
                                   count(DISTINCT reciever_id) as users_received,
                                   os, gender, age
                            FROM simulator_20221120.message_actions
                            WHERE toDate(time) = today() - 1
                            GROUP BY users, os, gender, age, event_date
                            )

                        SELECT l.*, messages_received, users_sent FROM table1 as l
                        LEFT JOIN 
                          (
                              SELECT reciever_id as users,
                                     toDate(time) as event_date,
                                     count(1) as messages_received, 
                                     count(DISTINCT user_id) as users_sent
                              FROM simulator_20221120.message_actions 
                              WHERE toDate(time) = today() - 1
                              GROUP BY users, event_date
                          ) as r ON l.users = r.users and l.event_date = r.event_date
                '''
            df_mess = ph.read_clickhouse(query=query, connection=connection)
            return df_mess
    @task()
    def merge_df(df_feed, df_mess):
        df_all = df_feed.merge(df_mess,
                                   left_on=['users', 'event_date', 'os', 'gender', 'age'],
                                   right_on=['users', 'event_date', 'os', 'gender', 'age'],
                                   how='outer')
        return df_all
    @task()
    def get_df_gender(df_all):
        df_gender = df_all[['gender', 'event_date', 'messages_sent', 'users_received',
                                      'messages_received', 'users_sent', 'views', 'likes']].groupby(['gender','event_date'])\
                                                            .sum().reset_index()\
                                                            .assign(demension = 'gender')\
                                                            .rename(columns={'gender':'dimension_value'})
        return df_gender

    @task()
    def get_df_age(df_all):
        df_age = df_all[['age', 'event_date', 'messages_sent', 'users_received',
                                      'messages_received', 'users_sent', 'views', 'likes']].groupby(['age','event_date'])\
                                                            .sum().reset_index()\
                                                            .assign(demension = 'age')\
                                                            .rename(columns={'age':'dimension_value'})
        return df_age

    @task()
    def get_df_os(df_all):
        df_os = df_all[['os', 'event_date', 'messages_sent', 'users_received',
                                      'messages_received', 'users_sent', 'views', 'likes']].groupby(['os', 'event_date'])\
                                                            .sum().reset_index()\
                                                            .assign(demension = 'os')\
                                                            .rename(columns={'os':'dimension_value'})
        return df_os
    @task()
    def get_final_df(df_gender, df_age, df_os):
        df_final = pd.concat([df_gender, df_age, df_os], axis=0).reset_index()

        df_final.drop(['index'], axis = 1, inplace = True)

        df_final = df_final[['event_date', 'demension','dimension_value', 'views', 'likes', 'messages_received',
                               'messages_sent', 'users_received', 'users_sent']]
        df_final[['views', 
          'likes', 
          'messages_received',
          'messages_sent', 
          'users_received', 
          'users_sent']] = df_final[['views', 
                                     'likes', 
                                     'messages_received',
                                     'messages_sent', 
                                     'users_received', 
                                     'users_sent']].astype('int')

        return df_final

    
    @task
    def load(df_final):
        create = '''CREATE TABLE IF NOT EXISTS test.d_merinov_24
        (event_date Date,
         demension String,
         dimension_value String,
         views Int64,
         likes Int64,
         messages_received Int64,
         messages_sent Int64,
         users_received Int64,
         users_sent Int64
         ) ENGINE = MergeTree()
         ORDER BY event_date
         '''
        ph.execute(query=create, connection=connection_test)
        ph.to_clickhouse(df=df_final, table='d_merinov_24', connection=connection_test, index=False)

    mess = get_messages()
    feed = get_feed()
    df_all = merge_df(mess, feed)
    df_os = get_df_os(df_all)
    df_age = get_df_age(df_all)
    df_gender = get_df_gender(df_all)
    df_final = get_final_df(df_gender, df_age, df_os)
    load(df_final)
    
lesson_6_etl_merinov = lesson_6_etl_merinov()