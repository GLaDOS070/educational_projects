from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
import pandahouse as ph
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io

# устанавливаем connection
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20221120',
                      'user':'USER', 
                      'password':'PASSWORD'
                     }

# выбираем необходимую тему для графиков
from matplotlib import style
sns.set_theme(({**style.library["fivethirtyeight"]}))
plt.rcParams["figure.figsize"] = (15,8)

# вставить токен для бота
my_token = 'my_token' 
bot = telegram.Bot(token=my_token) 

# вставить чат id
chat_id = 'chat_id'


default_args = {
    'owner': 'd-merinov-24',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 16)
    }

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def lesson_7_dag_1_merinov():

    @task()
    def get_dau_df():

        """
        функция вовзвращает датафрейм с данными по DAU
        """

        query = '''SELECT toStartOfDay(toDateTime(time)) AS  day,
                       count(DISTINCT user_id) AS uniq_users
                FROM simulator_20221120.feed_actions
                WHERE day > (today()-1) - 7 and day != today()
                GROUP BY toStartOfDay(toDateTime(time))
                ORDER BY day DESC'''
        dau_df = ph.read_clickhouse(query=query, connection=connection)
        dau_df.day = dau_df.day.dt.date
        return dau_df
    
    @task()
    def get_likes_views_df():

        """
        функция вовзвращает датафрейм с данными по лайкам и просмотрам
        """
         
        query = '''SELECT toStartOfDay(toDateTime(time)) AS day,
                       countIf(user_id, action='like') AS likes,
                       countIf(user_id, action='view') AS views
                FROM simulator_20221120.feed_actions
                WHERE day > (today()-1) - 7 and day != today()
                GROUP BY toStartOfDay(toDateTime(time))
                ORDER BY day DESC'''

        likes_views_df = ph.read_clickhouse(query=query, connection=connection)
        likes_views_df.day = likes_views_df.day.dt.date
        return likes_views_df
    
    @task()
    def get_ctr_df():

        """
        функция вовзвращает ctr по лайкам и просмотрам
        """
         
        query = '''SELECT toStartOfDay(toDateTime(time)) AS day,
                        CountIf(user_id, action = 'like') / CountIf(user_id, action = 'view') AS ctr
                    FROM simulator_20221120.feed_actions
                    WHERE day > (today()-1) - 7 and day != today()
                    GROUP BY toStartOfDay(toDateTime(time))
                    ORDER BY day DESC'''
        ctr_df = ph.read_clickhouse(query=query, connection=connection)
        ctr_df.day = ctr_df.day.dt.date
        ctr_df.ctr = ctr_df.ctr.mul(100).round(2)
        return ctr_df

    @task()
    def send_text_info(dau_df, likes_views_df, ctr_df):
        
        """
        функция отправляет сообщение с текстом отчета
            dau_df: pandas.DataFrame
                датафрейм с данными по пользователям
            likes_views_df: pandas.DataFrame
                датафрейм с данными по лайкам и просмотрам
            ctr_df: pandas.DataFrame
                датафрейм с данными по CTR
        """

        msg_general = f'📋ЕЖЕДНЕВНЫЙ ОТЧЕТ о ключевых метриках приложения на {dau_df.loc[0][0]}'
        bot.sendMessage(chat_id=chat_id, text=msg_general)

        msg = f'👱<strong>DAU</strong> - {dau_df.loc[0][1]} пользователей\n\n<strong>💚Лайки</strong> - {likes_views_df.loc[0][1]} лайков\n\n<strong>📲Просмотры</strong> - {likes_views_df.loc[0][2]} постов\n\n<strong>📈CTR</strong> - {ctr_df.loc[0][1]}%'
        bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='HTML')

    @task()
    def send_all_plots(dau_df, likes_views_df, ctr_df):

        """
        функция отправляет сообщение с графиками по метрикам
            dau_df: pandas.DataFrame
                датафрейм с данными по пользователям
            likes_views_df: pandas.DataFrame
                датафрейм с данными по лайкам и просмотрам
            ctr_df: pandas.DataFrame
                датафрейм с данными по CTR
        """

        sns.lineplot(x=dau_df.day, y=dau_df.uniq_users)
        plt.title("DAU in last 7 days")
        plot_object1 = io.BytesIO()
        plt.savefig(plot_object1)
        plot_object1.seek(0)
        plot_object1.name = 'dau.png'
        plt.close()

        sns.lineplot(x=likes_views_df.day, y=likes_views_df.likes)
        plt.title("Likes in last 7 days")
        plot_object2 = io.BytesIO()
        plt.savefig(plot_object2)
        plot_object2.seek(0)
        plot_object2.name = 'likes.png'
        plt.close()

        sns.lineplot(x=likes_views_df.day, y=likes_views_df.views)
        plt.title("Views in last 7 days")
        plot_object3 = io.BytesIO()
        plt.savefig(plot_object3)
        plot_object3.seek(0)
        plot_object3.name = 'views.png'
        plt.close()

        sns.lineplot(x=ctr_df.day, y=ctr_df.ctr)
        plt.title("CTR in last 7 days")
        plot_object4 = io.BytesIO()
        plt.savefig(plot_object4)
        plot_object4.seek(0)
        plot_object4.name = 'ctr.png'
        plt.close()

        media_group = [telegram.InputMediaPhoto(plot_object1), 
                       telegram.InputMediaPhoto(plot_object2),
                       telegram.InputMediaPhoto(plot_object3),
                      telegram.InputMediaPhoto(plot_object4)]
        bot.send_media_group(chat_id = chat_id, media = media_group)
    

    # определяем последовательность тасков
    dau_df = get_dau_df()
    likes_views_df = get_likes_views_df()
    ctr_df = get_ctr_df()
    send_text_info(dau_df, likes_views_df, ctr_df)
    send_all_plots(dau_df, likes_views_df, ctr_df)

# запускаем dag
lesson_7_dag_1_merinov = lesson_7_dag_1_merinov()