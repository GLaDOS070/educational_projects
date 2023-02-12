from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import pandahouse as ph
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io

# вставить токен для бота
my_token = 'my_token' 

# устанавливаем connection
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20221120',
                      'user':'USER', 
                      'password':'PASSWORD'
                     }

default_args = {
    'owner': 'd-merinov-24',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 16)
    }

schedule_interval = '15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def lesson_8_dag_merinov():
    

    @task()
    def run_alerts(chat=None):

        """
        функция детектирует аномалии в данных, при обнаружении анаомалии составляет отчет и график, которые отправляет сообщением в чат в телеграм
            chat: str,
                id чата в телеграм
        """
        
        my_token = 'my_token' 
        chat_id = 'MY_CHAT_ID' 
        bot = telegram.Bot(token=my_token)
        
        
        def check_anomaly(df, metric, a=4, n=5):

            """
            функция предлагает алгоритм проверки значения на аномальность посредством
                df: pandas.DataFrame
                    датафрейм для проведения поиска аномалий
                metric: str
                    метрика, по которой ищем аномалии
                a: int
                    коэффициент межквартильного размаха
                n: int
                    размер окна для скользящей средней
            """

            df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
            df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
            df['iqr'] = df['q75'] - df['q25']
            df['upper'] = df['q75'] + a * df['iqr']
            df['lower'] = df['q25'] - a * df['iqr']


            df['upper'] = df['upper'].rolling(n, center=True, min_periods=1).mean()
            df['lower'] =  df['lower'].rolling(n, center=True, min_periods=1).mean()

            if df[metric].iloc[-1] < df['lower'].iloc[-1] or df[metric].iloc[-1] > df['upper'].iloc[-1]:
                is_alert = 1
            else:
                is_alert = 0

            return is_alert, df

        query = '''  with feed as (
                                     SELECT
                                           toStartOfFifteenMinutes(time) as ts
                                        , toDate(ts) as date
                                        , formatDateTime(ts, '%R') as hm
                                        , uniqExact(user_id) as users_lenta,
                                        countIf(user_id, action='like') as likes,
                                        countIf(user_id, action='view') as views,
                                        likes / views as ctr
                                    FROM simulator_20221120.feed_actions l
                                    WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                                    GROUP BY ts, date, hm
                                    ORDER BY ts), 
                                        message as (
                                            SELECT
                                                toStartOfFifteenMinutes(time) as ts
                                                , toDate(ts) as date
                                                , formatDateTime(ts, '%R') as hm
                                                , count(user_id) as messages
                                            FROM simulator_20221120.message_actions
                                            WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                                            GROUP BY ts, date, hm
                                            ORDER BY ts)

                                    SELECT l.*, r.messages FROM feed AS l
                                    JOIN message as r ON l.ts = r.ts AND
                                                        l.date = r.date AND
                                                        l.hm = r.hm
                        '''
        data = ph.read_clickhouse(query=query, connection=connection)

        metric_list = ['users_lenta', 'likes', 'views', 'ctr', 'messages']
        for metric in metric_list:
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly(df, metric) 

            if is_alert == 1:
                current_value = float(df[metric].iloc[-1])
                last_value = float(df[metric].iloc[-2])
                diff = (current_value -last_value) / last_value * 100
                
                msg = f'''Метрика {metric}:\n текущее значение = {round(current_value,2):}\n отклонение от пред. значения: {round(diff,2)}% 
                https://superset.lab.karpov.courses/superset/dashboard/2390/'''

                sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
                plt.tight_layout()

                ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
                ax = sns.lineplot(x=df['ts'], y=df['upper'], label='upper')
                ax = sns.lineplot(x=df['ts'], y=df['lower'], label='lower')                  

                for ind, label in enumerate(ax.get_xticklabels()): 
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                ax.set(xlabel='time') # задаем имя оси Х
                ax.set(ylabel=metric) # задаем имя оси У

                ax.set_title('{}'.format(metric)) # задае заголовок графика
                ax.set(ylim=(0, None)) # задаем лимит для оси У


                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()


                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    run_alerts()

lesson_8_dag_merinov = lesson_8_dag_merinov()
