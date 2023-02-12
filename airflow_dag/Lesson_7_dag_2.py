from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
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
def lesson_7_dag_2_merinov():

    @task()
    def get_dau_df_2():

        """
        функция вовзвращает датафрейм с данными по DAU
        """

        query = '''
                    SELECT COUNT (DISTINCT user_id ) as uniq_users,
                    day, os, gender, age, source 
                    FROM (

                            SELECT user_id,
                              toStartOfDay(toDateTime(time)) AS  day, os, gender, age, source 
                            FROM simulator_20221120.feed_actions 
                            GROUP BY user_id, day,os, gender, age, source 
                            HAVING day > (today()-1) - 7 and day != today()

                            UNION ALL

                            SELECT user_id,
                              toStartOfDay(toDateTime(time)) AS  day, os, gender, age, source 
                            FROM simulator_20221120.message_actions 
                            GROUP BY user_id, day, os, gender, age, source 
                            HAVING day > (today()-1) - 7 and day != today()
                              )
                    GROUP BY day, os, gender, age, source 
                '''
        dau_df = ph.read_clickhouse(query=query, connection=connection)
        dau_df.day = dau_df.day.dt.date
        return dau_df

    @task()
    def get_dau_info(dau_df):

        """
        функция вовзвращает текстовый отчет по пользовательским метрикам в приложении
            dau_df: pandas.DataFrame
                датафрейм с данными по пользователя
        """

        dau = dau_df.groupby('day', as_index=False).agg({'uniq_users':'sum'})
        dau['growth_rate'] = dau.uniq_users.pct_change()

        date = dau.day.max()
        dau_value = dau.query('day == @dau_df.day.max()').iloc[0][1]
        diff = round((dau.query('day == @dau_df.day.max()').iloc[0][2]*100), 2)

        if diff > 0:
            change = 'больше'
        else:
            change = 'меньше'

        title = '👥<b>Пользователи</b>'

        text_1 = f'За {date} DAU составил {dau_value}, что на {abs(diff)}% {change}, чем днем ранее.'

        source = dau_df.groupby(['day', 'source'], as_index=False)\
                    .agg({'uniq_users':'sum'})\
                    .sort_values('day')
        source['ads_growth_rate'] = source.query('source == "ads"').uniq_users.pct_change()
        source['organic_growth_rate'] = source.query('source == "organic"').uniq_users.pct_change()

        ads_users = source.query('day == @dau_df.day.max() and source == "ads"').iloc[0][2]
        organic_users = source.query('day == @dau_df.day.max() and source == "organic"').iloc[0][2]

        ads_growth = round(source.query('day == @dau_df.day.max() and source == "ads"').iloc[0][3] * 100, 2)
        organic_growth = round(source.query('day == @dau_df.day.max() and source == "organic"').iloc[0][4] * 100, 2)

        text_2 = f'Их них {ads_users} ({ads_growth}% к пред. дню) пользователей с рекламы и {organic_users} ({organic_growth}% к пред. дню) с органического трафика. '

        os = dau_df.groupby(['day', 'os'], as_index=False)\
                    .agg({'uniq_users':'sum'})\
                    .sort_values('day')
        os['androind_growth_rate'] = os.query('os == "Android"').uniq_users.pct_change()
        os['iOS_growth_rate'] = os.query('os == "iOS"').uniq_users.pct_change()

        android_users = os.query('day == @dau_df.day.max() and os == "Android"').iloc[0][2]
        ios_users = os.query('day == @dau_df.day.max() and os == "iOS"').iloc[0][2]

        android_growth = round(os.query('day == @dau_df.day.max() and os == "Android"').iloc[0][3] * 100, 2)
        ios_growth = round(os.query('day == @dau_df.day.max() and os == "iOS"').iloc[0][4] * 100, 2)

        text_3 = f'Лентой воспользовались {android_users} ({android_growth}% к пред. дню) пользователей с Android и {ios_users} пользователей с iOS({ios_growth}%  к пред. дню). '

        return title + '\n' + '\n' + text_1 + '\n' + text_2 + '\n' + text_3 + '\n'

    @task()
    def get_df_new_users():

        """
        функция вовзвращает датафрейм с данными по новым пользователям
        """

        query = '''with mess as (Select    user_id,
                               min(toDate(time)) as bd,
                           os, gender, age, source
                            From simulator_20221120.message_actions 
                            Group by user_id, os, gender, age, source
                            having bd > (today()-1) - 7 and bd != today()),
                    feed as 
                                (Select    user_id,
                                           min(toDate(time)) as bd,
                                           os, gender, age, source
                                From simulator_20221120.feed_actions 
                                Group by user_id, os, gender, age, source
                                having bd > (today()-1) - 7 and bd != today())

            select count(distinct user_id) as users, bd, os,gender, age, source from feed l
            full Join mess r on l.user_id = r.user_id 
                        AND l.bd=r.bd 
                        AND l.os=r.os 
                        AND l.gender=r.gender 
                        AND l.age=r.age 
                        AND l.source = r.source
            group by bd, os,gender, age, source
            ORDER BY bd DESC'''
        df_new_users = ph.read_clickhouse(query=query, connection=connection)
        df_new_users.bd = df_new_users.bd.dt.date
        return df_new_users
    
    @task()
    def get_info_new_users(df_new_users):

        """
        функция вовзвращает текстовый отчет по метрикам в приложении о новых пользователях
            df_new_users: pandas.DataFrame
                датафрейм с данными по новым пользователям
        """

        new_users = df_new_users.groupby('bd', as_index=False).agg({'users':'sum'})
        new_users['growth_rate'] = new_users.users.pct_change()

        date = new_users.bd.max()
        new_users_value = new_users.query('bd == @df_new_users.bd.max()').iloc[0][1]
        diff = round((new_users.query('bd == @df_new_users.bd.max()').iloc[0][2]*100), 2)

        if diff > 0:
            change = 'больше'
        else:
            change = 'меньше'

        title = "🆕<b>Новые пользователи</b>"

        text_1 = f'За день пришло {new_users_value} новых пользователей, что на {abs(diff)}% {change}, чем днем ранее.'

        source = df_new_users.groupby(['bd', 'source'], as_index=False)\
                    .agg({'users':'sum'})\
                    .sort_values('bd')

        source['ads_growth_rate'] = source.query('source == "ads"').users.pct_change()
        source['organic_growth_rate'] = source.query('source == "organic"').users.pct_change()

        ads_users = source.query('bd == @df_new_users.bd.max() and source == "ads"').iloc[0][2]
        organic_users = source.query('bd == @df_new_users.bd.max() and source == "organic"').iloc[0][2]

        ads_growth = round(source.query('bd == @df_new_users.bd.max() and source == "ads"').iloc[0][3] * 100, 2)
        organic_growth = round(source.query('bd == @df_new_users.bd.max() and source == "organic"').iloc[0][4] * 100, 2)

        text_2 = f'Их них {ads_users} ({ads_growth}% к пред. дню) пользователей с рекламы и {organic_users} ({organic_growth}% к пред. дню) с органического трафика. '

        df_new_users['age_cut'] = pd.cut(df_new_users.age, [0, 15, 21, 27, 35, 45, 60, 70, 150])

        male = df_new_users.groupby(['gender', 'bd'])['users'].sum()\
                        .to_frame().reset_index()\
                        .query('bd == @df_new_users.bd.max() and gender == 1')\
                        .iloc[0][2]
        female = df_new_users.groupby(['gender', 'bd'])['users'].sum()\
                        .to_frame().reset_index()\
                        .query('bd == @df_new_users.bd.max() and gender == 0')\
                        .iloc[0][2]

        age = df_new_users.groupby(['age_cut', 'bd'])['users'].sum()\
                        .to_frame().reset_index()\
                        .sort_values(['bd', 'users'], ascending=False)\
                        .iloc[0][0]
        male_share = round(male/(female+male)*100)
        female_share = round(female/(female+male)*100)

        text_3 = f'Среди новых пользователей мужчин - {male} ({male_share}%) человек, девушек - {female} ({female_share}%) человек.  Наибольшее число новых пользователей в возрасте {age}'


        return title+ '\n' + '\n' + text_1 + '\n' + text_2 + '\n' + text_3
    
    @task()
    def get_likes_views_df():
            
            """
            функция вовзвращает датафрейм с данными по лайкам и просмотрам
            """
            query = '''SELECT toStartOfDay(toDateTime(time)) AS day,
                           count(user_id) as actions,
                           action 
                      FROM simulator_20221120.feed_actions
                      WHERE day > (today()-1) - 7 and day != today()
                      GROUP BY toStartOfDay(toDateTime(time)), action
                      ORDER BY day DESC '''

            likes_views_df = ph.read_clickhouse(query=query, connection=connection)
            likes_views_df.day = likes_views_df.day.dt.date
            return likes_views_df

    @task()
    def get_messages_df():
            
            """
            функция вовзвращает датафрейм с данными по сообщениям
            """

            query = '''SELECT toStartOfDay(toDateTime(time)) AS day,
                           count(user_id) as messages
                    FROM simulator_20221120.message_actions
                    WHERE day > (today()-1) - 7 and day != today()
                    GROUP BY toStartOfDay(toDateTime(time))
                    ORDER BY day DESC'''
            messages_df = ph.read_clickhouse(query=query, connection=connection)
            # messages_df.day = likes_views_df.day.dt.date

            return messages_df
    @task()
    def get_info_likes_views_mess(likes_views_df, messages_df):

        """
        функция вовзвращает текстовый отчет по метрикам в приложении о лайках, просмотрах и сообщениях
            likes_views_df: pandas.DataFrame
                датафрейм с данными по лайкам и просмотрам
            messages_df: pandas.DataFrame
                датафрейм с данными по сообщениям
        """

        actions_df = likes_views_df.groupby(['day', 'action'], as_index=False)\
                    .agg({'actions':'sum'})\
                    .sort_values('day')
        actions_df['like_growth_rate'] = actions_df.query('action == "like"').actions.pct_change()
        actions_df['view_growth_rate'] = actions_df.query('action == "view"').actions.pct_change()

        likes = actions_df.query('day == @actions_df.day.max() and action == "like"').iloc[0][2]
        views = actions_df.query('day == @actions_df.day.max() and action == "view"').iloc[0][2]

        likes_growth = round(actions_df.query('day == @actions_df.day.max() and action == "like"').iloc[0][3] * 100, 2)
        views_growth = round(actions_df.query('day == @actions_df.day.max() and action == "view"').iloc[0][4] * 100, 2)

        ctr = round(likes/views, 4)*100

        title = "💖💬<b>Активность</b>"

        text_1 = f'За вчера было поставлено {likes} лайков ({likes_growth}% к пред. дню) и просмотрено {views} постов ({views_growth}% к пред. дню). CTR составил  {ctr}%'

        mes_1 = messages_df.sort_values('day', ascending=False).iloc[0][1]
        mes_0 = messages_df.sort_values('day', ascending=False).iloc[1][1]

        mes_diff = round(mes_1/mes_0 - 1, 2)*100

        text_2 = f'Также было отправлено {mes_1} сообщений ({mes_diff}% к пред. дню)'

        return title + '\n'+ '\n' + text_1 + '\n' + text_2
    
    @task()
    def send_plot_dau_df(dau_df):

        """
        функция отпраляет графики на основании данных о DAU
            dau_df: pandas.DataFrame
                датафрейм с данными по пользователям
        """

        dau_df.day = pd.to_datetime(dau_df["day"])
        dau_df = dau_df.sort_values('day')
        dau_df.day = dau_df.day.dt.strftime('%d-%m')

        plt.subplot(212)
        plt.title('Динамика DAU')
        sns.lineplot(y = 'uniq_users', x='day', data=dau_df)
        plt.subplot(221)
        plt.title('Динамика DAU в разбивке по OS')
        sns.lineplot(y = dau_df.uniq_users, x=dau_df.day, hue=dau_df.os)
        plt.subplot(222)
        plt.title('Динамика DAU в разбивке по Source')
        sns.lineplot(y = dau_df.uniq_users, x=dau_df.day, hue=dau_df.source)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'dau.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, parse_mode='HTML')
        
    @task()
    def send_plot_new_users_df(df_new_users):

        """
        функция отпраляет графики на основании данных о новых пользователях
            df_new_users: pandas.DataFrame
                датафрейм с данными по новым пользователям
        """
        
        df_new_users.bd = pd.to_datetime(df_new_users.bd)
        df_new_users = df_new_users.sort_values('bd').query('bd > "1971-01-01"')
        df_new_users.bd = df_new_users.bd.dt.strftime('%d-%m')


        plt.subplot(212)
        plt.title('динамика привлечения новых пользователей')
        sns.lineplot(y = 'users', x='bd', data=df_new_users)
        plt.subplot(221)
        plt.title('Новые пользователи в разрезе OS')
        sns.lineplot(y = 'users', x='bd', hue='os', data=df_new_users)
        plt.subplot(222)
        plt.title('Новые пользователи в разрезе Source')
        sns.lineplot(y = 'users', x='bd', hue='source', data=df_new_users)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'dau.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, parse_mode='HTML')
        
    @task()
    def send_plot_likes_views_df(likes_views_df, messages_df):

        """
        функция отпраляет графики на основании данных о сообщениях, лайках и просмотров
            likes_views_df: pandas.DataFrame
                датафрейм с данными по лайкам и просмотрам
            messages_df: pandas.DataFrame
                датафрейм с данными по сообщениям
        """

        likes_views_df.day = pd.to_datetime(likes_views_df["day"])
        likes_views_df = likes_views_df.sort_values('day')
        likes_views_df.day = likes_views_df.day.dt.strftime('%d-%m')

        messages_df.day = pd.to_datetime(messages_df["day"])
        messages_df = messages_df.sort_values('day')
        messages_df.day = messages_df.day.dt.strftime('%d-%m')

        ctr = likes_views_df.pivot_table(index='day', columns='action', values='actions').reset_index()
        ctr['ctr'] = ctr.like / ctr.view
        ctr['ctr'] = ctr.ctr.mul(100).round(2)

        plt.subplot(212)
        plt.title('Динамика CTR, %')
        sns.lineplot(y = 'ctr', x='day', data=ctr)
        plt.subplot(221)
        plt.title('Активность в ленте')
        sns.lineplot(y = 'actions', x='day', hue='action', data=likes_views_df)
        plt.subplot(222)
        plt.title('Кол-во отправленных сообщений')
        sns.lineplot(y = 'messages', x='day', data=messages_df)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'dau.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, parse_mode='HTML')
        
    @task()
    def send_message(title):

        """
        функция отправлет сообщение 
            title: str
                текст сообщения
        """
        bot.sendMessage(chat_id=chat_id, text=title, parse_mode='HTML')
        
    @task()
    def send_message_title():

        """
        функция отправлет заголовок отчета 
        """
         
        context = get_current_context()
        ds = context['ds']
        bot.sendMessage(chat_id=chat_id, text=f"📄<b>Ежедневный отчет по ленте новостей, и по сервису отправки сообщений. Дата: {ds}</b>", parse_mode='HTML')
    
    # определяем последовательность тасков
    send_message_title()
    dau_df = get_dau_df_2()
    title_1 = get_dau_info(dau_df)
    send_message(title_1)
    send_plot_dau_df(dau_df)
    df_new_users = get_df_new_users()
    title_2 = get_info_new_users(df_new_users)
    send_message(title_2)
    send_plot_new_users_df(df_new_users)
    likes_views_df = get_likes_views_df()
    messages_df = get_messages_df()
    title_3 = get_info_likes_views_mess(likes_views_df, messages_df)
    send_message(title_3)
    send_plot_likes_views_df(likes_views_df, messages_df)
    
 # запускаем dag   
lesson_7_dag_2_merinov = lesson_7_dag_2_merinov()