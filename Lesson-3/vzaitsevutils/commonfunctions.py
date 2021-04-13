#
# Модуль типовых функций
#

import os
import pandas as pd
from sqlalchemy import create_engine


# -----------------------------------------------------------------
# Код ниже - рабочий код с урока 2. Код рабочий, просто закомментил
# -----------------------------------------------------------------

# def get_path(file_name):
#     return os.path.join(os.path.expanduser('~'), file_name)


# def download_titanic_dataset():
#     url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
#     df = pd.read_csv(url)
#     df.to_csv(get_path('titanic.csv'), encoding='utf-8')


# def pivot_dataset_processing():
#     titanic_df = pd.read_csv(get_path('titanic.csv'))
#     df = titanic_df.pivot_table(index=['Sex'],
#                                 columns=['Pclass'],
#                                 values='Name',
#                                 aggfunc='count').reset_index()
#     df.to_csv(get_path('titanic_pivot.csv'))

# 2. Написать функцию mean_fare_per_class(), которая считывает файл titanic.csv и
# расчитывает среднюю арифметическую цену билета (Fare) для каждого класса (Pclass)
# и сохраняет результирующий датафрейм в файл titanic_mean_fares.csv
# def mean_fare_per_class_processing():
#     titanic_df = pd.read_csv(get_path('titanic.csv'))
#     df = titanic_df.pivot_table(index=['Pclass'],
#                                 values='Fare',
#                                 aggfunc='mean').reset_index()
#     df.to_csv(get_path('titanic_mean_fares.csv'))


def flush_to_postgres(table_name, df):
    # для docker явно проговорили базу service
    # подключение для случая PostreSQL на локальной машине
    #engine = create_engine('postgresql+psycopg2://service:service@127.0.0.1:54322/service', pool_recycle=3600)
    #------------------------------------------------------
    # docker ps
    # подключение для случая PostreSQL в докере
    engine = create_engine('postgresql+psycopg2://service:service@servicedb:5432/service', pool_recycle=3600)
    try:
        df.to_sql(table_name, engine.connect(), if_exists='fail')
        print("Success. PostgreSQL Table %s was created successfully and data was uploaded." % table_name)
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    finally:
        engine.connect().close()


def download_titanic_dataset_via_xcom(**context):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df = df.to_json()
    context['ti'].xcom_push(key='original_dataset', value=df)


def pivot_dataset_processing_via_xcom(output_table_name: str, **context):
    # df = pd.read_csv(get_path('titanic.csv'))
    df = context['ti'].xcom_pull(task_ids='download_titanic_dataset', key='original_dataset')
    df = pd.read_json(df)
    df = df.pivot_table(index=['Sex'],
                        columns=['Pclass'],
                        values='Name',
                        aggfunc='count').reset_index()
    #context['ti'].xcom_push(key='titanic_pivot', value=df)
    #flush_to_postgres('titanic_pivot', df)
    flush_to_postgres(output_table_name, df)


def mean_fare_per_class_processing_via_xcom(output_table_name: str, **context):
    # df = pd.read_csv(get_path('titanic.csv'))
    df = context['ti'].xcom_pull(task_ids='download_titanic_dataset', key='original_dataset')
    df = pd.read_json(df)
    df = df.pivot_table(index=['Pclass'],
                        values='Fare',
                        aggfunc='mean').reset_index()
    #context['ti'].xcom_push(key='titanic_mean_fares', value=df)
    #flush_to_postgres('mean_fares', df)
    flush_to_postgres(output_table_name, df)
