import os
import pandas as pd
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------
# Функции обработки
def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)


def pivot_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_csv(get_path('titanic_pivot.csv'))


def mean_fare_per_class():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Pclass'],
                                values='Fare',
                                aggfunc='mean').reset_index()
    df.to_csv(get_path('titanic_mean_fares.csv'))


# -----------------------------------------------------------
# Сам subdag
def subdag_dataprocessing(dag_id, schedule_interval, start_date):
    subdag_dataprocessing = DAG(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        start_date=start_date)

    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_dataset',
        python_callable=pivot_dataset,
        dag=subdag_dataprocessing
    )

    mean_fares_titanic_dataset = PythonOperator(
        task_id='pivot_mean_fare_per_class_dataset',
        python_callable=mean_fare_per_class,
        dag=subdag_dataprocessing
    )

    jointpoint = BashOperator(
        task_id='join_point',
        bash_command="echo This is a join",
        trigger_rule=TriggerRule.ONE_SUCCESS,
        dag=subdag_dataprocessing
    )

    # Порядок выполнения тасок
    [mean_fares_titanic_dataset, pivot_titanic_dataset] >> jointpoint

    return subdag_dataprocessing
