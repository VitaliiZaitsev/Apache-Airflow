import os
import datetime as dt
import pandas as pd
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from vzaitsevutils.subdag import subdag_dataprocessing


# В реализации этого ДЗ помогли следующие ссылки
# Apache Airflow | Minimising DAGs With SubDAGs
# https://www.youtube.com/watch?v=7CdIixOw4MI
#
# Airflow – Sub-DAGs
# https://www.cloudwalker.io/2019/07/29/airflow-sub-dags/
#
# Airflow Tricks — Xcom and SubDAG
# https://medium.com/analytics-vidhya/airflow-tricks-xcom-and-subdag-361ff5cd46ff
#
# Using SubDAGs in Airflow
# https://www.astronomer.io/guides/subdags
#
# https://airflow.apache.org/docs/apache-airflow/stable/concepts.html?highlight=subdag#subdags

# ---------------------------------------------------------
# Функции обработки
def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)


def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')


# --------------------------------------------------------------------------------
DAG_ID = 'VZaitsev-lesson-4-parentdag'
# базовые аргументы DAG
args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
    'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
}

# Родительский DAG
with DAG(
        dag_id=DAG_ID,               # Имя DAG
        schedule_interval='@daily',  # Периодичность запуска
        default_args=args            # Базовые аргументы
) as dag:
    # Начало
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag
    )
    # Загрузка датасета
    create_titanic_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        python_callable=download_titanic_dataset,
        dag=dag
    )
    # SubDAG
    SUBDAG_TASK_ID = 'subdag_processing'
    dataprocessing = SubDagOperator(
        task_id=SUBDAG_TASK_ID,
        subdag=subdag_dataprocessing('%s.%s' % (DAG_ID, SUBDAG_TASK_ID),
                              dag.schedule_interval,
                              dag.default_args['start_date']),
        dag=dag)
    # Final
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ ds }}"',
        dag=dag
    )
    # Порядок выполнения тасок
    first_task >> create_titanic_dataset >> dataprocessing >> last_task
