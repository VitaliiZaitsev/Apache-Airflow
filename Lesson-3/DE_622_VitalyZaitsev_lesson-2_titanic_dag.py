import os
import datetime as dt
import pandas as pd
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


# базовые аргументы DAG
args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
    'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
}


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)


def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')


def pivot_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_csv(get_path('titanic_pivot.csv'))

# 2. Написать функцию mean_fare_per_class(), которая считывает файл titanic.csv и
# расчитывает среднюю арифметическую цену билета (Fare) для каждого класса (Pclass)
# и сохраняет результирующий датафрейм в файл titanic_mean_fares.csv
# Функция ПОКА НЕ НАПИСАНА
def mean_fare_per_class():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Pclass'],
                                values='Fare',
                                aggfunc='mean').reset_index()
    df.to_csv(get_path('titanic_mean_fares.csv'))


# В контексте DAG'а зададим набор task'ок
# Объект-инстанс Operator'а - это и есть task
with DAG(
        dag_id='titanic_sample_ZaitsevV',  # Имя DAG
        schedule_interval=None,  # Периодичность запуска, например, "00 15 * * *"
        default_args=args,  # Базовые аргументы
) as dag:
    # BashOperator, выполняющий указанную bash-команду
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    )
    # Загрузка датасета
    create_titanic_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        python_callable=download_titanic_dataset,
        dag=dag,
    )
    # Чтение, преобразование и запись датасета
    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_dataset',
        python_callable=pivot_dataset,
        dag=dag,
    )
    # 3. Добавить в DAG таск с названием mean_fares_titanic_dataset,
    # который будет исполнять функцию mean_fare_per_class(),
    # причем эта задача должна запускаться в параллель с pivot_titanic_dataset после таски create_titanic_dataset.
    # t1 >> [t2, t3]
    mean_fares_titanic_dataset = PythonOperator(
        task_id='pivot_mean_fare_per_class_dataset',
        python_callable=mean_fare_per_class,
        dag=dag,
    )
    # 4. В конец пайплайна (после завершения тасок pivot_titanic_dataset и mean_fares_titanic_dataset)
    # добавить шаг с названием last_task,
    # на котором в STDOUT выводится строка, сообщающая об окончании расчета и
    # выводящая execution date в формате YYYY-MM-DD.
    # Пример строки: "Pipeline finished! Execution date is 2020-12-28"
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ execution_date }}"',
        dag=dag,
    )
    # Порядок выполнения тасок
    first_task >> create_titanic_dataset >> [mean_fares_titanic_dataset, pivot_titanic_dataset] >> last_task