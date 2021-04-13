#
# Теперь рассмотрим следующую задачу, необходимо выполнить те же действия с датасетом Titanic,
# что и в предыдущем уроке, но с рядом отличий:
#
# 1. Все функции вынесены в отдельный модуль и в DAG файле только сама структура графа
# (директория модулей должна быть в PATH)
# 2.Отказ от работы с локальными файлами:
# 2.1.сначала скачанный датасет пушится в XCom (он весит ~50 КБ)
# 2.2. затем он пуллится из XCom и передается двум преобразованиям (pivot и mean_fare)
# 3.Результаты преобразований записываются в две таблицы локальной базы PostgreSQL
# (Connections+Hooks или psycopg2/sqlalchemy).
# 4.Имена таблиц в PostgreSQL заданы в Variables


# -----------------------------------------------------------------------------
# 1. Все функции и сеттинги - в отдельный модуль vzaitsevutils
# vzaitsevutils/commonfunctions.py
# vzaitsevutils/settings.py
from vzaitsevutils.settings import default_settings
from vzaitsevutils.commonfunctions import download_titanic_dataset_via_xcom
from vzaitsevutils.commonfunctions import pivot_dataset_processing_via_xcom
from vzaitsevutils.commonfunctions import mean_fare_per_class_processing_via_xcom
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator



# Сам DAG
with DAG(**default_settings()) as dag:
    # Начало
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag
    )

    # Загрузка датасета
    create_titanic_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        python_callable=download_titanic_dataset_via_xcom,
        dag=dag,
        provide_context=True # код имени поддержки Airflow 1.10.* Цена этого кода: 3 бессмысленно потраченных часа жизни
    )

    # Расчёт №1
    pivot_titanic_dataset_processing = PythonOperator(
        task_id='pivot_dataset_processing',
        python_callable=pivot_dataset_processing_via_xcom,
        dag=dag,
        op_kwargs={
            'output_table_name': Variable.get("titanic_pivot_table_name") or "titanic_pivot" # предустановленная переменная titanic_pivot_table_name = 'titanic_pivot'
        },
        provide_context=True  # код имени поддержки Airflow 1.10.* Цена этого кода: 3 бессмысленно потраченных часа жизни
    )

    # Расчёт №2
    mean_fares_titanic_dataset_processing = PythonOperator(
        task_id='pivot_mean_fare_per_class_dataset_processing',
        python_callable=mean_fare_per_class_processing_via_xcom,
        dag=dag,
        op_kwargs={
            'output_table_name': Variable.get("mean_fares_table_name") or "mean_fares" # предустановленная переменная mean_fares_table_name = 'mean_fares'
        },
        provide_context=True # код имени поддержки Airflow 1.10.* Цена этого кода: 3 бессмысленно потраченных часа жизни
    )

    # Завершение
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ ds }}"',
        dag=dag
    )

    # Порядок выполнения тасок
    first_task >> create_titanic_dataset >> [mean_fares_titanic_dataset_processing, pivot_titanic_dataset_processing] >> last_task

