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


# Все функции и сеттинги - в отдельный модуль vzaitsevutils
from vzaitsevutils.settings import default_settings
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator


# Сам DAG
with DAG(
        dag_id='DE_622_VitalyZaitsev_lesson-3',  # Имя DAG
        schedule_interval=None,                  # Периодичность запуска, например, "00 15 * * *"
        default_args=default_settings,           # Базовые аргументы
) as dag:
    # Начало
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    )

    # Завершение
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ execution_date }}"',
        dag=dag,
    )

    # Порядок выполнения тасок
    first_task >> last_task

