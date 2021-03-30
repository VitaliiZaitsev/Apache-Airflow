import os
import pandas as pd


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)


def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')


# def pivot_dataset():
#     titanic_df = pd.read_csv(get_path('titanic.csv'))
#     df = titanic_df.pivot_table(index=['Sex'],
#                                 columns=['Pclass'],
#                                 values='Name',
#                                 aggfunc='count').reset_index()
#     df.to_csv(get_path('titanic_pivot.csv'))


def mean_fare_per_class():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Pclass'],
                                values='Fare',
                                aggfunc='mean').reset_index()
    df.to_csv(get_path('titanic_mean_fares.csv'))


download_titanic_dataset()
mean_fare_per_class()
