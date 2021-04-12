#
# Модуль типовых функций
#

import os


def dummy_function():
    return


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)
