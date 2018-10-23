"""
This manager is responsible for mediating the import process of a csv file by ensuring that if a
given country submits an import, it gets blocked from making subsequent submissions until the
current import is complete.

It's also responsible for responding to the status requests of a country's import by a specified
task id. If the import is complete, it should include the results in the response in case of a success
otherwise error details in case of a failure.

Client code shouldn't directly call methods on this manager, instead they should call
functions in the import_util module

"""

import os
import uuid
from threading import Lock
import subprocess

from celery import Celery
from constants import *

__celery = Celery('import_task')
__celery.config_from_object(os.getenv(ENV_CELERY_CONFIG, 'celeryconfig'))

lock = Lock()


class ImportStatus:
    """
    # Encapsulates information about the status and results of an import
    """
    def __init__(self, status=None, result=None):
        self.status = status
        self.result = result


class ImportInProgressError(StandardError):
    pass


@__celery.task(name='import_task')
def __import_task(script_filename, country_code, period, csv, country_name, test_mode):
    # Calls the specified python import script along with the rest of the args
    return subprocess.check_output(['python', script_filename, country_code, period, csv, country_name, test_mode])


def import_csv(script_filename, country_code, period, csv, country_name, test_mode):
    """ Imports a csv file asynchronously, will ony process the import if the country has no
     existing import

        Arguments:
            script_filename (str): The name of the python import script
            country_code (str): Country code of the country
            period (str): The period of the year the import is assigned to
            csv (str): The csv file to import
            country_name (str): Name of the country
            test_mode (str): Boolean True or False

        Returns:
              None
    """
    with lock:
        if has_existing_import(country_code):
            raise ImportInProgressError

        task_id = country_code + TASK_ID_SEPARATOR + uuid.uuid4().__str__()
        script_args = [script_filename, country_code, period, csv, country_name, test_mode]

        __import_task.apply_async(task_id=task_id, args=script_args)

        return task_id


def has_existing_import(country_code):
    """ Checks if the country has an existing import request that is not completed, this will
    typically return true even if the import task is still queued up and waiting for execution

    Arguments: the country code to check against

    Returns:
          const:`True` if the country has an existing import request otherwise const:`False`
    """
    country_task = None
    for task in get_all_tasks():
        if task.get(TASK_ID_KEY).startswith(country_code):
            country_task = task
            break

    return country_task is not None


def get_all_tasks():
    all_tasks = []
    inspect = __celery.control.inspect()

    reserved = inspect.reserved()
    if reserved is not None:
        for tasks1 in reserved.values():
            all_tasks.extend(tasks1)

    scheduled = inspect.scheduled()
    if scheduled is not None:
        for tasks2 in scheduled.values():
            all_tasks.extend(tasks2)

    active = inspect.active()
    if active is not None:
        for tasks3 in active.values():
            all_tasks.extend(tasks3)

    unique_tasks = []
    found_task_ids = []
    for task in all_tasks:
        task_id = task.get(TASK_ID_KEY)
        if task_id not in found_task_ids:
            found_task_ids.append(task_id)
            unique_tasks.append(task)

    return unique_tasks


def get_import_status(task_id):
    async_result = __celery.AsyncResult(task_id)
    result = None
    if async_result.ready():
        result = async_result.result

    return ImportStatus(async_result.state, result)
