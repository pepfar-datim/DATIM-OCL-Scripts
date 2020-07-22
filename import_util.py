"""
Clients should call this utility script rather than the import_manager to import a csv
"""

import sys
import json
from constants import RESPONSE_FIELD_ID, RESPONSE_FIELD_RESULT, RESPONSE_FIELD_STATUS_CODE
from constants import STATUS_CODE_ACCEPTED, STATUS_CODE_CONFLICT, STATUS_CODE_ERROR
from constants import STATUS_CODE_BAD_REQUEST
from import_manager import import_csv, ImportInProgressError


def generate_error_response(code, error_msg):
    """ Return dictionary for error response """
    return {
        RESPONSE_FIELD_STATUS_CODE: code,
        RESPONSE_FIELD_RESULT: error_msg
    }


response = None
MIN_ARG_COUNT = 6

try:
    if len(sys.argv) < MIN_ARG_COUNT:
        response = generate_error_response(STATUS_CODE_BAD_REQUEST, 'Missing request parameter(s)')
    else:
        importScript = sys.argv[1]
        country_code = sys.argv[2]
        period = sys.argv[3]
        csv = sys.argv[4]
        country_name = sys.argv[5]
        mode = 'False'

        if len(sys.argv) > MIN_ARG_COUNT:
            mode = sys.argv[MIN_ARG_COUNT]

        task_id = import_csv(importScript, country_code, period, csv, country_name, mode)
        response = {
            RESPONSE_FIELD_STATUS_CODE: STATUS_CODE_ACCEPTED,
            RESPONSE_FIELD_ID: task_id
        }
except ImportInProgressError:
    response = generate_error_response(
        STATUS_CODE_CONFLICT, 'There is already an import in progress for the country')
except Exception:
    response = generate_error_response(STATUS_CODE_ERROR, 'An error occurred')

print json.dumps(response)
