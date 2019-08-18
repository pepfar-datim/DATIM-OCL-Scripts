"""
Clients should call this utility script rather than the import_manager to fetch the import status
"""

import sys
import json
from constants import RESPONSE_FIELD_STATUS, RESPONSE_FIELD_RESULT, RESPONSE_FIELD_STATUS_CODE
from constants import STATUS_CODE_OK, STATUS_CODE_ERROR, STATUS_CODE_BAD_REQUEST, STATUS_CODE_NOT_FOUND, STATUS_CODE_ACCEPTED
from import_manager import get_import_status

response = None

try:
    if len(sys.argv) < 2:
        response = {
            RESPONSE_FIELD_STATUS_CODE: STATUS_CODE_BAD_REQUEST,
            RESPONSE_FIELD_RESULT: "Import id required"
        }
    else:
        import_status = get_import_status(sys.argv[1])
        result = (import_status.result).encode(encoding="utf-8", errors="strict")
        status_code = STATUS_CODE_OK
        if import_status.status == 'PENDING':
            result = "Pending status could be because of an invalid import id, please confirm that it's correct"
            status_code = STATUS_CODE_NOT_FOUND
        if import_status.status == 'STARTED':
            result = "This task id is currently being processed"
            status_code = STATUS_CODE_ACCEPTED
        else:
            result = (import_status.result).encode(encoding="utf-8", errors="strict")
            status_code = STATUS_CODE_OK
        response = {
            RESPONSE_FIELD_STATUS_CODE: status_code,
            RESPONSE_FIELD_STATUS: import_status.status,
            RESPONSE_FIELD_RESULT: result
        }
except Exception, e:
    response = {
        RESPONSE_FIELD_STATUS_CODE: STATUS_CODE_ERROR,
        RESPONSE_FIELD_RESULT: str(e)
    }

print json.dumps(response)
