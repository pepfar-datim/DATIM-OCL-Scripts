"""
Script to retrieve BulkImport status from OCL and return as JSON.
Designed to work with PEPFAR IMAP and QMAP resources.

python bulkImportStatus.py --env=staging -t=[my-ocl-api-token]
    --bulkImportId=[bulk-import-task-id]
"""
import argparse
import json
import requests
import common


# Checks OCL bulk import status
def check_bulk_import_status(bulkImportId='', ocl_env_url='', ocl_api_token='',
                             import_result_format=''):
    """ Retrieves bulk import status from OCL """
    import_result_formats = ['report', 'json', 'summary']
    if import_result_format not in import_result_formats:
        import_result_format = 'summary'

    ocl_api_headers = {'Content-Type': 'application/json'}
    if ocl_api_token:
        ocl_api_headers['Authorization'] = 'Token ' + ocl_api_token
    import_status_url = "%s/manage/bulkimport/?task=%s&result=%s" % (
        ocl_env_url, bulkImportId, import_result_format)
    response = requests.get(import_status_url, headers=ocl_api_headers)
    response.raise_for_status()
    return response.text


# Configure
parser = argparse.ArgumentParser("bulkImportStatus", description="Get Bulk Import Status from OCL")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=common.ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('--bulkImportId', help='Bulk Import Status ID', required=True)
parser.add_argument('-t', '--token', help='OCL API token', required=True)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + common.APP_VERSION)
parser.add_argument(
    '--format', help='Format of bulk import results to return from OCL', default="summary")
parser.add_argument(
    '--domain', help='Domain of bulk import results to return from OCL', required=True)
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.env_url

# Display debug output
if args.verbosity > 1:
    print args

# Get the bulk import response from OCL and process -- always return as JSON response even if error
ocl_bulk_import_response = ''
try:
    ocl_bulk_import_response = check_bulk_import_status(
        bulkImportId=args.bulkImportId, ocl_env_url=ocl_env_url,
        ocl_api_token=args.token, import_result_format=args.format)
except Exception as e:
    # Not Found or Error -- change the status/status_code based on response
    output_json = {
        "status": "Error",
        "status_code": 0,
        "message": ocl_bulk_import_response
    }
else:
    # Pending, Completed and Success, Completed and Error during import
    output_json = {
        "status": "",
        "status_code": 0,
        "message": ocl_bulk_import_response
    }

if output_json:
    print json.dumps(output_json)
