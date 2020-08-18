"""
Script to retrieve QMAP status from OCL and return as JSON.
Designed to work with PEPFAR IMAP and QMAP resources.

python bulkImportStatus.py --env=staging -t="my-api-token-here"
    --bulkImportId=d6ac3dd2-e565-455c-b29a-f7d859fd1fe8-datim-admin
"""
import json
import argparse
import requests
import common


# Checks OCL bulk import status
def check_bulk_import_status(bulkImportId='', ocl_env_url='', ocl_api_token='',
                             import_result_format=''):
    """ Return bulk import status ID """
    import_result_formats = ['report', 'json', 'summary']
    if import_result_format not in import_result_formats:
        import_result_format = 'summary'
    ocl_api_headers = {'Content-Type': 'application/json'}
    if ocl_api_token:
        ocl_api_headers['Authorization'] = 'Token ' + ocl_api_token
    import_status_url = "%s/manage/bulkimport/?task=%s&result=%s" % (
        ocl_env_url, bulkImportId, import_result_format)
    response = requests.get(import_status_url, headers=ocl_api_headers)
    response.raise_for_status() #- see if raise for status can be implemented without conflict
    # check for if it is JSON or Text - if can't check test if it can be converted to json
    is_json = True
    try:
        json_object = json.loads(response.content)
    except ValueError as e:
        is_json = False
    if import_result_format == 'summary':
        if response.status_code == 200 and is_json == False:
            output_json = {
                "status": "Completed",
                "status_code": response.status_code,
                "message": response.content}
        elif (response.status_code == 200 or response.status_code == 202) and is_json == True:
            output_json = {
                "status": "Pending",
                "status_code": 202}
        else:
            output_json = {
                "status": "Failure",
                "status_code": response.status_code,
                "message": response.text}
    return json.dumps(output_json)


def getQMAPDomainDetails(ocl_env_url='', domain=''):
    """ get QMAP domain details """
    ocl_api_headers = {'Content-Type': 'application/json'}
    qmapDetailsURL = "%s/orgs/%s" % (
        ocl_env_url, domain)
    response = requests.get(qmapDetailsURL, headers=ocl_api_headers)
    response.raise_for_status()
    return response.text


def getDATIMCodeLists(ocl_env_url=''):
    """ get DATIM Codelist details """
    ocl_api_headers = {'Content-Type': 'application/json'}
    datimCodelistsDetailsURL = '%s/orgs/PEPFAR-Test8/collections/?collectionType="Code+List"&limit=0' % (
        ocl_env_url)
    response = requests.get(datimCodelistsDetailsURL, headers=ocl_api_headers)
    response.raise_for_status()
    return response.text


def getMOHCodelists(ocl_env_url=''):
    """ get MOH Codelists details """
    ocl_api_headers = {'Content-Type': 'application/json'}
    mohCodelistsDetailsURL = '%s/orgs/PEPFAR/sources/?extras__datim_moh_codelist=true&verbose=true' % (
        ocl_env_url)
    response = requests.get(mohCodelistsDetailsURL, headers=ocl_api_headers)
    response.raise_for_status()
    return response.text


# Configure
parser = argparse.ArgumentParser("bulkImportStatus", description="Get Bulk Import Status from OCL")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=common.ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('--bulkImportId', help='Bulk Import Status ID', required=False)
parser.add_argument('-t', '--token', help='OCL API token', required=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + common.APP_VERSION)
parser.add_argument(
    '--format', help='Format of bulk import results to return from OCL', default="summary")
parser.add_argument(
    '--domain', help='Domain of bulk import results to return from OCL', required=False)
parser.add_argument(
    '--requestType', help='Type of Passthrough Request', required=True)
parser.add_argument(
    '--country_code', help='Country Code', required=False)
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.env_url

# Display debug output
if args.verbosity > 1:
    print args

response = ''
try:
    if args.requestType == "bulkImportStatus":
        if not args.format:
            args.format = "json"
        response = check_bulk_import_status(
            bulkImportId=args.bulkImportId, ocl_env_url=ocl_env_url,
            ocl_api_token=args.token, import_result_format=args.format.upper())
    if args.requestType == "qmapDetails":
        response = getQMAPDomainDetails(ocl_env_url=ocl_env_url, domain=args.domain)
    if args.requestType == "datimCodelists":
        response = getDATIMCodeLists(ocl_env_url=ocl_env_url)
    if args.requestType == "mohCodelists":
        response = getMOHCodelists(ocl_env_url=ocl_env_url)
except Exception as e:
    output_json = {
        "status": "Error",
        "status_code": 500,
        "message": str(e)}
else:
    output_json = response

if output_json:
    print output_json
