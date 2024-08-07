"""
Script to retrieve QMAP status from OCL and return as JSON.
Designed to work with PEPFAR IMAP and QMAP resources.

python oclPassThroughRequests.py --env=staging -t="{token_here}" --bulkImportId="{bulkimportid}" --requestType="bulkImportStatus"
"""
import argparse
import json

import requests

import common

# Checks OCL bulk import status
def check_bulk_import_status(bulkImportId='', ocl_env_url='', ocl_api_token='',
                             import_result_format=''):
    """ Return bulk import status ID """
    ocl_api_headers = {'Content-Type': 'application/json'}
    if ocl_api_token:
        ocl_api_headers['Authorization'] = 'Token ' + ocl_api_token
    import_status_url = (f"{ocl_env_url}/manage/bulkimport/?task={bulkImportId}"
                         f"&result={import_result_format}")
    response = requests.get(import_status_url, headers=ocl_api_headers)
    response.raise_for_status()
    try:
        output_json = response.json()
        output_json["status_code"] = response.status_code
        output_json["status"] = output_json["state"].capitalize()
        if output_json["status"] == 'Started':
            output_json["status"] = 'Pending'
        elif output_json["status"] == 'Success':
            output_json["status"] = 'Completed'
    except Exception as e:
        output_json = {
            "status": "Failure",
            "status_code": response.status_code,
            "message": response.text}
    return json.dumps(output_json)



def getQMAPDomainDetails(ocl_env_url='', domain=''):
    """ get QMAP domain details """
    ocl_api_headers = {'Content-Type': 'application/json'}
    qmapDetailsURL = f"{ocl_env_url}/orgs/{domain}"
    response = requests.get(qmapDetailsURL, headers=ocl_api_headers)
    response.raise_for_status()
    return response.text


def getDATIMCodeLists(ocl_env_url='',owner=''):
    """ get DATIM Codelist details """
    ocl_api_headers = {'Content-Type': 'application/json'}
    datimCodelistsDetailsURL = (f'{ocl_env_url}/orgs/{owner}/collections/?'
                                f'collectionType=Code List&q=&limit=400')
    response = requests.get(datimCodelistsDetailsURL, headers=ocl_api_headers)
    response.raise_for_status()
    return response.text


def getMOHCodelists(ocl_env_url='',owner=''):
    """ get MOH Codelists details """
    ocl_api_headers = {'Content-Type': 'application/json'}
    mohCodelistsDetailsURL = (f'{ocl_env_url}/orgs/{owner}/sources/?'
                              f'extras.datim_moh_codelist=true&verbose=true')
    response = requests.get(mohCodelistsDetailsURL, headers=ocl_api_headers)
    response.raise_for_status()
    return response.text


# Configure
parser = argparse.ArgumentParser(
    "OCL Passthrough", description="Set of passthrough requests to direct ocl api endpoints")
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
parser.add_argument(
    '--owner', help='owner', required=False)
args = parser.parse_args()

ocl_env_url = args.env if args.env else args.env_url

# Display debug output
if args.verbosity > 1:
    print(args)

response = ''
try:
    if args.requestType == "bulkImportStatus":
        if not args.format:
            args.format = "json"
        response = check_bulk_import_status(
            bulkImportId=args.bulkImportId, ocl_env_url=ocl_env_url,
            ocl_api_token=args.token, import_result_format=args.format.upper())
    elif args.requestType == "qmapDetails":
        response = getQMAPDomainDetails(ocl_env_url=ocl_env_url, domain=args.domain)
    elif args.requestType == "datimCodelists":
        response = getDATIMCodeLists(ocl_env_url=ocl_env_url, owner=args.owner)
    elif args.requestType == "mohCodelists":
        response = getMOHCodelists(ocl_env_url=ocl_env_url, owner=args.owner)
except Exception as e:
    output_json = {
        "status": "Error",
        "status_code": 500,
        "message": str(e)}
else:
    output_json = response

if output_json:
    print(output_json)
