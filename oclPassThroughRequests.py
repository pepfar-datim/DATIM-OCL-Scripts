"""
Script to retrieve QMAP status from OCL and return as JSON.
Designed to work with PEPFAR IMAP and QMAP resources.

python bulkImportStatus.py --env=staging -t="my-api-token-here"
    --bulkImportId=d6ac3dd2-e565-455c-b29a-f7d859fd1fe8-datim-admin
"""
import requests
import json
import argparse

# Script constants
APP_VERSION = '0.1.0'
OCL_ENVIRONMENTS = {
    'qa': 'https://api.qa.openconceptlab.org',
    'staging': 'https://api.staging.openconceptlab.org',
    'production': 'https://api.openconceptlab.org',
    'demo': 'https://api.demo.openconceptlab.org',
}


# Argument parser validation functions
def ocl_environment(string):
    if string not in OCL_ENVIRONMENTS:
        raise argparse.ArgumentTypeError(
            'Argument "env" must be %s' % ', '.join(OCL_ENVIRONMENTS.keys()))
    return OCL_ENVIRONMENTS[string]


# Checks OCL bulk import status
def check_bulk_import_status(bulkImportId='', ocl_env_url='', ocl_api_token='',
                             import_result_format=''):
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
    if import_result_format == 'summary':
        output_json = {
            "status": "Success",
            "status_code": response.status_code,
            "message": response.text
        }
    return output_json

# get QMAP domain details
def getQMAPDomainDetails(ocl_env_url='', domain=''):
    ocl_api_headers = {'Content-Type': 'application/json'}
    qmapDetailsURL = "%s/orgs/%s" % (
        ocl_env_url, domain)
    response = requests.get(qmapDetailsURL, headers=ocl_api_headers)
    response.raise_for_status()
    return response.text

# get MOH Codelist details
def getMOHCodeLists(ocl_env_url=''):
    ocl_api_headers = {'Content-Type': 'application/json'}
    mohCodelistsDetailsURL = '%s/orgs/PEPFAR-Test7/collections/?collectionType="Code+List"' % (
        ocl_env_url)
    response = requests.get(mohCodelistsDetailsURL, headers=ocl_api_headers)
    response.raise_for_status()
    return response.text

# get MOH Sources details
def getMOHSources(ocl_env_url=''):
    ocl_api_headers = {'Content-Type': 'application/json'}
    mohCodelistsDetailsURL = '%s/orgs/PEPFAR/sources/?extras__datim_moh_codelist=true&verbose=true' % (
        ocl_env_url)
    response = requests.get(mohCodelistsDetailsURL, headers=ocl_api_headers)
    response.raise_for_status()
    return response.text


# Configure
parser = argparse.ArgumentParser("bulkImportStatus", description="Get Bulk Import Status from OCL")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('--bulkImportId', help='Bulk Import Status ID', required=False)
parser.add_argument('-t', '--token', help='OCL API token', required=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + APP_VERSION)
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
    if (args.requestType=="bulkImportStatus"):
        if not args.format:
            args.format="json"
        response = check_bulk_import_status(
            bulkImportId=args.bulkImportId, ocl_env_url=ocl_env_url,
            ocl_api_token=args.token, import_result_format=args.format)
    if (args.requestType=="qmapDetails"):
        response = getQMAPDomainDetails(ocl_env_url=ocl_env_url, domain=args.domain)
    if (args.requestType=="mohCodeLists"):
        response = getMOHCodeLists(ocl_env_url=ocl_env_url)
    if (args.requestType=="mohSources"):
        response = getMOHSources(ocl_env_url=ocl_env_url)
except Exception as e:
    output_json = {
        "status": "Error",
        "status_code": response.status_code,
        "message": response.text
    }
else:
    output_json = response

if output_json:
    print output_json
