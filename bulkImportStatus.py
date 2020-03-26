"""
Script to get a BulkImport status from OCL. 

python bulkImpostStatus.py --bulkImportId=d6ac3dd2-e565-455c-b29a-f7d859fd1fe8-datim-admin --env=staging
"""
import requests
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
        raise argparse.ArgumentTypeError('Argument "env" must be %s' % ', '.join(OCL_ENVIRONMENTS.keys()))
    return OCL_ENVIRONMENTS[string]
parser = argparse.ArgumentParser("bulkImportStatus", description="Get Bulk Import Status from OCL")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('--bulkImportId', help='Bulk Import Status ID', required=True)
parser.add_argument('-t', '--token', help='OCL API token', required=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + APP_VERSION)
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.env_url

# Display debug output
if args.verbosity > 1:
    import pprint
    print args

def check_bulk_import_status(bulkImportId='', ocl_env_url='', ocl_api_token=''):
        ocl_api_headers = {'Content-Type': 'application/json'}
        if ocl_api_token:
            ocl_api_headers['Authorization'] = 'Token ' + ocl_api_token
        response = requests.get("https://api.staging.openconceptlab.org/manage/bulkimport/?task="+bulkImportId+"&result=report", headers=ocl_api_headers)
        try:
            response.raise_for_status()
        except:
            return False
        return response.text


status = check_bulk_import_status (bulkImportId=args.bulkImportId,ocl_env_url=ocl_env_url,ocl_api_token=args.token)
print(status)