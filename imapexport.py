"""
Script to generate a country mapping export for a specified country (e.g. UG) and
period (e.g. FY17). Export follows the format of the country mapping CSV template,
though JSON format is also supported.

Example Usage:
* To request an export as CSV:
    python imapexport.py -c="BI" --env=staging -p=FY19 -t="my-token-here" -v0 -f=CSV
* To request an export as JSON:
    python imapexport.py -c="BI" --env=staging -p=FY19 -t="my-token-here" -v0 -f=JSON
* To see all options:
    python imapexport.py -h

"""
import sys
import requests
import datim.datimimap
import datim.datimimapexport
from import_manager import has_existing_import
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
        raise argparse.ArgumentTypeError('Argument "env" must be %s' % ', '.join(OCL_ENVIRONMENTS.keys()))
    return OCL_ENVIRONMENTS[string]


# Script argument parser
parser = argparse.ArgumentParser("imap-export", description="Export IMAP from OCL")
parser.add_argument('-c', '--country_code', help='Country code, eg "UG", "BI"', required=True)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    '--env', help='Name of the OCL API environment: production, staging, demo, qa',
    type=ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('-p', '--period', help='Period, eg "FY18"', required=True)
parser.add_argument('-t', '--token', help='OCL API token', required=False, default='')
parser.add_argument(
    '-f', '--format', help='Format of the export: JSON, XML, CSV, HTML',
    default=datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV, required=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument(
    '--country_version', help='Country version number (e.g. v0, v1, v2, etc.)',
    default='', required=False)
parser.add_argument(
    '--exclude_empty_maps', help='Excludes empty maps', default=True, required=False)
parser.add_argument(
    '--include_extra_info', help='Includes extra columns', default=False, required=False)
parser.add_argument(
    '--run_ocl_offline', help='Runs OCL offline', default=False, required=False)
parser.add_argument('--version', action='version', version='%(prog)s v' + APP_VERSION)
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.env_url

# Display debug output
if args.verbosity > 1:
    print args

# Exit if import is already in process
# TODO: Fix this so that it is automatically skipped if not run in an async environment
# if has_existing_import(args.country_code):
#     response = {
#             'status_code': 409,
#             'result': 'There is an import already in progress for this country code'
#         }
#     print json.dumps(response)
#     sys.exit(1)

# Pre-process input parameters
country_org = 'DATIM-MOH-%s-%s' % (args.country_code, args.period)

# Debug output
if args.verbosity:
    print('\n\n' + '*' * 100)
    print('** [EXPORT] Country Code: %s, Org: %s, Format: %s, Period: %s, Version: %s, Exclude Empty Maps: %s, Verbosity: %s' % (
        args.country_code, country_org, args.format, args.period, args.country_version, str(args.exclude_empty_maps), str(args.verbosity)))
    print('*' * 100)

# Generate the IMAP export
datim_imap_export = datim.datimimapexport.DatimImapExport(
    oclenv=ocl_env_url, oclapitoken=args.token, verbosity=args.verbosity,
    run_ocl_offline=args.run_ocl_offline)
try:
    imap = datim_imap_export.get_imap(
        period=args.period, version=args.country_version, country_org=country_org,
        country_code=args.country_code)
except (requests.exceptions.HTTPError, Exception) as e:
    output = {
        'status': 'Error',
        'message': str(e)
    }
    print json.dumps(output)
    sys.exit(1)
else:
    imap.display(fmt=args.format, sort=True, exclude_empty_maps=args.exclude_empty_maps,
                 include_extra_info=args.include_extra_info)
