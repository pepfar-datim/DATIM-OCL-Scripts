"""
Script to present DATIM MOH metadata -
NOTE This only shows the global PEPFAR DATIM MOH Alignment indicators, without mapping to country indicators.
To view country mappings, use the IMAP export script and mediator.

Supported Formats: html, xml, csv, json
OpenHIM Mediator Request Format: /datim-moh?period=____&format=____

This script fetches an export from OCL for the latest released version of the specified collection.
If it seems like you're looking at old data, check the collection version first.
"""
import sys
import settings
import datim.datimshow
import datim.datimshowmoh
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
parser = argparse.ArgumentParser("moh", description="Export MOH data from OCL")
parser.add_argument('-f', '--format', help='Format of Export', required=True)
parser.add_argument('-p', '--period', help='Period of MOH Export', required=True)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('-t', '--token', help='OCL API token', required=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + APP_VERSION)
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.env_url
# Default Script Settings
verbosity = 0  # 0=none, 1=some, 2=all
run_ocl_offline = False  # Set to true to use local copies of ocl exports
export_format = datim.datimshow.DatimShow.DATIM_FORMAT_CSV
period = ''  # e.g. FY18, FY19

# OCL Settings - JetStream Staging user=datim-admin
#oclenv = settings.oclenv
#oclapitoken = settings.oclapitoken

# Create Show object and run
datim_show = datim.datimshowmoh.DatimShowMoh(
    oclenv=ocl_env_url, oclapitoken=args.token, run_ocl_offline=run_ocl_offline, verbosity=args.verbosity)
datim_show.get(period=args.period, export_format=args.format)
