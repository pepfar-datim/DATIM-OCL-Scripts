"""
Script to export a QMAP from OCL. A QMAP is a JSON representation of a mapping between
a point of service system (POS) and a FHIR Questionnaire.

python exportqmap.py -dmAtches3 --qmapid=HIV-example-map --env=staging
"""
import argparse
import json
import ocldev.oclexport
import datim.qmap


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
parser = argparse.ArgumentParser("qmap", description="Export a QMAP into OCL")
parser.add_argument('-d', '--domain', help='ID of the QMAP domain', required=True)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('--qmapid', help='QMAP ID', required=True)
parser.add_argument('-t', '--token', help='OCL API token', required=False)
# parser.add_argument('--testmode', action="store_true", help='Enable test mode', default=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + APP_VERSION)
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.env_url

# Display debug output
if args.verbosity > 1:
    import pprint
    print args

# Process the qmap export
try:
    qmap = datim.qmap.Qmap.export_qmap(
        domain=args.domain, qmap_id=args.qmapid,
        ocl_env_url=ocl_env_url, ocl_api_token=args.token,
        verbosity=args.verbosity)
    # Export successful -- Return 200 status code and print this
    print qmap
except ocldev.oclexport.OclExportNotAvailableError as export_error:
    # Export not yet ready -- Return 204 status code
    print json.dumps({
        "message": "QMAP export not available. Try requesting again later."
    })
