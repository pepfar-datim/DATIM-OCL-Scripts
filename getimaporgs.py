import requests
import datim.datimimap
import datim.datimimapexport
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
parser = argparse.ArgumentParser("imap-orgs", description="Export IMAP country list from OCL")
parser.add_argument('-c', '--country_code', help='Country code', required=False, default='')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('-p','--period', help='Period', required=False, default='')
parser.add_argument('-t', '--token', help='OCL API token', required=False)
parser.add_argument('-f', '--format', help='Format of the export',
                    default=datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV, required=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + APP_VERSION)
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.env_url

# Display debug output
if args.verbosity > 1:
    print args


def get_imap_orgs(ocl_env_url, ocl_api_token, period_filter='', country_code_filter=''):
    """
    Returns list of country Indicator Mapping organizations available in the specified OCL environment.
    This is determined by the 'datim_moh_object' == True custom attribute of the org. Orgs typically have an
    ID in the format 'DATIM-MOH-xx-FYyy', where 'xx' is the country code (eg. CM, BI, UG) and 'yy' is the
    fiscal year (eg. 18, 19, 20). Optional arguments 'period_filter' and 'country_code_filter' may be either
    a string or a list and will filter the country list accordingly. For example, setting period_filter to
    ['FY18', 'FY19'] will only return IMAP orgs from those fiscal years. Similarly, setting country_code_filter
    to ['UG', 'BI', 'UA'] will only return those three matching country codes.
    """

    # Prepare the filters
    if period_filter:
        if not isinstance(period_filter, list):
            period_filter = [period_filter]
    if country_code_filter:
        if not isinstance(country_code_filter, list):
            country_code_filter = [country_code_filter]

    ocl_api_headers = {'Content-Type': 'application/json'}
    if ocl_api_token:
        ocl_api_headers['Authorization'] = 'Token ' + ocl_api_token
    url_all_orgs = '%s/orgs/?limit=0&verbose=true' % ocl_env_url
    response = requests.get(url_all_orgs, headers=ocl_api_headers)
    response.raise_for_status()
    ocl_all_orgs = response.json()
    ocl_moh_orgs = []
    for ocl_org in ocl_all_orgs:
        if 'extras' in ocl_org and ocl_org['extras'] and 'datim_moh_object' in ocl_org['extras'] and ocl_org['extras']['datim_moh_object']:
            is_match = True
            if period_filter:
                if 'datim_moh_period' in ocl_org['extras'] and ocl_org['extras']['datim_moh_period'] in period_filter:
                    pass
                else:
                    is_match = False
            if is_match and country_code_filter:
                if 'datim_moh_country_code' in ocl_org['extras'] and ocl_org['extras']['datim_moh_country_code'] in country_code_filter:
                    pass
                else:
                    is_match = False
            if is_match:
                ocl_moh_orgs.append(ocl_org)
    return ocl_moh_orgs


# Prepare filters
period_filter = ''
if args.period:
    period_filter = [x.strip() for x in args.period.split(',')]
country_code_filter = ''
if args.country_code:
    country_code_filter = [x.strip() for x in args.country_code.split(',')]

# Get the orgs
ocl_imap_orgs = get_imap_orgs(
    ocl_env_url=ocl_env_url, ocl_api_token=args.token,
    period_filter=period_filter, country_code_filter=country_code_filter)

# Display
if isinstance(ocl_imap_orgs, list):
    output_format = args.format.lower()
    if output_format == 'csv':
        if ocl_imap_orgs:
            csv_columns = ocl_imap_orgs[0].keys()
            csv_columns.remove('extras')
            csv_columns.remove('id')
            csv_columns.remove('name')
            csv_columns.insert(0, 'name')
            csv_columns.insert(0, 'id')
            for ocl_org in ocl_imap_orgs:
                for attr_key in ocl_org['extras']:
                    if ('attr:%s' % attr_key) not in csv_columns:
                        csv_columns.append('attr:%s' % attr_key)
            import csv
            import io
            output_stream = io.BytesIO()
            writer = csv.DictWriter(output_stream, fieldnames=csv_columns)
            writer.writeheader()
            for ocl_org in ocl_imap_orgs:
                csv_row = ocl_org.copy()
                del csv_row['extras']
                for attr_key in ocl_org['extras']:
                    csv_row['attr:%s' % attr_key] = ocl_org['extras'][attr_key]
                writer.writerow(csv_row)
            print output_stream.getvalue().strip('\r\n')
    elif output_format == 'text':
        for ocl_org in ocl_imap_orgs:
            print '%s: %s %s %s' % (
                ocl_org['id'],
                ocl_org['location'],
                ocl_org['extras'].get('datim_moh_country_code'),
                ocl_org['extras'].get('datim_moh_period'))
    else:
        print json.dumps(ocl_imap_orgs)
