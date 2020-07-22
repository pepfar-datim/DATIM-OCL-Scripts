"""
Returns list of country Indicator Mapping (IMAP) organizations available in the specified
OCL environment. This is determined by the 'datim_moh_object' == True custom attribute of
the org. Orgs typically have an ID in the format 'DATIM-MOH-xx-FYyy', where 'xx' is the
country code (eg. CM, BI, UG) and 'yy' is the fiscal year (eg. 18, 19, 20), though this is
not required by this script. Optional arguments 'period_filter' and 'country_code_filter'
may be either a string or a list and will filter the country list accordingly. For example,
setting period_filter to ['FY18', 'FY19'] will only return IMAP orgs from those fiscal years.
Similarly, setting country_code_filter to ['UG', 'BI', 'UA'] will only return those three
matching country codes.

Example Usage:
- To get list of all IMAP orgs (for all country codes and periods):
    python getimaporgs.py --env=staging -t="your-token-here" --format=text
- Filter list of IMAP orgs by period:
    python getimaporgs.py --env=staging -t="your-token-here" --format=text --period=FY18,FY19
- Filter list of IMAP orgs by country code:
    python getimaporgs.py --env=staging -t="your-token-here" --format=text --country_code=BI

Arguments:
  -h, --help            show this help message and exit
  -c COUNTRY_CODE, --country_code COUNTRY_CODE
                        Country code, eg "UG", "BI"
  --env ENV             Name of the OCL API environment: production, staging,
                        demo, qa
  --envurl ENVURL       URL of the OCL API environment
  -p PERIOD, --period PERIOD
                        Period, eg "FY18", "FY19"
  -t TOKEN, --token TOKEN
                        OCL API token
  -f FORMAT, --format FORMAT
                        Format of the export: csv, json, text
  -v VERBOSITY, --verbosity VERBOSITY
                        Verbosity level: 0 (default), 1, or 2
  --version             show program's version number and exit
"""
import json
import argparse
import iol
import common


# Script argument parser
parser = argparse.ArgumentParser("imap-orgs", description="Export IMAP country list from OCL")
parser.add_argument(
    '-c', '--country_code', help='Country code, eg "ETH", "KEN"', required=False, default='')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    '--env', help='Name of the OCL API environment: production, staging, demo, qa',
    type=common.ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument(
    '-p', '--period', help='Period, eg "FY18", "FY19"', required=False, default='')
parser.add_argument('-t', '--token', help='OCL API token', required=False)
parser.add_argument('-f', '--format', help='Format of the export: TEXT (default), CSV, JSON',
                    default='text', required=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + common.APP_VERSION)
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.env_url
if args.verbosity > 1:
    print args

# Prepare filters
period_filter = ''
if args.period:
    period_filter = [x.strip() for x in args.period.split(',')]
country_code_filter = ''
if args.country_code:
    country_code_filter = [x.strip() for x in args.country_code.split(',')]

# Get the IMAP orgs
ocl_imap_orgs = common.get_imap_orgs(
    ocl_env_url=ocl_env_url, ocl_api_token=args.token, verbose=bool(args.verbosity),
    period_filter=period_filter, country_code_filter=country_code_filter)

# Display the results
if isinstance(ocl_imap_orgs, list):
    output_format = args.format.lower()
    if output_format == 'csv':
        print iol.get_as_csv(
            ocl_imap_orgs, start_columns=['id', 'name'],
            exclude_columns=['members_url', 'collections_url', 'sources_url', 'uuid', 'members'])
    elif output_format == 'text':
        for ocl_org in ocl_imap_orgs:
            print '%s: %s %s %s' % (
                ocl_org['id'],
                ocl_org.get('location', ''),
                ocl_org['extras'].get('datim_moh_country_code'),
                ocl_org['extras'].get('datim_moh_period'))
    else:
        print json.dumps(ocl_imap_orgs)
