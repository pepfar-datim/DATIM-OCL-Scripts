"""
Script to retrieve a backup of all matching IMAPs
"""
import sys
import json
import argparse
import requests
import common
import datim.datimimapexport


# Script argument parser
parser = argparse.ArgumentParser("imap-orgs", description="Backup IMAPs in OCL environment")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    '--env', help='Name of the OCL API environment: production, staging, demo, qa',
    type=common.ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('-t', '--token', help='OCL API token')
parser.add_argument(
    '-c', '--country_code', help='Country code, eg "ETH", "KEN"', required=False, default='')
parser.add_argument('-p', '--period', help='Period, eg "FY18", "FY19"', required=False, default='')
parser.add_argument(
    '-f', '--format', help='Format of the export: CSV (default), JSON, XML, HTML',
    default=datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV, required=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument(
    '--exclude_empty_maps', help='Excludes empty maps', default=True, required=False)
parser.add_argument(
    '--include_extra_info', help='Includes extra IMAP columns', default=False, required=False)
parser.add_argument('--version', action='version', version='%(prog)s v' + common.APP_VERSION)
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.env_url
if args.verbosity > 1:
    print args
    print 'ocl_env_url=%s' % ocl_env_url

# Prepare filters
period_filter = ''
if args.period:
    period_filter = [x.strip() for x in args.period.split(',')]
country_code_filter = ''
if args.country_code:
    country_code_filter = [x.strip() for x in args.country_code.split(',')]
imap_export_verbosity = 1 if args.verbosity > 1 else 0

# Get the list of IMAP orgs
ocl_imap_orgs = common.get_imap_orgs(
    ocl_env_url=ocl_env_url, ocl_api_token=args.token, verbose=bool(args.verbosity),
    period_filter=period_filter, country_code_filter=country_code_filter)

# Export each one
imap_backups = []
imap_export = datim.datimimapexport.DatimImapExport(
    oclenv=ocl_env_url, oclapitoken=args.token, verbosity=imap_export_verbosity)
for org in ocl_imap_orgs:
    # Print debug info
    if args.verbosity:
        print '\n\n' + '*' * 100
        print '** [EXPORT] Org: %s, Country Code: %s, Country Name: %s, Format: %s, Period: %s, Exclude Empty Maps: %s, Verbosity: %s' % (
            org['id'], org['extras']['datim_moh_country_code'], org['name'], args.format,
            org['extras']['datim_moh_period'], str(args.exclude_empty_maps), str(args.verbosity))
        print '*' * 100

    # Generate the IMAP export
    try:
        imap = imap_export.get_imap(
            period=org['extras']['datim_moh_period'], country_org=org['id'],
            country_code=org['extras']['datim_moh_country_code'])
    except (requests.exceptions.HTTPError, Exception) as e:
        output = {
            'status': 'Error',
            'message': str(e)
        }
        print json.dumps(output)
        sys.exit(1)
    else:
        if args.verbosity:
            imap.display(fmt=args.format, sort=True, exclude_empty_maps=args.exclude_empty_maps,
                         include_extra_info=args.include_extra_info)
        imap_backups.append({
            'country_org': org['id'],
            'country_code': org['extras'].get('datim_moh_country_code'),
            'country_name': org.get('location'),
            'period': org['extras'].get('datim_moh_period'),
            'imap': imap.get_imap_data(
                sort=True,
                exclude_empty_maps=args.exclude_empty_maps,
                include_extra_info=args.include_extra_info)
        })

if args.verbosity:
    print '\n%s IMAPs exported successfully' % len(imap_backups)
else:
    print json.dumps(imap_backups)
