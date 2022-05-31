"""
Script to present DATIM MOH metadata -
NOTE This only shows the global PEPFAR DATIM MOH Alignment indicators, without mapping to country indicators.
To view country mappings, use the IMAP export script and mediator.

Supported Formats: html, xml, csv, json
OpenHIM Mediator Request Format: /datim-moh?period=____&format=____

This script fetches an export from OCL for the latest released version of the specified
collection. If it seems like you're looking at old data, check the collection version first.
"""
import argparse
import json

import requests

import common
import iol
from datim import datimshowmoh

# Script argument parser
parser = argparse.ArgumentParser("moh", description="Export MOH data from OCL")
parser.add_argument('-f', '--format', help='Format of Export', default='csv')
parser.add_argument('-p', '--period', help='Period of MOH Export')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=common.ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('-t', '--token', help='OCL API token', required=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + common.APP_VERSION)
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.env_url

if args.period:
    # Create Show object and run -- returns a single codelist
    datim_show = datimshowmoh.DatimShowMoh(
        oclenv=ocl_env_url, oclapitoken=args.token, verbosity=args.verbosity)
    datim_show.get(period=args.period, export_format=args.format)
else:
    # Just return the list of periods available in the target environment
    url_sources = '%s/orgs/%s/sources/?verbose=true&extras.datim_moh_codelist=true&limit=100' % (ocl_env_url, 'PEPFAR')
    ocl_api_headers = {'Content-Type': 'application/json'}
    if args.token:
        ocl_api_headers['Authorization'] = 'Token %s' % args.token
    response = requests.get(url_sources, headers=ocl_api_headers)
    response.raise_for_status()
    ocl_moh_sources = response.json()
    output_format = args.format.lower()
    if output_format == 'csv':
        print(iol.get_as_csv(
            ocl_moh_sources, start_columns=['attr:Period', 'id'],
            include_columns=['attr:Period', 'id', 'url']))
    elif output_format == 'text':
        for ocl_moh_source in ocl_moh_sources:
            datim_moh_period = None
            if 'extras' in ocl_moh_source and ocl_moh_source['extras']:
                datim_moh_period = ocl_moh_source['extras'].get('Period')
            print('%s: %s' % (
                datim_moh_period, ocl_moh_source['id']))
    else:
        print(json.dumps(ocl_moh_sources, indent=4))
