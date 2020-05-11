"""
Returns a list of OCL organizations corresponding to QMAP domains or a list
of QMAP repositories for a specified QMAP domain. A QMAP repository in OCL is
stored as a source and represents a single QMAP submission.

Example Usage:
- Get list of orgs in OCL that correspond with a QMAP domain:
    python getqmaprepos.py --env=staging -v2 --format=text
- Get list of sources in OCL for a specified QMAP domain:
    python getqmaprepos.py --env=staging -v2 -d=mAtches3 --format=text

Arguments:
  -h, --help            show this help message and exit
  -d DOMAIN, --domain DOMAIN
                        QMAP domain ID, eg "mAtches"
  --env ENV             Name of the OCL API environment: production, staging,
                        demo, qa
  --envurl ENVURL       URL of the OCL API environment
  -t TOKEN, --token TOKEN
                        OCL API token
  --format FORMAT       Format of bulk import results to return from OCL
  -v VERBOSITY, --verbosity VERBOSITY
                        Verbosity level: 0 (default), 1, or 2
  --version             show program's version number and exit
"""
import json
import requests
import argparse
import iol


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


# Script argument parser
parser = argparse.ArgumentParser("qmap-repos", description="Get QMAP domains and repos from OCL")
parser.add_argument('-d', '--domain', help='QMAP domain ID, eg "mAtches"', required=False)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment: production, staging, demo, qa', type=ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('-t', '--token', help='OCL API token', required=False)
parser.add_argument(
    '--format', help='Format of bulk import results to return from OCL', default="text")
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + APP_VERSION)
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.env_url

# Display debug output
if args.verbosity > 1:
    print args


def get_qmap_domains(ocl_env_url, ocl_api_token):
    # Retreive list of all orgs from OCL
    # TODO: Implement OCL's custom attribute API filter when supported
    ocl_api_headers = {'Content-Type': 'application/json'}
    if ocl_api_token:
        ocl_api_headers['Authorization'] = 'Token ' + ocl_api_token
    url_all_orgs = '%s/orgs/?limit=0&verbose=true' % ocl_env_url
    response = requests.get(url_all_orgs, headers=ocl_api_headers)
    response.raise_for_status()
    ocl_all_orgs = response.json()

    # Filter orgs based on custom attributes and specified filters
    filtered_orgs = []
    for ocl_org in ocl_all_orgs:
        if 'extras' in ocl_org and ocl_org['extras'] and 'qmap_org' in ocl_org['extras'] and ocl_org['extras']['qmap_org']:
            filtered_orgs.append(ocl_org)
    return filtered_orgs


def get_qmap_sources(ocl_env_url, ocl_api_token, qmap_domain):
    # Verify that org is a qmap domain
    ocl_api_headers = {'Content-Type': 'application/json'}
    if ocl_api_token:
        ocl_api_headers['Authorization'] = 'Token ' + ocl_api_token
    url_qmap_domain = '%s/orgs/%s/' % (ocl_env_url, qmap_domain)
    response = requests.get(url_qmap_domain, headers=ocl_api_headers)
    response.raise_for_status()
    ocl_org_qmap_domain = response.json()
    if ('extras' in ocl_org_qmap_domain and ocl_org_qmap_domain['extras'] and
            'qmap_org' in ocl_org_qmap_domain['extras'] and ocl_org_qmap_domain['extras']['qmap_org']):
        pass
    else:
        raise Exception('OCL organization "%s" is not a QMAP domain' % qmap_domain)

    # Retreive list of sources for the specified org
    # TODO: Implement OCL's custom attribute API filter when supported
    url_qmap_sources = '%s/orgs/%s/sources/?limit=0&verbose=true' % (ocl_env_url, qmap_domain)
    response = requests.get(url_qmap_sources, headers=ocl_api_headers)
    response.raise_for_status()
    ocl_qmap_sources = response.json()

    # Filter orgs based on custom attributes and specified filters
    filtered_qmap_sources = []
    for ocl_source in ocl_qmap_sources:
        if 'extras' in ocl_source and ocl_source['extras'] and 'qmap_version' in ocl_source['extras'] and ocl_source['extras']['qmap_version']:
            filtered_qmap_sources.append(ocl_source)
    return filtered_qmap_sources

output_format = args.format.lower()

# Get the QMAP orgs or sources based on settings
if ocl_env_url and args.domain:
    result_type = 'Source'
    filtered_results = get_qmap_sources(ocl_env_url=ocl_env_url, ocl_api_token=args.token, qmap_domain=args.domain)
else:
    result_type = 'Organization'
    filtered_results = get_qmap_domains(ocl_env_url=ocl_env_url, ocl_api_token=args.token)

# Display the results
if result_type == 'Organization':
    if output_format == 'text':
        for ocl_org in filtered_results:
            print '%s' % (ocl_org['id'])
    elif output_format == 'csv':
        print iol.get_as_csv(
            filtered_results, start_columns=['id','name'],
            exclude_columns=['members_url', 'collections_url', 'sources_url', 'uuid', 'members'])
    else:
        print json.dumps(filtered_results)
elif result_type == 'Source':
    if output_format == 'text':
        for ocl_source in filtered_results:
            print '%s: %s %s (%s)' % (ocl_source['id'], ocl_source['name'], ocl_source['extras']['questionnaireuid'], ocl_source['extras']['qmap_version'])
    elif output_format == 'csv':
        print iol.get_as_csv(
            filtered_results, start_columns=['id', 'owner', 'full_name'],
            exclude_columns=['versions_url', 'uuid', 'mappings_url', 'concepts_url', 'custom_validation_schema', 'owne_type'])
    else:
        print json.dumps(filtered_results)
