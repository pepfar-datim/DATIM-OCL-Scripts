""" Common methods and functions for command-line python tools """
import argparse
import requests


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
    """ Return OCL environment URL for the specified enviroment key"""
    if string not in OCL_ENVIRONMENTS:
        raise argparse.ArgumentTypeError('Argument "env" must be %s' % (
            ', '.join(OCL_ENVIRONMENTS.keys())))
    return OCL_ENVIRONMENTS[string]


def get_imap_orgs(ocl_env_url, ocl_api_token, period_filter='', country_code_filter='',
                  verbose=False):
    """
    Returns list of country Indicator Mapping organizations available in the specified OCL
    environment. This is determined by the 'datim_moh_object' == True custom attribute of
    the org. Orgs typically have an ID in the format 'DATIM-MOH-xx-FYyy', where 'xx' is
    the country code (eg. CM, BI, UG) and 'yy' is the fiscal year (eg. 18, 19, 20).
    Optional arguments 'period_filter' and 'country_code_filter' may be either a string or
    a list and will filter the country list accordingly. For example, setting period_filter to
    ['FY18', 'FY19'] will only return IMAP orgs from those fiscal years. Similarly, setting
    country_code_filter to ['UG', 'BI', 'UA'] will only return those three matching
    country codes.
    """

    # Prepare the filters
    if period_filter:
        if not isinstance(period_filter, list):
            period_filter = [period_filter]
    if country_code_filter:
        if not isinstance(country_code_filter, list):
            country_code_filter = [country_code_filter]

    # Retrieve list of all orgs from OCL
    ocl_api_headers = {'Content-Type': 'application/json'}
    request_params = {
        'limit': '0',
        'verbose': 'true',
        'extras__datim_moh_object': 'true'
    }
    if period_filter:
        request_params['extras__datim_moh_period'] = ','.join(period_filter)
    if country_code_filter:
        request_params['extras__datim_moh_country_code'] = ','.join(country_code_filter)
    if ocl_api_token:
        ocl_api_headers['Authorization'] = 'Token ' + ocl_api_token
    url_all_orgs = '%s/orgs/' % ocl_env_url
    response = requests.get(url_all_orgs, headers=ocl_api_headers, params=request_params)
    if verbose:
        print response.url
    response.raise_for_status()
    ocl_all_orgs = response.json()
    return ocl_all_orgs
