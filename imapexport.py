"""
Script to generate a country mapping export for a specified country (e.g. UG) and
period (e.g. FY17). Export follows the format of the country mapping CSV template,
though JSON format is also supported.
"""
import sys
import settings
import requests
import datim.datimimap
import datim.datimimapexport
from import_manager import has_existing_import
import json


# Default Script Settings
country_code = ''  # e.g. RW, LS, etc.
export_format = datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_JSON  # CSV, JSON and HTML are supported
period = ''  # e.g. FY17, FY18, etc.
exclude_empty_maps = True
include_extra_info = False
verbosity = 0
run_ocl_offline = False

# OCL Settings - JetStream Staging user=datim-admin
oclenv = settings.ocl_api_url_staging
oclapitoken = settings.api_token_staging_datim_admin

# Optionally set arguments from the command line
if sys.argv and len(sys.argv) > 6:
    country_code = sys.argv[1]
    export_format = datim.datimimapexport.DatimImapExport.get_format_from_string(sys.argv[2])
    if sys.argv[3] == 'default':
        period = ''
    else:
        period = sys.argv[3]
    verbosity = int(sys.argv[4])
    if sys.argv[5].lower() == 'true':
        exclude_empty_maps = True
    else:
        exclude_empty_maps = False
    if sys.argv[6].lower() == 'true':
        include_extra_info = True
    else:
        include_extra_info = False

# Exit if import is already in process
# TODO: Fix this so that it is automatically skipped if not run in an async environment
if has_existing_import(country_code):
    response = {
            'status_code': 409,
            'result': 'There is an import already in progress for this country code'
        }
    print json.dumps(response)
    sys.exit(1)

# Pre-process input parameters
country_org = 'DATIM-MOH-%s-%s' % (country_code, period)

# Debug output
if verbosity:
    print('\n\n' + '*' * 100)
    print('** [EXPORT] Country Code: %s, Org: %s, Format: %s, Period: %s, Exclude Empty Maps: %s, Verbosity: %s' % (
        country_code, country_org, export_format, period, str(exclude_empty_maps), str(verbosity)))
    print('*' * 100)

# Generate the IMAP export
datim_imap_export = datim.datimimapexport.DatimImapExport(
    oclenv=oclenv, oclapitoken=oclapitoken, verbosity=verbosity, run_ocl_offline=run_ocl_offline)
try:
    imap = datim_imap_export.get_imap(period=period, country_org=country_org, country_code=country_code)
except requests.exceptions.HTTPError as e:
    print(e)
    sys.exit(1)
else:
    imap.display(fmt=export_format, sort=True, exclude_empty_maps=exclude_empty_maps,
                 include_extra_info=include_extra_info)
