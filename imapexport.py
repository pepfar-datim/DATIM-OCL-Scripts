"""
Script to generate a country mapping export for a specified country (e.g. UG) and
period (e.g. FY17). Export follows the format of the country mapping CSV template,
though JSON format is also supported.

TODO:
- Content type returned should be text if an error occurs
"""
import sys
import settings
import requests
import datim.datimimap
import datim.datimimapexport


# Default Script Settings
country_code = 'RW3jy' # e.g. RW, LS, etc.
export_format = datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV
period = 'FY18'
exclude_empty_maps = True
verbosity = 0
run_ocl_offline = False

# OCL Settings - JetStream Staging user=datim-admin
oclenv = settings.ocl_api_url_staging
oclapitoken = settings.api_token_staging_datim_admin

# Optionally set arguments from the command line
if sys.argv and len(sys.argv) > 3:
    country_code = sys.argv[1]
    export_format = datim.datimimapexport.DatimImapExport.get_format_from_string(sys.argv[2])
    period = sys.argv[3]

# Prepocess input parameters
country_org = 'DATIM-MOH-%s' % country_code

# Generate the imap export
datim_imap_export = datim.datimimapexport.DatimImapExport(
	oclenv=oclenv, oclapitoken=oclapitoken, verbosity=verbosity, run_ocl_offline=run_ocl_offline)
try:
	imap = datim_imap_export.get_imap(period=period, country_org=country_org, country_code=country_code)
except requests.exceptions.HTTPError:
	print 'ERROR: Unrecognized country code "%s" for period "%s"' % (country_code, period)
	sys.exit(1)
else:
	imap.display(export_format, exclude_empty_maps=exclude_empty_maps)
