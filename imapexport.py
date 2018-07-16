"""
Script to generate a country mapping export for a specified country (e.g. UG) and
period (e.g. FY17). Export follows the format of the country mapping CSV template,
though JSON format is also supported.
"""
import sys
import settings
from datim.datimimapexport import DatimImapExport
from datim.datimimap import DatimImap


# Default Script Settings
country_code = 'LS'
export_format = DatimImap.DATIM_IMAP_FORMAT_CSV
period = 'FY17'
run_ocl_offline = False
verbosity = 0

# OCL Settings - JetStream Staging user=datim-admin
oclenv = settings.ocl_api_url_staging
oclapitoken = settings.api_token_staging_datim_admin

# Optionally set arguments from the command line
if sys.argv and len(sys.argv) > 3:
    country_code = sys.argv[1]
    export_format = DatimImapExport.get_format_from_string(sys.argv[2])
    period = sys.argv[3]

# Prepocess input parameters
country_org = 'DATIM-MOH-%s' % country_code

# Generate the imap export
datim_imap_export = DatimImapExport(
	oclenv=oclenv, oclapitoken=oclapitoken, verbosity=verbosity, run_ocl_offline=run_ocl_offline)
imap = datim_imap_export.get_imap(period=period, country_org=country_org, country_code=country_code)
imap.display(export_format)
