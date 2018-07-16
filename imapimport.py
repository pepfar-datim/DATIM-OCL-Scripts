"""
Script to import a country mapping CSV for a specified country (e.g. UG) and
period (e.g. FY17). CSV must follow the format of the country mapping CSV template.
"""
import sys
import settings
from datim.datimimap import DatimImap, DatimImapFactory
from datim.datimimapimport import DatimImapImport


# Default Script Settings
csv_filename = 'csv/UG-FY17.csv'
country_code = 'UG'
period = 'FY17'
run_ocl_offline = False
verbosity = 2

# OCL Settings - JetStream Staging user=datim-admin
oclenv = settings.ocl_api_url_staging
oclapitoken = settings.api_token_staging_datim_admin

# Optionally set arguments from the command line
if sys.argv and len(sys.argv) > 2:
    country_code = sys.argv[1]
    period = sys.argv[2]

# Prepocess input parameters
country_org = 'DATIM-MOH-%s' % country_code

# Load i-map from CSV file
imap_input = DatimImapFactory.load_imap_from_csv(
	csv_filename=csv_filename, country_org=country_org,
	country_code=country_code, period=period)

# Run the import
imap_import = DatimImapImport(
	oclenv=oclenv, oclapitoken=oclapitoken, verbosity=verbosity, run_ocl_offline=run_ocl_offline)
imap_import.import_imap(imap_input=imap_input)
