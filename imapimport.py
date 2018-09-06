"""
Script to import a country mapping CSV for a specified country (e.g. UG) and
period (e.g. FY17, FY18). CSV must follow the format of the country mapping CSV template.

TODO:
- Encode error messages as text not JSON
"""
import sys
import settings
import datim.datimimap
import datim.datimimapimport


# Default Script Settings
csv_filename = '' # e.g. csv/RW-FY18.csv
country_name = '' # e.g. Rwanda
country_code = '' # e.g. RW
period = '' # e.g. FY18, FY19
verbosity = 2
run_ocl_offline = False
test_mode = False

# OCL Settings
oclenv = settings.ocl_api_url_staging
oclapitoken = settings.api_token_staging_datim_admin

# Optionally set arguments from the command line
if sys.argv and len(sys.argv) > 4:
    country_code = sys.argv[1]
    period = sys.argv[2]
    csv_filename = sys.argv[3]
    country_name = sys.argv[4]

# Pre-pocess input parameters
country_org = 'DATIM-MOH-%s' % country_code

# Debug output
if verbosity:
    print '\n\n*****************************************************************************************************'
    print '** [IMPORT] Country: %s (%s), Org: %s, CSV: %s, Period: %s, Exclude Empty Maps: %s, Verbosity: %s, Test Mode: %s' % (
        country_code, country_name, country_org, csv_filename, period, str(verbosity), str(test_mode))
    print '*****************************************************************************************************'

# Load i-map from CSV file
imap_input = datim.datimimap.DatimImapFactory.load_imap_from_csv(
    csv_filename=csv_filename, period=period,
    country_org=country_org, country_name=country_name, country_code=country_code)

# Run the import
imap_import = datim.datimimapimport.DatimImapImport(
    oclenv=oclenv, oclapitoken=oclapitoken, verbosity=verbosity,
    run_ocl_offline=run_ocl_offline, test_mode=test_mode)
imap_import.import_imap(imap_input=imap_input)

