"""
Script to import a country mapping CSV for a specified country (e.g. UG) and
period (e.g. FY17, FY18). CSV must follow the format of the country mapping CSV template.
"""
import sys
import time
import settings
import datim.datimimap
import datim.datimimapimport


# Default Script Settings
country_code = ''  # e.g. RW
period = ''  # e.g. FY18, FY19
csv_filename = ''  # e.g. csv/RW-FY18.csv
country_name = ''  # e.g. Rwanda
verbosity = 2  # Set to 0 to hide all debug info, or 2 to show all debug info
run_ocl_offline = False  # Not currently supported
test_mode = False  # If true, generates the import script but does not actually import it
delete_org_if_exists = False  # Be very careful with this option!

# OCL Settings
oclenv = settings.ocl_api_url_staging
oclapitoken = settings.api_token_staging_datim_admin

# Optionally set arguments from the command line
if sys.argv and len(sys.argv) > 5:
    country_code = sys.argv[1]
    period = sys.argv[2]
    csv_filename = sys.argv[3]
    country_name = sys.argv[4]
    if sys.argv[5].lower() == 'true':
        test_mode = True

# Pre-pocess input parameters
country_org = 'DATIM-MOH-%s' % country_code
country_names = {
        "BW": "Botswana",
        "BI": "Burundi",
        "CM": "Cameroon",
        "CI": "Cote d'Ivoire",
        "CD": "Democratic Republic of the Congo",
        "SZ": "Eswatini",
        "ET": "Ethiopia",
        "HT": "Haiti",
        "KE": "Kenya",
        "LS": "Lesotho",
        "MW": "Malawi",
        "MZ": "Mozambique",
        "NA": "Namibia",
        "NG": "Nigeria",
        "RW": "Rwanda",
        "ZA": "South Africa",
        "SS": "South Sudan",
        "TZ": "Tanzania",
        "UG": "Uganda",
        "UA": "Ukraine",
        "VN": "Vietnam",
        "ZM": "Zambia",
        "ZW": "Zimbabwe",
    }
if not country_name and country_code in country_names:
    country_name = country_names[country_code]

# Debug output
if verbosity:
    print('\n\n' + '*' * 100)
    print('** [IMPORT] Country: %s (%s), Org: %s, CSV: %s, Period: %s, Verbosity: %s, Test Mode: %s' % (
        country_code, country_name, country_org, csv_filename, period, str(verbosity), str(test_mode)))
    print('*' * 100)

# (Optionally) Delete org if it exists
if delete_org_if_exists:
    if verbosity:
        print('"delete_org_if_exists" is set to True:')
    if not test_mode:
        if verbosity:
            print('Deleting org "%s" if it exists in 10 seconds...' % country_org)
        # Pause briefly to allow user to cancel in case deleting org on accident...
        time.sleep(10)
        result = datim.datimimap.DatimImapFactory.delete_org_if_exists(
            org_id=country_org, oclenv=oclenv, ocl_root_api_token=settings.api_token_staging_root)
        if verbosity:
            if result:
                print('Org successfully deleted.')
            else:
                print('Org does not exist.')
    elif verbosity:
        print('Skipping "delete_org_if_exists" step because in "test_mode"')

# Load i-map from CSV file
imap_input = datim.datimimap.DatimImapFactory.load_imap_from_csv(
    csv_filename=csv_filename, period=period,
    country_org=country_org, country_name=country_name, country_code=country_code)
if verbosity and imap_input:
    print('IMAP CSV file "%s" loaded successfully' % csv_filename)
elif not imap_input:
    print('Unable to load IMAP CSV file "%s"' % csv_filename)
    exit(1)

# Run the import
imap_import = datim.datimimapimport.DatimImapImport(
    oclenv=oclenv, oclapitoken=oclapitoken, verbosity=verbosity,
    run_ocl_offline=run_ocl_offline, test_mode=test_mode)
imap_import.import_imap(imap_input=imap_input)
