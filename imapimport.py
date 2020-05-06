"""
Script to import a CSV or JSON country mapping import file into OCL for a specified country
(e.g. UG) and period (e.g. FY17, FY18, FY19). Import file must follow the format of the country
mapping template.

Example Usage:
- Import IMAP:
    python imapimport.py --env=staging -c=BI --country_name=Burundi -p=FY19 -v2
        -t="your-token-here" csv/BI-FY19.csv
- Import IMAP and delete existing IMAP org if it exists:
    python imapimport.py --env=staging -c=BI --country_name=Burundi -p=FY19 -v2
        -t="your-token-here" --admin-token="your-admin-token-here" --delete_existing_org
        csv/BI-FY19.csv
- Use test mode (produces import script but does not submit):
    python imapimport.py --env=staging -c=BI --country_name=Burundi -p=FY19 -v2
        -t="your-token-here" --test_mode csv/BI-FY19.csv

"""
import time
import datim.datimimap
import datim.datimimapimport
import argparse


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
        raise argparse.ArgumentTypeError('Argument "env" must be %s' % (
            ', '.join(OCL_ENVIRONMENTS.keys())))
    return OCL_ENVIRONMENTS[string]


# Script argument parser
parser = argparse.ArgumentParser("imap-import", description="Import IMAP into OCL")
parser.add_argument('-c', '--country_code', help='Country code, eg "BI" or "UG"', required=True)
parser.add_argument('--country_name', help='Country name, eg Burundi', required=False, default='')
parser.add_argument('-p', '--period', help='Period, eg "FY19" or "FY20"', required=True)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('-t', '--token', help='OCL API token', required=True)
parser.add_argument(
    '--admin-token', help='OCL API administrative token required to delete existing repositories')
parser.add_argument('--test_mode', action="store_true", help='Enable test mode', default=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--public_access', help="Level of public access: View, None", default='View')
parser.add_argument('--version', action='version', version='%(prog)s v' + APP_VERSION)
parser.add_argument('--delete_existing_org', action="store_true",
                    help="Delete existing org if it exists", default=False)
parser.add_argument(
    'file', type=argparse.FileType('r'), help='IMAP JSON document, eg "BI-FY19.csv"')
args = parser.parse_args()

# with args.file as imap_file:
#     imap_json = json.load(imap_file)
#     imap = datim.qmap.Qmap(qmap_json=qmap_json)

# Default Script Settings
# country_code = 'BI'  # e.g. RW, UA - DATIM expects 2-letter code, but OCL accepts any length
# period = 'FY19'  # e.g. FY18, FY19
# imap_import_filename = 'csv/BI-FY19.csv'  # e.g. RW-FY18.csv or RW-FY18.json
# country_name = ''  # e.g. Rwanda
# verbosity = 2  # Set to 0 to hide all debug info, or 2 to show all debug info
# run_ocl_offline = False  # Not currently supported
# test_mode = False  # If true, generates the import script but does not actually import it
# delete_org_if_exists = True  # Be very careful with this option!
# country_public_access = 'View'  # Set visibility of country org/repos. None, View, Edit supported

# Pre-process input parameters
ocl_admin_api_token = args.admin_token
if not ocl_admin_api_token:
    ocl_admin_api_token = args.token
ocl_env_url = args.env if args.env else args.envurl
imap_import_filename = args.file.name
country_org = 'DATIM-MOH-%s-%s' % (args.country_code, args.period)
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
country_name = args.country_name
if not country_name and args.country_code in country_names:
    country_name = country_names[args.country_code]
if not country_name:
    country_name = args.country_code

# Display debug output
if args.verbosity:
    print args
    print 'Pre-processed arguments:'
    print '  country_name =', country_name
    print '  country_org =', country_org
    print '  import_filename =', imap_import_filename

exit()

# (Optionally) Delete org if it exists
if args.delete_existing_org:
    if args.verbosity:
        print('"delete_existing_org" is set to True:')
    if not args.test_mode:
        if args.verbosity:
            print('Deleting org "%s" if it exists in 5 seconds...' % country_org)
        # Pause briefly to allow user to cancel in case deleting org on accident...
        time.sleep(5)
        result = datim.datimimap.DatimImapFactory.delete_org_if_exists(
            org_id=country_org, oclenv=ocl_env_url, ocl_root_api_token=args.admin_token,
            verbose=args.verbosity)
    elif args.verbosity:
        print('Skipping "delete_existing_org" step because "test_mode" is enabled')

# Load IMAP from import file
imap_input = datim.datimimap.DatimImapFactory.load_imap_from_file(
    imap_filename=imap_import_filename, period=args.period,
    country_org=country_org, country_name=country_name, country_code=args.country_code)
if args.verbosity and imap_input:
    print('IMAP import file "%s" loaded successfully' % imap_import_filename)
elif not imap_input:
    print('Unable to load IMAP import file "%s"' % imap_import_filename)
    exit(1)

# Process the IMAP import
imap_import = datim.datimimapimport.DatimImapImport(
    oclenv=ocl_env_url, oclapitoken=args.token, verbosity=args.verbosity,
    run_ocl_offline=False, test_mode=args.test_mode,
    country_public_access=args.public_access)
imap_import.import_imap(imap_input=imap_input)
