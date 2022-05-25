"""
Utility script to easily import CSV files for multiple countries in one go.
Provides an option to delete one or multiple existing orgs in the case of a clean import.
Be super careful with the org delete option!
"""
import requests
import settings
import time
import datim.datimimap
import datim.datimimapimport

# Import Script Settings
period = 'FY18'
test_mode = True
country_code_postfix = ''
country_codes = {
        #"BW": "Botswana",
        "BI": "Burundi",
        "CM": "Cameroon",
        "CI": "Cote d'Ivoire",
        "CD": "Democratic Republic of the Congo",
        #"SZ": "Eswatini",
        "ET": "Ethiopia",
        #"HT": "Haiti",
        "KE": "Kenya",
        #"LS": "Lesotho",
        "MW": "Malawi",
        "MZ": "Mozambique",
        "NA": "Namibia",
        #"NG": "Nigeria",
        "RW": "Rwanda",
        #"ZA": "South Africa",
        #"SS": "South Sudan",
        #"TZ": "Tanzania",
        "UG": "Uganda",
        "UA": "Ukraine",
        "VN": "Vietnam",
        "ZM": "Zambia",
        #"ZW": "Zimbabwe",
    }

# Org delete settings (Disabled by default)
do_delete_datim_country_orgs = False  # deletes orgs with IDs like "DATIM-MOH-*"
do_delete_orgs_remove_list = False
org_remove_list = [
    "DATIM-MOH-ET",
    "DATIM-MOH-BW",
    "DATIM-MOH-HT",
    "DATIM-MOH-SZ",
]

# OCL Settings
ocl_env = settings.ocl_env
#oclapitoken = settings.api_token_staging_datim_admin
# root token
oclapitoken = settings.ocl_root_api_token
oclapiheaders = {
    'Authorization': 'Token ' + oclapitoken,
    'Content-Type': 'application/json'
}

# Delete orgs - "DATIM-MOH-*" and anything in org_remove_list
if do_delete_datim_country_orgs or do_delete_orgs_remove_list:
    orgs_url = ocl_env + '/orgs/?limit=200'
    r_orgs = requests.get(orgs_url, headers=oclapiheaders)
    orgs = r_orgs.json()
    for org in orgs:
        if ((do_delete_datim_country_orgs and org['id'][:10] == 'DATIM-MOH-') or (
                    do_delete_orgs_remove_list and org['id'] in org_remove_list)):
            org_url = ocl_env + org['url']
            print(('*** DELETE ORG:', org['id'], org_url))
            if test_mode:
                print('    Skipping because test_mode=True')
            else:
                r_org_delete = requests.delete(org_url, headers=oclapiheaders)
                print(r_org_delete)

# Run the imports
i = 0
for country_code in country_codes:
    i += 1
    imap_country_code = '%s%s' % (country_code, country_code_postfix)
    country_name = country_codes[country_code]
    csv_filename = 'csv/%s-%s.csv' % (country_code, period)
    country_org = 'DATIM-MOH-%s%s' % (country_code, country_code_postfix)
    print(('\n\n' + '*' * 100))
    print(('** [IMPORT %s of %s] DATIM Country Code: %s, IMAP Country Code: %s, Org: %s, Period: %s, CSV: %s' % (
        str(i), str(len(country_codes)), country_code, imap_country_code, country_org, period, csv_filename)))
    print(('*' * 100))

    # Load i-map from CSV file
    try:
        imap_input = datim.datimimap.DatimImapFactory.load_imap_from_csv(
            csv_filename=csv_filename, period=period,
            country_org=country_org, country_name=country_name, country_code=imap_country_code)
        imap_input.display(sort=True, exclude_empty_maps=True)
    except IOError:
        print(('No such file or directory: "%s". Skipping...' % csv_filename))
        continue

    # Run the import
    imap_import = datim.datimimapimport.DatimImapImport(
        oclenv=ocl_env, oclapitoken=oclapitoken, verbosity=2, test_mode=test_mode)
    imap_import.import_imap(imap_input=imap_input)
    time.sleep(5) # short delay before the next one begins
