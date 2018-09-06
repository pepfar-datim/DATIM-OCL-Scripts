"""
Utility script to easily import CSV files for multiple countries in one go.
Provides an option to delete existing orgs in the case of a clean import.
"""
import json
import requests
import settings
import datim.datimimap
import datim.datimimapimport
import time

# Script Settings
period = 'FY17'
do_delete_datim_country_orgs = False
do_delete_orgs_remove_list = False
test_mode = True
country_codes = {
        'BW': 'Botswana',
        'CI': "Cote d'Ivoire",
        'HT': 'Haiti',
        'KE': 'Kenya',
        'LS': 'Lesotho',
        'MW': 'Malawi',
        'NA': 'Namibia',
        'RW': 'Rwanda',
        'SZ': 'Swaziland',
        'TZ': 'Tanzania',
        'UG': 'Uganda',
        'ZM': 'Zambia',
        'ZW': 'Zimbabwe',
    }
org_remove_list = [
    'EthiopiaMoH-test-FqNIc',
    'AnjalisOrg',
    'LGTEST3'
]

# OCL Settings
ocl_env = settings.ocl_api_url_staging
oclapitoken = settings.api_token_staging_datim_admin
oclapiheaders = {
    'Authorization': 'Token ' + oclapitoken,
    'Content-Type': 'application/json'
}

# Delete orgs - "DATIM-*" and anything in org_remove_list
if do_delete_datim_country_orgs or do_delete_orgs_remove_list:
    orgs_url = ocl_env + '/orgs/?limit=200'
    r_orgs = requests.get(orgs_url, headers=oclapiheaders)
    orgs = r_orgs.json()
    for org in orgs:
        if ((do_delete_datim_country_orgs and org['id'][:6] == 'DATIM-') or (
                    do_delete_orgs_remove_list and org['id'] in org_remove_list)):
            print '*** DELETE ORG:', org['id']
            if test_mode:
                print '    Skipping because test_mode=True'
            else:
                r_org_delete = requests.delete(ocl_env + org['url'], headers=oclapiheaders)
                print r_org_delete.status_code()

# Run the imports
i = 0
for country_code in country_codes:
    i += 1
    country_name = country_codes[country_code]
    csv_filename = 'csv/%s-FY17.csv' % country_code
    country_org = 'DATIM-MOH-%s' % country_code
    print '\n\n*******************************************************************************'
    print '** [IMPORT %s of %s] %s, Period: %s, CSV: %s' % (
        str(i), str(len(country_codes)), country_org, period, csv_filename)
    print '*******************************************************************************'

    # Load i-map from CSV file
    imap_input = datim.datimimap.DatimImapFactory.load_imap_from_csv(
        csv_filename=csv_filename, period=period,
        country_org=country_org, country_name=country_name, country_code=country_code)

    # Run the import
    imap_import = datim.datimimapimport.DatimImapImport(
        oclenv=ocl_env, oclapitoken=oclapitoken, verbosity=2, test_mode=test_mode)
    imap_import.import_imap(imap_input=imap_input)

    time.sleep(5) # short delay before the next one begins
