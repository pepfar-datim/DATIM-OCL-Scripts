'''
Settings for OCL Flexible JSON Importer
'''
import os


# Capture the root directory for the project
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# API tokens for different environments
api_token_qa_root = ''
api_token_qa_paynejd = ''
api_token_qa_paynejd99 = ''
api_token_qa_datim_admin = ''
api_token_staging_root = ''
api_token_staging_paynejd = ''
api_token_staging_datim_admin = ''
api_token_production_root = ''
api_token_production_paynejd = ''
api_token_production_datim_admin = ''

# File to be imported
import_file_path = 'DATIM/Staging-Mapping-Demo.json'

# API Token of the user account to use for importing
#api_token = 'api_token_goes_here'
api_token = ocl_api_token = api_token_staging_paynejd

# URL root - no slash at the end
ocl_api_url_production = 'https://api.openconceptlab.org'
ocl_api_url_staging = 'https://api.staging.openconceptlab.org'
ocl_api_url_qa = 'https://api.qa.openconceptlab.org'
api_url_root = ocl_api_url_root = ocl_api_url_staging

# DATIM DHIS2 Credentials for different environments
dhis2env_devde = 'https://dev-de.datim.org'
dhis2uid_devde = 'system_ocl_metadata_sync'
dhis2pwd_devde = ''
dhis2env_triage = 'https://triage.datim.org'
dhis2uid_triage = 'system_ocl_metadata_sync'
dhis2pwd_triage = ''
dhis2env_testgeoalign = 'https://test.geoalign.datim.org'
dhis2uid_testgeoalign = 'system_ocl_metadata_sync'
dhis2pwd_testgeoalign = ''
dhis2env_geoalign = 'https://geoalign.datim.org'
dhis2uid_geoalign = 'system_ocl_metadata_sync'
dhis2pwd_geoalign = ''

# DATIM DHIS2 Settings
dhis2env = dhis2env_testgeoalign
dhis2uid = dhis2env_testgeoalign
dhis2pwd = dhis2env_testgeoalign

#OCL API Settings
oclenv = ocl_api_url_staging
oclapitoken = api_token_staging_datim_admin
ocl_root_api_token = api_token_staging_root

# Set to True to allow updates to existing objects
do_update_if_exists = False

# Test mode -- set to True to process the file without actually importing
test_mode = False

# Verbosity
verbosity = 3

# Limit - number of resources to process; 0=unlimited
limit = 0
