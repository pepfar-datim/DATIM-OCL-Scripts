'''
Settings for DATIM-OCL scripts
'''
import os


# Capture the root directory for the project
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

### Settings for different environments

# API tokens for different environments
api_token_qa_root = ''
api_token_qa_datim_admin = ''
api_token_staging_root = ''
api_token_staging_datim_admin = ''
api_token_production_root = ''
api_token_production_datim_admin = ''
api_token_dev_root = ''
api_token_dev_datim_admin = ''

# OCL API URL root - no slash at the end
ocl_api_url_production = 'https://api.openconceptlab.org'
ocl_api_url_staging = 'https://api.staging.openconceptlab.org'
ocl_api_url_qa = 'https://api.qa.openconceptlab.org'
ocl_api_url_who_staging = 'https://api.staging.who.openconceptlab.org'
ocl_api_url_demo = 'https://api.demo.openconceptlab.org'
ocl_api_url_dev = 'https://api.dev.openconceptlab.org'

# IMAP Mediator URL roots - no slash at the end
imap_mediator_url_test = 'https://test.ohie.datim.org:5000'
imap_mediator_url_production = 'https://ohie.datim4u.org:5000'

# File to be imported
import_file_path = ''

# DATIM DHIS2 Credentials for different environments
dhis2env_devde = 'https://dev-de.datim.org'
dhis2uid_devde = ''
dhis2pwd_devde = ''
dhis2env_triage = 'https://triage.datim.org'
dhis2uid_triage = ''
dhis2pwd_triage = ''
dhis2env_data = 'https://data.datim.org'
dhis2uid_data = ''
dhis2pwd_data = ''
dhis2env_testgeoalign = 'https://test.geoalign.datim.org'
dhis2uid_testgeoalign = ''
dhis2pwd_testgeoalign = ''
dhis2env_geoalign = 'https://geoalign.datim.org'
dhis2uid_geoalign = ''
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
