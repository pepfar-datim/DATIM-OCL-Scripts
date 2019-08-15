'''
Script to import PEPFAR mechanisms
'''
import settings
from json_flex_import import ocl_json_flex_import


# File to be imported
import_file_path = 'mechanisms_zendesk_20170804.json'

# API Token of the user account to use for importing
api_token = settings.oclapitoken

# URL root - no slash at the end
api_url_production = 'https://api.openconceptlab.org'
api_url_staging = 'https://api.staging.openconceptlab.org'
api_url_showcase = 'https://api.showcase.openconceptlab.org'
api_url_root = settings.oclenv

# Set to True to allow updates to existing objects
do_update_if_exists = False

# Test mode -- set to True to process the file without actually importing
test_mode = True

# Verbosity
verbosity = 3

# Limit - number of resources to process; 0=unlimited
limit = 0


ocl_importer = ocl_json_flex_import(
    file_path=import_file_path, api_token=api_token,
    api_url_root=api_url_root, test_mode=test_mode,
    do_update_if_exists=do_update_if_exists, verbosity=verbosity,
    limit=limit)
ocl_importer.process()
