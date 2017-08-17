'''
Settings for OCL Flexible JSON Importer
'''


# API tokens for different environments
api_token_showcase_root = ''
api_token_showcase_paynejd = 'a28072a2eb82c6c9949ba6bb8489002438e5bcc7'
api_token_showcase_paynejd99 = '2da0f46b7d29aa57970c0b3a535121e8e479f881'
api_token_staging_root = '23c5888470d4cb14d8a3c7f355f4cdb44000679a'
api_token_staging_paynejd = 'a61ba53ed7b8b26ece8fcfc53022b645de0ec055'
api_token_production_root = '230e6866c2037886909c58d8088b1a5e7cabc74b'
api_token_production_paynejd = '950bd651dc4ee29d6bcee3e6dacfe7834bb0f881'

# File to be imported
import_file_path = 'DATIM/Staging-Mapping-Demo.json'

# API Token of the user account to use for importing
#api_token = 'api_token_goes_here'
api_token = api_token_staging_paynejd

# URL root - no slash at the end
api_url_production = 'https://api.openconceptlab.org'
api_url_staging = 'https://api.staging.openconceptlab.org'
api_url_showcase = 'https://api.showcase.openconceptlab.org'
api_url_root = api_url_staging

# Set to True to allow updates to existing objects
do_update_if_exists = False

# Test mode -- set to True to process the file without actually importing
test_mode = False

# Verbosity
verbosity = 3

# Limit - number of resources to process; 0=unlimited
limit = 0
