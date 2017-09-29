import oclfleximporter

# JSON Lines files to import
json_org_and_sources = 'datim_init.jsonl'
json_collections = 'dhis2datasets.jsonl'

# OCL Settings
# api_url_root = ''
# ocl_api_token = ''
# JetStream Staging user=datim-admin
api_url_root = 'https://api.staging.openconceptlab.org'
ocl_api_token = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'

# Recommend running with test mode set to True before running for real
test_mode = False

'''
importer_org_sources = oclfleximporter.OclFlexImporter(
    file_path=json_org_and_sources, limit=0,
    api_url_root=api_url_root, api_token=ocl_api_token,
    test_mode=test_mode)
importer_org_sources.process()
'''

importer_collections = oclfleximporter.OclFlexImporter(
    file_path=json_collections, limit=0,
    api_url_root=api_url_root, api_token=ocl_api_token,
    test_mode=test_mode)
importer_collections.process()
