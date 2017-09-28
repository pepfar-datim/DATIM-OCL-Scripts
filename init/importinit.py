import oclfleximporter

# JSON Lines files to import
json_org_and_sources = 'datim_init.jsonl'
json_collections = 'dhis2datasets.jsonl'

# OCL Settings
api_url_root = ''
ocl_api_token = ''

# Recommend running with test mode set to True before running for real
test_mode = False

importer_org_sources = oclfleximporter.OclFlexImporter(
    file_path=json_org_and_sources,
    api_url_root=api_url_root, api_token=ocl_api_token,
    test_mode=test_mode)
importer_org_sources.process()

importer_collections = oclfleximporter.OclFlexImporter(
    file_path=json_collections,
    api_url_root=api_url_root, api_token=ocl_api_token,
    test_mode=test_mode)
importer_collections.process()
