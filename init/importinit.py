import oclfleximporter

# JSON Lines files to import
import_filenames = {
    'json_org_and_sources': 'datim_init.jsonl',
    'json_collections': 'dhis2datasets.jsonl',
    'json_tiered_support': 'tiered_support.json',
}

# OCL Settings
# api_url_root = ''
# ocl_api_token = ''
# JetStream Staging user=datim-admin
api_url_root = 'https://api.staging.openconceptlab.org'
ocl_api_token = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'

# Recommend running with test mode set to True before running for real
test_mode = False

for k in import_filenames:
    json_filename = import_filenames[k]
    ocl_importer = oclfleximporter.OclFlexImporter(
        file_path=json_filename, limit=0, api_url_root=api_url_root, api_token=ocl_api_token,
        test_mode=test_mode)
    ocl_importer.process()
