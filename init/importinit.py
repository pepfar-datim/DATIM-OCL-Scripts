from ocldev.oclfleximporter import OclFlexImporter
import settings


# JSON Lines files to import
import_filenames = {
    #'json_org_and_sources': 'datim_init.jsonl',
    #'json_collections': 'dhis2datasets.jsonl',
    #'json_tiered_support': 'tiered_support.json',
    'json_datim_moh': 'datimmoh.json',
}

# OCL Settings - JetStream Staging user=datim-admin
api_url_root = settings.ocl_api_url_staging
ocl_api_token = settings.api_token_staging_datim_admin

# Recommend running with test mode set to True before running for real
test_mode = False

for k in import_filenames:
    json_filename = import_filenames[k]
    ocl_importer = OclFlexImporter(
        file_path=json_filename, limit=0, api_url_root=api_url_root, api_token=ocl_api_token,
        test_mode=test_mode)
    ocl_importer.process()
