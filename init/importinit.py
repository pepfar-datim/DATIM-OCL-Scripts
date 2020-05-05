"""
Use to import starter content into OCL to initialize the PEPFAR DATIM environment.
Before running, edit import_filenames variable with the list of files you wish to import.

Files:
* datim_init_all.json - Do this first - imports PEPFAR Org and sources for MER,SIMS,Mechanisms,Tiered-Site-Support
* datim_init_only_moh.json - imports DATIM-MOH sources, collections, and null_disag concepts for both FY18 and FY19
* dhis2datasets.json - imports OCL collections that stay in sync with a DHIS2 Dataset must be pre-defined in OCL.
  Required for MER, SIMS, and Tiered Site Support (not needed for DATIM-MOH or Mechanisms). Includes collections and
  their initial empty repo versions.
* tiered_support.json - Tiered site support content is static so it does not have a sync script. The content can
  simply be imported using this JSON file. Includes Concepts and Mappings for Tiered Site Support. Note that no
  repo versions and no collection references are created for Tiered Site Support
"""
from ocldev.oclfleximporter import OclFlexImporter
import settings


# JSON Lines files to import
import_filenames_all = [
    'datim_init_all.json',
    'datim_init_only_moh.json',
    'dhis2datasets.json',
    'tiered_support.json',
    'code-list-collections-fy18-fy19.json',
]
import_filenames_datim_moh_only = [
    'datim_init_only_moh.json',
]
import_filenames = import_filenames_all

# OCL Settings - JetStream Staging user=datim-admin
ocl_api_url_root = settings.oclenv
ocl_api_token = settings.oclapitoken

# Recommend running with test mode set to True before running for real
test_mode = False
limit = 0

for import_filename in import_filenames:
    ocl_importer = OclFlexImporter(
        file_path=import_filename, limit=limit, api_url_root=ocl_api_url_root, api_token=ocl_api_token,
        test_mode=test_mode, do_update_if_exists=False)
    ocl_importer.process()
