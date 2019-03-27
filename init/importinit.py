"""
Use to import content into OCL to initialize the PEPFAR DATIM environment.

1. Start by creating the org/source structure by importing one of these:
1a. If synchronizing with all content (MER, SIMS, Mechanisms, Tiered Site Support, & DATIM-MOH):
  datim_init_all.json -- Org:PEPFAR; Sources:MER,DATIM-MOH,SIMS,Mechanisms,Tiered-Site-Support; DATIM-MOH collection;
    initial repo versions
1b. If only doing DATIM-MOH:
  datim_init_moh_only.json -- Org:PEPFAR; Sources:DATIM-MOH; DATIM-MOH collection; initial empty repo version

2. OCL collections that stay in sync with a DHIS2 Dataset must be pre-defined in OCL. Import the following if
synchronizing with MER, SIMS, or Tiered Site Support (not needed for DATIM-MOH or Mechanisms):
  dhis2datasets.json -- Collections and initial empty versions for MER, SIMS, and Tiered Site Support

3. Tiered site support content is static and can be imported using the provided JSON file. If using Tiered Site
Support, then import the following:
  tiered_support.json -- Concepts and Mappings for Tiered Site Support

TODO:
- Create empty source version for Tiered Site Support? Add references for Tiered Site Support collections?
"""
from ocldev.oclfleximporter import OclFlexImporter
import settings


# JSON Lines files to import
import_filenames_all = {
    'json_org_and_sources': 'datim_init_all.json',
    'json_collections': 'dhis2datasets.json',
    'json_tiered_support': 'tiered_support.json',
}
import_filenames_datim_moh_only = {
    'json_datim_moh': 'datim_init_only_moh.json',
}
#import_filenames = import_filenames_datim_moh_only
import_filenames = {'test':'test.json'}

# OCL Settings - JetStream Staging user=datim-admin
#ocl_api_url_root = settings.ocl_api_url_production
#ocl_api_token = settings.api_token_production_datim_admin
ocl_api_url_root = settings.ocl_api_url_staging
ocl_api_token = settings.api_token_staging_paynejd

# Recommend running with test mode set to True before running for real
test_mode = True
limit = 0

for k in import_filenames:
    json_filename = import_filenames[k]
    ocl_importer = OclFlexImporter(
        file_path=json_filename, limit=limit, api_url_root=ocl_api_url_root, api_token=ocl_api_token,
        test_mode=test_mode)
    ocl_importer.process()
