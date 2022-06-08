"""
Use to import starter content into OCL to initialize the PEPFAR DATIM environment.
Before running, edit IMPORT_FILENAMES variable with the list of files you wish to import.

Files: (THIS LIST NEEDS TO BE UPDATED!)
* datim_init_all.json - Do this first - imports PEPFAR Org and sources for MER,SIMS,Mechanisms,
    Tiered-Site-Support
* datim_init_only_moh.json - imports DATIM-MOH sources, collections, and null_disag concepts
    for both FY18 and FY19
* dhis2datasets.json - imports OCL collections that stay in sync with a DHIS2 Dataset must be
    pre-defined in OCL. Required for MER, SIMS, and Tiered Site Support (not needed for DATIM-MOH
    or Mechanisms). Includes collections and their initial empty repo versions.
* tiered_support.json - Tiered site support content is static so it does not have a sync script.
    The content can simply be imported using this JSON file. Includes Concepts and Mappings for
    Tiered Site Support. Note that no repo versions and no collection references are created for
    Tiered Site Support
"""
import ocldev.oclresourcelist
import ocldev.oclfleximporter
import settings

# Edit this list to import the files that you need

IMPORT_FILENAMES_ALL = [
    'init/pepfar_org.json',
    'init/datim_moh_fy19.json',
    'init/datim_moh_fy20.json',
    'init/datim_moh_fy21_cs.json',
    'init/datim_moh_fy21_daa.json',
    'init/datim_moh_fy22_daa.json',
    'init/datim_moh_fy22q1_daa.json',
]
IMPORT_FILENAMES = IMPORT_FILENAMES_ALL

# OCL Settings
VERBOSE = False
DO_BULK_IMPORT = True
DO_WAIT_UNTIL_IMPORT_COMPLETE = True
OCL_API_URL_ROOT = settings.ocl_api_url_staging
OCL_API_TOKEN = settings.api_token_staging_datim_admin

# Build a combined resource list
resource_list = ocldev.oclresourcelist.OclJsonResourceList()
for import_filename in IMPORT_FILENAMES:
    resource_list += ocldev.oclresourcelist.OclJsonResourceList.load_from_file(
        filename=import_filename)
print('%s resources will be imported:' % len(resource_list))
print((resource_list.summarize(core_attr_key='type')))

# Display the full list of resources
if VERBOSE:
    import json
    for resource in resource_list:
        print(json.dumps(resource))

# Process as bulk import
if DO_BULK_IMPORT and resource_list:
    print('Submitting bulk import to: %s' % OCL_API_URL_ROOT)
    bulk_import_response = ocldev.oclfleximporter.OclBulkImporter.post(
        input_list=resource_list, api_token=OCL_API_TOKEN,
        api_url_root=OCL_API_URL_ROOT, parallel=True)
    task_id = bulk_import_response.json()['task']
    print('BULK IMPORT TASK ID: %s' % task_id)
    if DO_WAIT_UNTIL_IMPORT_COMPLETE:
        print('INFO: Waiting until import is complete...')
        import_results = ocldev.oclfleximporter.OclBulkImporter.get_bulk_import_results(
            task_id=task_id, api_url_root=OCL_API_URL_ROOT, api_token=OCL_API_TOKEN,
            delay_seconds=5, max_wait_seconds=800)
        if import_results:
            print(import_results.display_report())
