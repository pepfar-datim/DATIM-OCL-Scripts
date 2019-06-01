"""
Script to synchronize FY19 DATIM MOH Alignment definitions between DHIS2 and OCL.
The script runs 1 import batch, consisting of one query to DHIS2, which is synchronized with
repositories in OCL as described below.

Note: This script is set to run against vshioshvili.datim.org, while FY18 runs against
test.geoalign.org and FY17 (syncmoh.py) runs against www.datim.org.

|-------------|----------|----------------------------------------|
| ImportBatch | DHIS2    | OCL                                    |
|-------------|----------|----------------------------------------|
| MOH-FY19    | MOH-FY19 | /orgs/PEPFAR/sources/DATIM-MOH-FY19/   |
|-------------|----------|----------------------------------------|
"""
import sys
import os
import settings
import datim.datimsync
import datim.datimsyncmohfy19


# DATIM DHIS2 Settings
dhis2env = settings.dhis2env_vshioshvili
dhis2uid = settings.dhis2uid_vshioshvili
dhis2pwd = settings.dhis2pwd_vshioshvili

# OCL Settings - staging user=datim-admin
oclenv = settings.ocl_api_url_staging
oclapitoken = settings.api_token_staging_datim_admin

# Local development environment settings
sync_mode = datim.datimsync.DatimSync.SYNC_MODE_FULL_IMPORT  # Set which sync operation is performed
verbosity = 2  # 0=none, 1=some, 2=all
import_limit = 0  # Number of resources to import; 0=all
import_delay = 1  # Number of seconds to delay between each import request
compare2previousexport = False  # Set to False to ignore the previous export; set to True only after a full import
run_dhis2_offline = False  # Set to true to use local copies of dhis2 exports
run_ocl_offline = False  # Set to true to use local copies of ocl exports

# Set variables from environment if available
if len(sys.argv) > 1 and sys.argv[1] in ['true', 'True']:
    # Server environment settings (required for OpenHIM)
    dhis2env = os.environ['DHIS2_ENV']
    dhis2uid = os.environ['DHIS2_USER']
    dhis2pwd = os.environ['DHIS2_PASS']
    oclenv = os.environ['OCL_ENV']
    oclapitoken = os.environ['OCL_API_TOKEN']
    if "IMPORT_LIMIT" in os.environ:
      import_limit = os.environ['IMPORT_LIMIT']
    if "IMPORT_DELAY" in os.environ:
      import_delay = float(os.environ['IMPORT_DELAY'])
    if "COMPARE_PREVIOUS_EXPORT" in os.environ:
      compare2previousexport = os.environ['COMPARE_PREVIOUS_EXPORT'] in ['true', 'True']
    if "SYNC_MODE" in os.environ:
      sync_mode = os.environ['SYNC_MODE']
    if "RUN_DHIS2_OFFLINE" in os.environ:
      run_dhis2_offline = os.environ['RUN_DHIS2_OFFLINE'] in ['true', 'True']
    if "RUN_OCL_OFFLINE" in os.environ:
      run_ocl_offline = os.environ['RUN_OCL_OFFLINE'] in ['true', 'True']

# Create sync object and run
datim_sync = datim.datimsyncmohfy19.DatimSyncMohFy19(
    oclenv=oclenv, oclapitoken=oclapitoken, dhis2env=dhis2env, dhis2uid=dhis2uid, dhis2pwd=dhis2pwd,
    compare2previousexport=compare2previousexport, run_dhis2_offline=run_dhis2_offline,
    run_ocl_offline=run_ocl_offline, verbosity=verbosity, import_limit=import_limit)
datim_sync.import_delay = import_delay
datim_sync.run(sync_mode=sync_mode)
