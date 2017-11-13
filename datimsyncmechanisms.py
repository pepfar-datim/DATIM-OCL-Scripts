"""
Class to synchronize DATIM-DHIS2 Mechanisms definitions with OCL
The script runs 1 import batch, which consists of two queries to DHIS2, which are
synchronized with repositories in OCL as described below.
|-------------|-----------------|----------------------------------|
| ImportBatch | DHIS2           | OCL                              |
|-------------|-----------------|----------------------------------|
| mechanisms  | MechanismsQuery | /orgs/PEPFAR/sources/Mechanisms/ |
|-------------|-----------------|----------------------------------|
"""
from __future__ import with_statement
import os
import sys
import json
from datimsync import DatimSync
from datimconstants import DatimConstants


class DatimSyncMechanisms(DatimSync):
    """ Class to manage DATIM Mechanisms Synchronization """

    # Name of this sync script (used to name files and in logging)
    SYNC_NAME = 'Mechanisms'

    # Dataset ID settings
    OCL_DATASET_ENDPOINT = ''
    REPO_ACTIVE_ATTR = 'datim_sync_mechanisms'

    # File names
    DATASET_REPOSITORIES_FILENAME = 'mechanisms_ocl_dataset_repos_export.json'
    NEW_IMPORT_SCRIPT_FILENAME = 'mechanisms_dhis2ocl_import_script.json'
    DHIS2_CONVERTED_EXPORT_FILENAME = 'mechanisms_dhis2_converted_export.json'
    OCL_CLEANED_EXPORT_FILENAME = 'mechanisms_ocl_cleaned_export.json'

    # Import batches
    IMPORT_BATCHES = [DatimConstants.IMPORT_BATCH_MECHANISMS]

    # Overwrite DatimSync.SYNC_LOAD_DATASETS
    SYNC_LOAD_DATASETS = False

    # DATIM DHIS2 Query Definitions
    DHIS2_QUERIES = DatimConstants.MECHANISMS_DHIS2_QUERIES

    # OCL Export Definitions
    OCL_EXPORT_DEFS = DatimConstants.MECHANISMS_OCL_EXPORT_DEFS

    def __init__(self, oclenv='', oclapitoken='', dhis2env='', dhis2uid='', dhis2pwd='', compare2previousexport=True,
                 run_dhis2_offline=False, run_ocl_offline=False, verbosity=0, import_limit=0):
        DatimSync.__init__(self)
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.dhis2env = dhis2env
        self.dhis2uid = dhis2uid
        self.dhis2pwd = dhis2pwd
        self.run_dhis2_offline = run_dhis2_offline
        self.run_ocl_offline = run_ocl_offline
        self.verbosity = verbosity
        self.compare2previousexport = compare2previousexport
        self.import_limit = import_limit
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    def dhis2diff_mechanisms(self, dhis2_query_def=None, conversion_attr=None):
        """
        Convert new DATIM DHIS2 Mechanisms export to the diff format
        :param dhis2_query_def: DHIS2 query definition
        :param conversion_attr: Optional dictionary of attributes to pass to the conversion method
        :return: Boolean
        """
        dhis2filename_export_new = self.dhis2filename_export_new(dhis2_query_def['id'])
        with open(self.attach_absolute_path(dhis2filename_export_new), "rb") as input_file:
            self.vlog(1, 'Loading new DHIS2 export "%s"...' % dhis2filename_export_new)
            new_dhis2_export = json.load(input_file)
            partner = ''
            primeid = ''
            agency = ''
            orgunit = ''
            c = None

            # Iterate through each DataElement and transform to an OCL-JSON concept
            num_concepts = 0
            for coc in new_dhis2_export['categoryOptionCombos']:
                concept_id = coc['code']
                concept_key = '/orgs/PEPFAR/sources/Mechanisms/concepts/%s/' % concept_id
                for co in coc['categoryOptions']:
                    costartDate = co.get('startDate', '')
                    coendDate = co.get('endDate', '')
                    for ou in co["organisationUnits"]:
                        orgunit = ou.get('name', '')
                    for cog in co['categoryOptionGroups']:
                        cogname = cog['name']
                        cogcode = cog.get('code', '')
                        for gs in cog['groupSets']:
                            groupsetname = gs['name']
                            if groupsetname == 'Funding Agency':
                                agency = cogname
                            elif groupsetname == 'Implementing Partner':
                                partner = cogname
                                primeid = cogcode
                        if agency and partner and primeid:
                            c = {
                                'type': 'Concept',
                                'id': concept_id,
                                'concept_class': 'Funding Mechanism',
                                'datatype': 'None',
                                'owner': 'PEPFAR',
                                'owner_type': 'Organization',
                                'source': 'Mechanisms',
                                'external_id': coc['id'],
                                'descriptions': None,
                                'retired': False,
                                'names': [
                                    {
                                        'name': coc['name'],
                                        'name_type': 'Fully Specified',
                                        'locale': 'en',
                                        'locale_preferred': True,
                                        'external_id': None
                                    }
                                ],
                                'extras': {
                                    'Partner': partner,
                                    'Prime Id': primeid,
                                    'Agency': agency,
                                    'Start Date': costartDate,
                                    'End Date': coendDate,
                                    'Organizational Unit': orgunit
                                }
                            }
                            self.dhis2_diff[DatimConstants.IMPORT_BATCH_MECHANISMS][
                                self.RESOURCE_TYPE_CONCEPT][concept_key] = c
                            num_concepts += 1
                            partner = ''
                            primeid = ''
                            agency = ''
                            costartDate = ''
                            coendDate = ''
                            orgunit = ''

            self.vlog(1, 'DHIS2 export "%s" successfully transformed to %s concepts' % (
                dhis2filename_export_new, num_concepts))
            return True


# DATIM DHIS2 Settings
dhis2env = 'https://dev-de.datim.org/'
dhis2uid = 'paynejd'
dhis2pwd = 'Jonpayne1!'

# OCL Settings - JetStream Staging user=datim-admin
oclenv = 'https://api.staging.openconceptlab.org'
oclapitoken = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'

# Local development environment settings
sync_mode = DatimSync.SYNC_MODE_DIFF_ONLY  # Set which operation is performed by the sync script
verbosity = 2  # 0=none, 1=some, 2=all
import_limit = 0  # Number of resources to import; 0=all
import_delay = 3  # Number of seconds to delay between each import request
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
datim_sync = DatimSyncMechanisms(
    oclenv=oclenv, oclapitoken=oclapitoken, dhis2env=dhis2env, dhis2uid=dhis2uid, dhis2pwd=dhis2pwd,
    compare2previousexport=compare2previousexport, run_dhis2_offline=run_dhis2_offline,
    run_ocl_offline=run_ocl_offline, verbosity=verbosity, import_limit=import_limit)
datim_sync.import_delay = import_delay
datim_sync.run(sync_mode=sync_mode)
