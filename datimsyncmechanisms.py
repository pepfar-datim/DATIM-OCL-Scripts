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
from datimbase import DatimConstants


class DatimSyncMechanisms(DatimSync):
    """ Class to manage DATIM Mechanisms Synchronization """

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
    DHIS2_QUERIES = {
        'Mechanisms': {
            'id': 'Mechanisms',
            'name': 'DATIM-DHIS2 Funding Mechanisms',
            'query': 'api/categoryOptionCombos.json?fields=id,code,name,created,lastUpdated,'
                     'categoryOptions[id,endDate,startDate,organisationUnits[code,name],'
                     'categoryOptionGroups[id,name,code,groupSets[id,name]]]&order=code:asc&filter=categoryCombo.id:eq:wUpfppgjEza&paging=false',
            'conversion_method': 'dhis2diff_mechanisms'
        }
    }

    # OCL Export Definitions
    OCL_EXPORT_DEFS = DatimConstants.MECHANISMS_OCL_EXPORT_DEFS

    def __init__(self, oclenv='', oclapitoken='', dhis2env='', dhis2uid='', dhis2pwd='', compare2previousexport=True,
                 run_dhis2_offline=False, run_ocl_offline=False,
                 verbosity=0, data_check_only=False, import_test_mode=False, import_limit=0):
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
        self.import_test_mode = import_test_mode
        self.import_limit = import_limit
        self.data_check_only = data_check_only
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
                                'datatype': 'Text',
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


# Default Script Settings
verbosity = 2  # 0=none, 1=some, 2=all
import_limit = 0  # Number of resources to import; 0=all
import_test_mode = False  # Set to True to see which import API requests would be performed on OCL
run_dhis2_offline = True  # Set to true to use local copies of dhis2 exports
run_ocl_offline = True  # Set to true to use local copies of ocl exports
compare2previousexport = True  # Set to False to ignore the previous export

# DATIM DHIS2 Settings
dhis2env = ''
dhis2uid = ''
dhis2pwd = ''

# OCL Settings
oclenv = ''
oclapitoken = ''

# Set variables from environment if available
if len(sys.argv) > 1 and sys.argv[1] in ['true', 'True']:
    # Server environment settings (required for OpenHIM)
    dhis2env = os.environ['DHIS2_ENV']
    dhis2uid = os.environ['DHIS2_USER']
    dhis2pwd = os.environ['DHIS2_PASS']
    oclenv = os.environ['OCL_ENV']
    oclapitoken = os.environ['OCL_API_TOKEN']
    compare2previousexport = os.environ['COMPARE_PREVIOUS_EXPORT'] in ['true', 'True']
else:
    # Local development environment settings
    import_limit = 10
    import_test_mode = False
    compare2previousexport = False
    run_dhis2_offline = False
    run_ocl_offline = False
    dhis2env = 'https://dev-de.datim.org/'
    dhis2uid = 'paynejd'
    dhis2pwd = 'Jonpayne1!'

    # JetStream Staging user=datim-admin
    oclenv = 'https://api.staging.openconceptlab.org'
    oclapitoken = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'

# Create sync object and run
datim_sync = DatimSyncMechanisms(oclenv=oclenv, oclapitoken=oclapitoken,
                                 dhis2env=dhis2env, dhis2uid=dhis2uid, dhis2pwd=dhis2pwd,
                                 compare2previousexport=compare2previousexport,
                                 run_dhis2_offline=run_dhis2_offline, run_ocl_offline=run_ocl_offline,
                                 verbosity=verbosity,
                                 import_test_mode=import_test_mode,
                                 import_limit=import_limit)
datim_sync.import_delay = 3
datim_sync.run()
# datim_sync.data_check()
