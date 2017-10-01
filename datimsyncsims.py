"""
Class to synchronize DATIM-DHIS2 SIMS definitions with OCL
The script runs 1 import batch, which consists of two queries to DHIS2, which are
synchronized with repositories in OCL as described below.
|-------------|-------------------------|--------------------------------------------|
| ImportBatch | DHIS2                   | OCL                                        |
|-------------|-------------------------|--------------------------------------------|
| sims        | SimsAssessmentTypeQuery | /orgs/PEPFAR/sources/SIMS/                 |
|             |                         | /orgs/PEPFAR/collections/SIMS3-Facility/   |
|             |                         | /orgs/PEPFAR/collections/SIMS3-Community/  |
|             |                         | /orgs/PEPFAR/collections/SIMS3-Above-Site/ |
|             |                         | /orgs/PEPFAR/collections/SIMS2-Facility/   |
|             |                         | /orgs/PEPFAR/collections/SIMS2-Community/  |
|             |                         | /orgs/PEPFAR/collections/SIMS2-Above-Site/ |
|             |-------------------------|--------------------------------------------|
|             | SimsOptionsQuery        | /orgs/PEPFAR/sources/SIMS/                 |
|             |                         | /orgs/PEPFAR/collections/SIMS-Options/     |
|-------------|-------------------------|--------------------------------------------|
"""
from __future__ import with_statement
import os
import sys
import json
from datimsync import DatimSync


class DatimSyncSims(DatimSync):
    """ Class to manage DATIM SIMS Synchronization """

    # Dataset ID settings
    OCL_DATASET_ENDPOINT = '/orgs/PEPFAR/collections/?q=SIMS&verbose=true&limit=200'
    REPO_ACTIVE_ATTR = 'datim_sync_sims'
    DATASET_REPOSITORIES_FILENAME = 'ocl_dataset_repos_export.json'

    # Filenames
    NEW_IMPORT_SCRIPT_FILENAME = 'sims_dhis2ocl_import_script.json'
    DHIS2_CONVERTED_EXPORT_FILENAME = 'sims_dhis2_converted_export.json'
    OCL_CLEANED_EXPORT_FILENAME = 'sims_ocl_cleaned_export.json'

    # Import batches
    IMPORT_BATCH_SIMS = 'SIMS'
    IMPORT_BATCHES = [IMPORT_BATCH_SIMS]

    # DATIM DHIS2 Query Definitions
    DHIS2_QUERIES = {
        'SimsAssessmentTypes': {
            'id': 'SimsAssessmentTypes',
            'name': 'DATIM-DHIS2 SIMS Assessment Types',
            'query': 'api/dataElements.json?fields=name,code,id,valueType,lastUpdated,dataElementGroups[id,name]&'
                     'order=code:asc&paging=false&filter=dataElementGroups.id:in:[{{active_dataset_ids}}]',
            'conversion_method': 'dhis2diff_sims_assessment_types'
        }
    }
    DHIS2_QUERIES_INACTIVE = {
        'SimsOptions': {
            'id': 'SimsOptions',
            'name': 'DATIM-DHIS2 SIMS Options',
            'query': '',
            'conversion_method': 'dhis2diff_sims_options'
        }
    }

    # OCL Export Definitions
    OCL_EXPORT_DEFS = {
        'sims_source': {'import_batch': IMPORT_BATCH_SIMS,
                        'endpoint': '/orgs/PEPFAR/sources/SIMS/'},
        'sims2_above_site': {'import_batch': IMPORT_BATCH_SIMS,
                             'endpoint': '/orgs/PEPFAR/collections/SIMS2-Above-Site/'},
        'sims2_community': {'import_batch': IMPORT_BATCH_SIMS,
                            'endpoint': '/orgs/PEPFAR/collections/SIMS2-Community/'},
        'sims2_facility': {'import_batch': IMPORT_BATCH_SIMS,
                           'endpoint': '/orgs/PEPFAR/collections/SIMS2-Facility/'},
        'sims3_above_site': {'import_batch': IMPORT_BATCH_SIMS,
                             'endpoint': '/orgs/PEPFAR/collections/SIMS3-Above-Site/'},
        'sims3_community': {'import_batch': IMPORT_BATCH_SIMS,
                            'endpoint': '/orgs/PEPFAR/collections/SIMS3-Community/'},
        'sims3_facility': {'import_batch': IMPORT_BATCH_SIMS,
                           'endpoint': '/orgs/PEPFAR/collections/SIMS3-Facility/'},
    }

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

    def dhis2diff_sims_options(self, dhis2_query_def=None, conversion_attr=None):
        """
        Convert new DHIS2 SIMS Options export to the diff format
        :param dhis2_query_def:
        :param conversion_attr:
        :return:
        """
        pass

    def dhis2diff_sims_assessment_types(self, dhis2_query_def=None, conversion_attr=None):
        """
        Convert new DHIS2 SIMS Assessment Types export to the diff format
        :param dhis2_query_def: DHIS2 query definition
        :param conversion_attr: Optional dictionary of attributes to pass to the conversion method
        :return: Boolean
        """
        dhis2filename_export_new = self.dhis2filename_export_new(dhis2_query_def['id'])
        with open(self.attach_absolute_path(dhis2filename_export_new), "rb") as input_file:
            new_dhis2_export = json.load(input_file)
            ocl_dataset_repos = conversion_attr['ocl_dataset_repos']
            num_concepts = 0
            num_references = 0

            # Iterate through each DataElement and transform to an OCL-JSON concept
            for de in new_dhis2_export['dataElements']:
                concept_id = de['code']
                concept_key = '/orgs/PEPFAR/sources/SIMS/concepts/' + concept_id + '/'
                c = {
                    'type': 'Concept',
                    'id': concept_id,
                    'concept_class': 'Assessment Type',
                    'datatype': 'None',
                    'owner': 'PEPFAR',
                    'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                    'source': 'SIMS',
                    'retired': False,
                    'descriptions': None,
                    'external_id': de['id'],
                    'names': [
                        {
                            'name': de['name'],
                            'name_type': 'Fully Specified',
                            'locale': 'en',
                            'locale_preferred': False,
                            'external_id': None,
                        }
                    ],
                    'extras': {'Value Type': de['valueType']}
                }
                self.dhis2_diff[self.IMPORT_BATCH_SIMS][self.RESOURCE_TYPE_CONCEPT][concept_key] = c
                num_concepts += 1

                # Iterate through each DataElementGroup and transform to an OCL-JSON reference
                for deg in de['dataElementGroups']:
                    collection_id = ocl_dataset_repos[deg['id']]['id']
                    concept_url = '/orgs/PEPFAR/sources/SIMS/concepts/' + concept_id + '/'
                    concept_ref_key = ('/orgs/PEPFAR/collections/' + collection_id +
                                       '/references/?concept=' + concept_url)
                    r = {
                        'type': 'Reference',
                        'owner': 'PEPFAR',
                        'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                        'collection': collection_id,
                        'data': {"expressions": [concept_url]}
                    }
                    self.dhis2_diff[self.IMPORT_BATCH_SIMS][self.RESOURCE_TYPE_CONCEPT_REF][concept_ref_key] = r
                    num_references += 1

            self.vlog(1, 'DHIS2 export "%s" successfully transformed to %s concepts + %s references (%s total)' % (
                dhis2filename_export_new, num_concepts, num_references, num_concepts + num_references))
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
    import_limit = 1
    import_test_mode = True
    compare2previousexport = False
    run_dhis2_offline = True
    run_ocl_offline = True
    dhis2env = 'https://dev-de.datim.org/'
    dhis2uid = 'paynejd'
    dhis2pwd = 'Jonpayne1!'

    # Digital Ocean Showcase - user=paynejd99
    # oclenv = 'https://api.showcase.openconceptlab.org'
    # oclapitoken = '2da0f46b7d29aa57970c0b3a535121e8e479f881'

    # JetStream Staging - user=paynejd
    # oclenv = 'https://oclapi-stg.openmrs.org'
    # oclapitoken = 'a61ba53ed7b8b26ece8fcfc53022b645de0ec055'

    # JetStream QA - user=paynejd
    oclenv = 'https://oclapi-qa.openmrs.org'
    oclapitoken = 'a5678e5f7971f3003e7be563ee4b90297b841f05'


# Create sync object and run
sims_sync = DatimSyncSims(oclenv=oclenv, oclapitoken=oclapitoken,
                          dhis2env=dhis2env, dhis2uid=dhis2uid, dhis2pwd=dhis2pwd,
                          compare2previousexport=compare2previousexport,
                          run_dhis2_offline=run_dhis2_offline, run_ocl_offline=run_ocl_offline,
                          verbosity=verbosity,
                          import_test_mode=import_test_mode,
                          import_limit=import_limit)
# sims_sync.run()
sims_sync.data_check()
