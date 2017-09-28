"""
Class to synchronize DATIM DHIS2 MER Indicator definitions with OCL
The script runs 1 import batch, which consists of two queries to DHIS2, which are
synchronized with repositories in OCL as described below.
|-------------|--------|-------------------------------------------------|
| ImportBatch | DHIS2  | OCL                                             |
|-------------|--------|-------------------------------------------------|
| MER         | MER    | /orgs/PEPFAR/sources/MER/                       |
|             |        | /orgs/PEPFAR/collections/MER-*/                 |
|             |        | /orgs/PEPFAR/collections/HC-*/                  |
|             |        | /orgs/PEPFAR/collections/Planning-Attributes-*/ |
|-------------|--------|-------------------------------------------------|
"""
from __future__ import with_statement
import os
import sys
import json
from oclfleximporter import OclFlexImporter
from datimsync import DatimSync


class DatimSyncMer(DatimSync):
    """ Class to manage DATIM MER Indicators Synchronization """

    # Dataset ID settings
    OCL_DATASET_ENDPOINT = '/orgs/PEPFAR/collections/?verbose=true&limit=200'
    REPO_ACTIVE_ATTR = 'datim_sync_mer'
    DATASET_REPOSITORIES_FILENAME = 'ocl_dataset_repos_export.json'

    # File names
    NEW_IMPORT_SCRIPT_FILENAME = 'mer_dhis2ocl_import_script.json'
    DHIS2_CONVERTED_EXPORT_FILENAME = 'mer_dhis2_converted_export.json'
    OCL_CLEANED_EXPORT_FILENAME = 'mer_ocl_cleaned_export.json'

    # Import batches
    IMPORT_BATCH_MER = 'MER'
    IMPORT_BATCHES = [IMPORT_BATCH_MER]

    # DATIM DHIS2 Query Definitions
    DHIS2_QUERIES = {
        'MER': {
            'name': 'DATIM-DHIS2 MER Indicators',
            'query': '/api/dataElements.xml?fields=id,code,name,shortName,lastUpdated,description,'
                     'categoryCombo[id,code,name,lastUpdated,created,'
                     'categoryOptionCombos[id,code,name,lastUpdated,created]],'
                     'dataSetElements[*,dataSet[id,name,shortName]]&'
                     'paging=false&filter=dataSetElements.dataSet.id:in:[{{active_dataset_ids}}]',
            'new_export_filename': 'new_dhis2_mer_export_raw.json',
            'old_export_filename': 'old_dhis2_mer_export_raw.json',
            'converted_export_filename': 'new_dhis2_mer_export_converted.json',
            'conversion_method': 'dhis2diff_mer'
        }
    }

    # OCL Export Definitions
    OCL_EXPORT_DEFS = {
        'MER': {'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-DoD-FY17Q1/'},
        'MER-R-Facility-DoD-FY17Q1': {'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-DoD-FY17Q1/'},
    }

    def __init__(self, oclenv='', oclapitoken='', dhis2env='', dhis2uid='', dhis2pwd='', compare2previousexport=True,
                 runoffline=False, verbosity=0, data_check_only=False, import_test_mode=False, import_limit=0):
        DatimSync.__init__(self)

        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.dhis2env = dhis2env
        self.dhis2uid = dhis2uid
        self.dhis2pwd = dhis2pwd
        self.runoffline = runoffline
        self.verbosity = verbosity
        self.compare2previousexport = compare2previousexport
        self.import_test_mode = import_test_mode
        self.import_limit = import_limit
        self.data_check_only = data_check_only
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    def dhis2diff_mer(self, dhis2_query_def=None, conversion_attr=None):
        """
        Convert new DHIS2 MER export to the diff format
        :param dhis2_query_def: DHIS2 query definition
        :param conversion_attr: Optional dictionary of attributes to pass to the conversion method
        :return: Boolean
        """
        with open(self.attach_absolute_path(dhis2_query_def['new_export_filename']), "rb") as input_file:
            new_dhis2_export = json.load(input_file)
            ocl_dataset_repos = conversion_attr['ocl_dataset_repos']
            num_concepts = 0
            num_references = 0

            # Iterate through each DataElement and transform to an OCL-JSON concept
            for de in new_dhis2_export['dataElements']:
                concept_id = de['code']
                concept_key = '/orgs/PEPFAR/sources/MER/concepts/' + concept_id + '/'
                c = {
                    'type': 'Concept',
                    'id': concept_id,
                    'concept_class': 'Indicator',
                    'datatype': 'Varies',
                    'owner': 'PEPFAR',
                    'owner_type': 'Organization',
                    'source': 'MER Indicators',
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
                        },
                        {
                            'name': de['shortName'],
                            'name_type': 'Short Name',
                            'locale': 'en',
                            'locale_preferred': False,
                            'external_id': None,
                        }
                    ],
                    'extras': {'Dataset':''}
                }
                self.dhis2_diff[self.IMPORT_BATCH_MERIndicators][self.RESOURCE_TYPE_CONCEPT][concept_key] = c
                num_concepts += 1

                # Iterate through each DataElementGroup and transform to an OCL-JSON reference
                for deg in de['dataElementGroups']:
                    collection_id = ocl_dataset_repos[deg['id']]['id']
                    concept_url = '/orgs/PEPFAR/sources/MERIndicators/concepts/' + concept_id + '/'
                    concept_ref_key = ('/orgs/PEPFAR/collections/' + collection_id +
                                       '/references/?concept=' + concept_url)
                    r = {
                        'type': 'Reference',
                        'owner': 'PEPFAR',
                        'owner_type': 'Organization',
                        'collection': collection_id,
                        'data': {"expressions": [concept_url]}
                    }
                    self.dhis2_diff[self.IMPORT_BATCH_MERIndicators][self.RESOURCE_TYPE_CONCEPT_REF][
                        concept_ref_key] = r
                    num_references += 1

            if self.verbosity:
                self.log('DHIS2 export "%s" successfully transformed to %s concepts + %s references (%s total)' % (
                    dhis2_query_def['new_export_filename'], num_concepts,
                    num_references, num_concepts + num_references))
            return True


# Default Script Settings
verbosity = 2  # 0=none, 1=some, 2=all
import_limit = 0  # Number of resources to import; 0=all
import_test_mode = False  # Set to True to see which import API requests would be performed on OCL
runoffline = False  # Set to true to use local copies of dhis2/ocl exports
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
    import_limit = 0
    import_test_mode = False
    compare2previousexport = False
    runoffline = False
    dhis2env = 'https://dev-de.datim.org/'
    dhis2uid = 'jpayne'
    dhis2pwd = 'Johnpayne1!'

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
mer_sync = DatimSyncMer(oclenv=oclenv, oclapitoken=oclapitoken,
                         dhis2env=dhis2env, dhis2uid=dhis2uid, dhis2pwd=dhis2pwd,
                         compare2previousexport=compare2previousexport,
                         runoffline=runoffline, verbosity=verbosity,
                         import_test_mode=import_test_mode,
                         import_limit=import_limit)
mer_sync.run()
#mer_sync.data_check()
