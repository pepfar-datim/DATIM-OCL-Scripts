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
from datimsync import DatimSync


class DatimSyncMer(DatimSync):
    """ Class to manage DATIM MER Indicators Synchronization """

    # Dataset ID settings
    OCL_DATASET_ENDPOINT = '/orgs/PEPFAR/collections/?verbose=true&limit=200'
    REPO_ACTIVE_ATTR = 'datim_sync_mer'

    # File names
    DATASET_REPOSITORIES_FILENAME = 'mer_ocl_dataset_repos_export.json'
    NEW_IMPORT_SCRIPT_FILENAME = 'mer_dhis2ocl_import_script.json'
    DHIS2_CONVERTED_EXPORT_FILENAME = 'mer_dhis2_converted_export.json'
    OCL_CLEANED_EXPORT_FILENAME = 'mer_ocl_cleaned_export.json'

    # Import batches
    IMPORT_BATCH_MER = 'MER'
    IMPORT_BATCHES = [IMPORT_BATCH_MER]

    # DATIM DHIS2 Query Definitions
    DHIS2_QUERIES = {
        'MER': {
            'id': 'MER',
            'name': 'DATIM-DHIS2 MER Indicators',
            'query': '/api/dataElements.json?fields=id,code,name,shortName,lastUpdated,description,'
                     'categoryCombo[id,code,name,lastUpdated,created,'
                     'categoryOptionCombos[id,code,name,lastUpdated,created]],'
                     'dataSetElements[*,dataSet[id,name,shortName]]&'
                     'paging=false&filter=dataSetElements.dataSet.id:in:[{{active_dataset_ids}}]',
            'conversion_method': 'dhis2diff_mer'
        }
    }

    # OCL Export Definitions
    OCL_EXPORT_DEFS = {
        'MER': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/sources/MER/'},
        'MER-R-Facility-DoD-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-DoD-FY17Q1/'},
        'MER-R-Facility-DoD-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-DoD-FY17Q2/'},
        'MER-R-Facility-DoD-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-DoD-FY16Q4/'},
        'MER-R-Facility-DoD-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-DoD-FY16Q1Q2Q3/'},
        'HC-R-COP-Prioritization-SNU-USG-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-R-COP-Prioritization-SNU-USG-FY16Q4/'},
        'HC-R-Narratives-USG-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Narratives-USG-FY16Q1Q2Q3/'},
        'HC-R-Narratives-USG-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Narratives-USG-FY16Q4/'},
        'HC-R-Narratives-USG-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Narratives-USG-FY17Q1/'},
        'HC-R-Narratives-USG-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Narratives-USG-FY17Q2/'},
        'HC-R-Operating-Unit-Level-USG-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Operating-Unit-Level-USG-FY16Q1Q2Q3/'},
        'HC-R-Operating-Unit-Level-USG-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Operating-Unit-Level-USG-FY16Q4/'},
        'HC-T-COP-Prioritization-SNU-USG-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-T-COP-Prioritization-SNU-USG-FY17/'},
        'HC-T-COP-Prioritization-SNU-USG-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-T-COP-Prioritization-SNU-USG-FY18/'},
        'HC-T-Narratives-USG-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-T-Narratives-USG-FY16/'},
        'HC-T-Narratives-USG-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-T-Narratives-USG-FY17/'},
        'HC-T-Operating-Unit-Level-USG-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-T-Operating-Unit-Level-USG-FY16/'},
        'HC-T-Operating-Unit-Level-USG-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-T-Operating-Unit-Level-USG-FY17/'},
        'HC-T-Operating-Unit-Level-USG-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/HC-T-Operating-Unit-Level-USG-FY18/'},
        'MER-R-Community-DoD-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-DoD-FY16Q1Q2Q3/'},
        'MER-R-Community-DoD-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-DoD-FY16Q4/'},
        'MER-R-Community-DoD-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-DoD-FY17Q1/'},
        'MER-R-Community-DoD-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-DoD-FY17Q2/'},
        'MER-R-Community-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-FY16Q1Q2Q3/'},
        'MER-R-Community-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-FY16Q4/'},
        'MER-R-Community-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-FY17Q1/'},
        'MER-R-Community-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-FY17Q2/'},
        'MER-R-Facility-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-FY16Q1Q2Q3/'},
        'MER-R-Facility-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-FY16Q4/'},
        'MER-R-Facility-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-FY17Q1/'},
        'MER-R-Facility-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-FY17Q2/'},
        'MER-R-Medical-Store-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Medical-Store-FY16Q1Q2Q3/'},
        'MER-R-Medical-Store-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Medical-Store-FY16Q4/'},
        'MER-R-Medical-Store-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Medical-Store-FY17Q1/'},
        'MER-R-Narratives-IM-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Narratives-IM-FY16Q1Q2Q3/'},
        'MER-R-Narratives-IM-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Narratives-IM-FY16Q4/'},
        'MER-R-Narratives-IM-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Narratives-IM-FY17Q1/'},
        'MER-R-Narratives-IM-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Narratives-IM-FY17Q2/'},
        'MER-R-Operating-Unit-Level-IM-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Operating-Unit-Level-IM-FY16Q1Q2Q3/'},
        'MER-R-Operating-Unit-Level-IM-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Operating-Unit-Level-IM-FY16Q4/'},
        'MER-R-Operating-Unit-Level-IM-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Operating-Unit-Level-IM-FY17Q1/'},
        'MER-R-Operating-Unit-Level-IM-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Operating-Unit-Level-IM-FY17Q2/'},
        'MER-T-Community-DoD-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Community-DoD-FY16/'},
        'MER-T-Community-DoD-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Community-DoD-FY17/'},
        'MER-T-Community-DoD-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Community-DoD-FY18/'},
        'MER-T-Community-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Community-FY16/'},
        'MER-T-Community-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Community-FY17/'},
        'MER-T-Community-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Community-FY18/'},
        'MER-T-Facility-DoD-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Facility-DoD-FY16/'},
        'MER-T-Facility-DoD-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Facility-DoD-FY17/'},
        'MER-T-Facility-DoD-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Facility-DoD-FY18/'},
        'MER-T-Facility-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Facility-FY16/'},
        'MER-T-Facility-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Facility-FY17/'},
        'MER-T-Facility-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Facility-FY18/'},
        'MER-T-Narratives-IM-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Narratives-IM-FY16/'},
        'MER-T-Narratives-IM-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Narratives-IM-FY17/'},
        'MER-T-Narratives-IM-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Narratives-IM-FY18/'},
        'MER-T-Operating-Unit-Level-IM-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Operating-Unit-Level-IM-FY16/'},
        'MER-T-Operating-Unit-Level-IM-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Operating-Unit-Level-IM-FY17/'},
        'MER-T-Operating-Unit-Level-IM-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Operating-Unit-Level-IM-FY18/'},
        'Planning-Attributes-COP-Prioritization-National-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/Planning-Attributes-COP-Prioritization-National-FY18/'},
        'Planning-Attributes-COP-Prioritization-SNU-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'endpoint': '/orgs/PEPFAR/collections/Planning-Attributes-COP-Prioritization-SNU-FY18/'}
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

    def dhis2diff_mer(self, dhis2_query_def=None, conversion_attr=None):
        """
        Convert new DHIS2 MER export to the diff format
        :param dhis2_query_def: DHIS2 query definition
        :param conversion_attr: Optional dictionary of attributes to pass to the conversion method
        :return: Boolean
        """
        dhis2filename_export_new = self.dhis2filename_export_new(dhis2_query_def['id'])
        with open(self.attach_absolute_path(dhis2filename_export_new), "rb") as input_file:
            self.vlog(1, 'Loading new DHIS2 export "%s"...' % dhis2filename_export_new)
            new_dhis2_export = json.load(input_file)
            ocl_dataset_repos = conversion_attr['ocl_dataset_repos']
            num_indicators = 0
            num_disaggregates = 0
            num_mappings = 0
            num_indicator_refs = 0
            num_disaggregate_refs = 0

            # Iterate through each DataElement and transform to an Indicator concept
            for de in new_dhis2_export['dataElements']:
                indicator_concept_id = de['code']
                indicator_concept_url = '/orgs/PEPFAR/sources/MER/concepts/' + indicator_concept_id + '/'
                indicator_concept_key = indicator_concept_url
                indicator_concept = {
                    'type': 'Concept',
                    'id': indicator_concept_id,
                    'concept_class': 'Indicator',
                    'datatype': 'Numeric',
                    'owner': 'PEPFAR',
                    'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                    'source': 'MER',
                    'retired': False,
                    'external_id': de['id'],
                    'descriptions': None,
                    'extras': None,
                    'names': [
                        {
                            'name': de['name'],
                            'name_type': 'Fully Specified',
                            'locale': 'en',
                            'locale_preferred': True,
                            'external_id': None,
                        },
                        {
                            'name': de['shortName'],
                            'name_type': 'Short',
                            'locale': 'en',
                            'locale_preferred': False,
                            'external_id': None,
                        }
                    ],
                }
                if 'description' in de and de['description']:
                    indicator_concept['descriptions'] = [
                        {
                            'description': de['description'],
                            'description_type': 'Description',
                            'locale': 'en',
                            'locale_preferred': True,
                            'external_id': None,
                        }
                    ]
                self.dhis2_diff[self.IMPORT_BATCH_MER][self.RESOURCE_TYPE_CONCEPT][
                    indicator_concept_key] = indicator_concept
                num_indicators += 1

                # Build disaggregates concepts and mappings
                indicator_disaggregate_concept_urls = []
                for coc in de['categoryCombo']['categoryOptionCombos']:
                    disaggregate_concept_id = coc['code']
                    disaggregate_concept_url = '/orgs/PEPFAR/sources/MER/concepts/' + disaggregate_concept_id + '/'
                    disaggregate_concept_key = disaggregate_concept_url
                    indicator_disaggregate_concept_urls.append(disaggregate_concept_url)

                    # Only build the disaggregate concept if it has not already been defined
                    if disaggregate_concept_key not in self.dhis2_diff[
                            self.IMPORT_BATCH_MER][self.RESOURCE_TYPE_CONCEPT]:
                        disaggregate_concept = {
                            'type': 'Concept',
                            'id': disaggregate_concept_id,
                            'concept_class': 'Disaggregate',
                            'datatype': 'None',
                            'owner': 'PEPFAR',
                            'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                            'source': 'MER',
                            'retired': False,
                            'descriptions': None,
                            'external_id': coc['id'],
                            'extras': None,
                            'names': [
                                {
                                    'name': coc['name'],
                                    'name_type': 'Fully Specified',
                                    'locale': 'en',
                                    'locale_preferred': True,
                                    'external_id': None,
                                }
                            ]
                        }
                        self.dhis2_diff[self.IMPORT_BATCH_MER][
                            self.RESOURCE_TYPE_CONCEPT][disaggregate_concept_key] = disaggregate_concept
                        num_disaggregates += 1

                    # Build the mapping
                    map_type = 'Has Option'
                    disaggregate_mapping_key = ('/orgs/PEPFAR/sources/MER/mappings/?from=' + indicator_concept_url +
                                                '&maptype=' + map_type + '&to=' + disaggregate_concept_url)
                    disaggregate_mapping = {
                        'type': "Mapping",
                        'owner': 'PEPFAR',
                        'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                        'source': 'MER',
                        'map_type': map_type,
                        'from_concept_url': indicator_concept_url,
                        'to_concept_url': disaggregate_concept_url,
                        'external_id': None,
                        'extras': None,
                        'retired': False,
                    }
                    self.dhis2_diff[self.IMPORT_BATCH_MER][self.RESOURCE_TYPE_MAPPING][
                        disaggregate_mapping_key] = disaggregate_mapping
                    num_mappings += 1

                # Iterate through DataSets to transform to build references
                # NOTE: References are created for the indicator as well as each of its disaggregates and mappings
                for dse in de['dataSetElements']:
                    ds = dse['dataSet']

                    # Confirm that this dataset is one of the ones that we're interested in
                    if ds['id'] not in ocl_dataset_repos:
                        continue
                    collection_id = ocl_dataset_repos[ds['id']]['id']

                    # Build the Indicator concept reference - mappings for this reference will be added automatically
                    indicator_ref_key, indicator_ref = self.get_concept_reference_json(
                        owner_id='PEPFAR', collection_id=collection_id, concept_url=indicator_concept_url)
                    self.dhis2_diff[self.IMPORT_BATCH_MER][self.RESOURCE_TYPE_CONCEPT_REF][
                        indicator_ref_key] = indicator_ref
                    num_indicator_refs += 1

                    # Build the Disaggregate concept reference
                    for disaggregate_concept_url in indicator_disaggregate_concept_urls:
                        disaggregate_ref_key, disaggregate_ref = self.get_concept_reference_json(
                            owner_id='PEPFAR', collection_id=collection_id, concept_url=disaggregate_concept_url)
                        if disaggregate_ref_key not in self.dhis2_diff[
                                self.IMPORT_BATCH_MER][self.RESOURCE_TYPE_CONCEPT_REF]:
                            self.dhis2_diff[self.IMPORT_BATCH_MER][self.RESOURCE_TYPE_CONCEPT_REF][
                                disaggregate_ref_key] = disaggregate_ref
                            num_disaggregate_refs += 1

            self.vlog(1, 'DHIS2 export "%s" successfully transformed to %s indicators, '
                         '%s disaggregates, %s mappings, %s indicator references, and %s disaggregate references' % (
                            dhis2filename_export_new, num_indicators, num_disaggregates, num_mappings,
                            num_indicator_refs, num_disaggregate_refs))
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
    import_limit = 0
    import_test_mode = False
    compare2previousexport = False
    run_dhis2_offline = True
    run_ocl_offline = False
    dhis2env = 'https://dev-de.datim.org/'
    dhis2uid = 'paynejd'
    dhis2pwd = 'Jonpayne1!'

    # JetStream Staging - user=paynejd
    # oclenv = 'https://oclapi-stg.openmrs.org'
    # oclapitoken = 'a61ba53ed7b8b26ece8fcfc53022b645de0ec055'

    # JetStream QA - user=paynejd
    # oclenv = 'https://api.qa.openconceptlab.org'
    # oclapitoken = 'a5678e5f7971f3003e7be563ee4b90297b841f05'

    # JetStream Staging user=datim-admin
    oclenv = 'https://api.staging.openconceptlab.org'
    oclapitoken = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'

# Create sync object and run
mer_sync = DatimSyncMer(oclenv=oclenv, oclapitoken=oclapitoken,
                        dhis2env=dhis2env, dhis2uid=dhis2uid, dhis2pwd=dhis2pwd,
                        compare2previousexport=compare2previousexport,
                        run_dhis2_offline=run_dhis2_offline, run_ocl_offline=run_ocl_offline,
                        verbosity=verbosity,
                        import_test_mode=import_test_mode,
                        import_limit=import_limit)
mer_sync.run(resource_types=[DatimSyncMer.RESOURCE_TYPE_MAPPING])
# mer_sync.data_check()
