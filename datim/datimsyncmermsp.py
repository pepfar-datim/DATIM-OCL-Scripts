"""
Class to synchronize DATIM DHIS2 MER-MSP Data Elememnt and Disag definitions with OCL
The script runs 1 import batch, which consists of two queries to DHIS2, which are
synchronized with repositories in OCL as described below.
|-------------|---------|-------------------------------------------------|
| ImportBatch | DHIS2   | OCL                                             |
|-------------|---------|-------------------------------------------------|
| MER-MSP     | MER-MSP | /orgs/PEPFAR/sources/MER-MSP/                   |
|             |         | /orgs/PEPFAR/collections/MER-*/                 |
|             |         | /orgs/PEPFAR/collections/HC-*/                  |
|             |         | /orgs/PEPFAR/collections/Planning-Attributes-*/ |
|-------------|---------|-------------------------------------------------|
"""

import json
from . import datimsync
from . import datimconstants
import ocldev.oclconstants
from . import datimbase


class DatimSyncMerMsp(datimsync.DatimSync):
    """ Class to manage DATIM MER-MSP Metadata Synchronization """

    # Name of this sync script (used to name files and in logging)
    SYNC_NAME = 'MER-MSP'

    # ID of the org and source in OCL
    DATIM_MER_MSP_ORG_ID = 'PEPFAR'
    DATIM_MER_MSP_SOURCE_ID = 'MER-MSP'
    DATIM_MER_MSP_INDICATOR_CONCEPT_CLASS = 'Indicator'
    DATIM_MER_MSP_INDICATOR_DATATYPE = 'None'
    DATIM_MER_MSP_MAP_TYPE_INDICATOR_TO_DE = 'Has Data Element'
    DATIM_MER_MSP_DE_CONCEPT_CLASS = 'Data Element'
    DATIM_MER_MSP_DE_DATATYPE = 'Numeric'
    DATIM_MER_MSP_MAP_TYPE_DE_TO_COC = 'Has Option'
    DATIM_MER_MSP_COC_CONCEPT_CLASS = 'Disaggregate'  # This is the DHIS2 categoryOptionCombo equivalent
    DATIM_MER_MSP_COC_DATATYPE = 'None'

    DATIM_MER_MSP_NAME_TYPE_FULL = 'Fully Specified'
    DATIM_MER_MSP_NAME_TYPE_SHORT = 'Short'
    DATIM_MER_MSP_DESCRIPTION_TYPE_DEFAULT = 'Description'

    # Dataset ID settings - Dataset IDs are hardcoded for this one
    # SYNC_LOAD_DATASETS = False
    OCL_DATASET_ENDPOINT = datimconstants.DatimConstants.OCL_DATASET_ENDPOINT_MER_MSP
    REPO_ACTIVE_ATTR = datimconstants.DatimConstants.REPO_ACTIVE_ATTR_MER_MSP

    # File names
    DATASET_REPOSITORIES_FILENAME = 'mer_msp_ocl_dataset_repos_export.json'
    NEW_IMPORT_SCRIPT_FILENAME = 'mer_msp_dhis2ocl_import_script.json'
    DHIS2_CONVERTED_EXPORT_FILENAME = 'mer_msp_dhis2_converted_export.json'
    OCL_CLEANED_EXPORT_FILENAME = 'mer_msp_ocl_cleaned_export.json'

    # Import batches
    IMPORT_BATCHES = [datimconstants.DatimConstants.IMPORT_BATCH_MER_MSP]

    # DATIM DHIS2 Query Definitions
    DHIS2_QUERIES = datimconstants.DatimConstants.MER_MSP_DHIS2_QUERIES

    # OCL Export Definitions
    OCL_EXPORT_DEFS = datimconstants.DatimConstants.MER_MSP_OCL_EXPORT_DEFS

    def __init__(self, oclenv='', oclapitoken='', dhis2env='', dhis2uid='', dhis2pwd='', compare2previousexport=True,
                 run_dhis2_offline=False, run_ocl_offline=False,
                 verbosity=0, data_check_only=False, import_test_mode=False, import_limit=0):
        datimsync.DatimSync.__init__(self)
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
        self.data_check_only = data_check_only
        self.import_test_mode = import_test_mode
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    def dhis2diff_mer_msp(self, dhis2_query_def=None, conversion_attr=None):
        """
        Convert new DHIS2 MER-MSP export to the diff format
        :param dhis2_query_def: DHIS2 query definition
        :param conversion_attr: Optional dictionary of attributes to pass to the conversion method
        :return: Boolean
        """
        dhis2filename_export_new = datimbase.DatimBase.dhis2filename_export_new(dhis2_query_def['id'])
        with open(self.attach_absolute_data_path(dhis2filename_export_new), "rb") as input_file:
            self.vlog(1, 'Loading new DHIS2 export "%s"...' % dhis2filename_export_new)
            new_dhis2_export = json.load(input_file)
            active_dataset_keys = conversion_attr['active_dataset_keys']

            # Counts
            num_indicators = 0
            num_data_elements = 0
            num_disaggregates = 0
            num_mappings_indicator_to_de = 0
            num_mappings_de_to_disag = 0
            num_indicator_refs = 0
            num_data_element_refs = 0
            num_disaggregate_refs = 0

            # Iterate through each DataElement and transform to a Data Element Concept
            for de in new_dhis2_export['dataElements']:
                de_concept_id = de['code']
                de_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                    self.DATIM_MER_MSP_ORG_ID, self.DATIM_MER_MSP_SOURCE_ID, de_concept_id)
                de_concept_key = de_concept_url
                de_concept = {
                    'type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_CONCEPT,
                    'id': de_concept_id,
                    'concept_class': self.DATIM_MER_MSP_DE_CONCEPT_CLASS,
                    'datatype': self.DATIM_MER_MSP_DE_DATATYPE,
                    'owner': self.DATIM_MER_MSP_ORG_ID,
                    'owner_type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION,
                    'source': self.DATIM_MER_MSP_SOURCE_ID,
                    'retired': False,
                    'external_id': de['id'],
                    'descriptions': None,
                    'extras': None,
                    'names': [
                        {
                            'name': de['name'],
                            'name_type': self.DATIM_MER_MSP_NAME_TYPE_FULL,
                            'locale': 'en',
                            'locale_preferred': True,
                            'external_id': None,
                        },
                        {
                            'name': de['shortName'],
                            'name_type': self.DATIM_MER_MSP_NAME_TYPE_SHORT,
                            'locale': 'en',
                            'locale_preferred': False,
                            'external_id': None,
                        }
                    ],
                }
                if 'description' in de and de['description']:
                    de_concept['descriptions'] = [
                        {
                            'description': de['description'],
                            'description_type': self.DATIM_MER_MSP_DESCRIPTION_TYPE_DEFAULT,
                            'locale': 'en',
                            'locale_preferred': True,
                            'external_id': None,
                        }
                    ]
                self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MER_MSP][
                    ocldev.oclconstants.OclConstants.RESOURCE_TYPE_CONCEPT][de_concept_key] = de_concept
                num_data_elements += 1

                # Build disaggregates concepts and mappings
                de_disaggregate_concept_urls = []
                for coc in de['categoryCombo']['categoryOptionCombos']:
                    disaggregate_concept_id = coc['id']  # "id" is the same as "code", but "code" is sometimes missing
                    disaggregate_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                        self.DATIM_MER_MSP_ORG_ID, self.DATIM_MER_MSP_SOURCE_ID, disaggregate_concept_id)
                    disaggregate_concept_key = disaggregate_concept_url
                    de_disaggregate_concept_urls.append(disaggregate_concept_url)

                    # Only build the disaggregate concept if it has not already been defined
                    if disaggregate_concept_key not in self.dhis2_diff[
                            datimconstants.DatimConstants.IMPORT_BATCH_MER_MSP][
                            ocldev.oclconstants.OclConstants.RESOURCE_TYPE_CONCEPT]:
                        disaggregate_concept = {
                            'type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_CONCEPT,
                            'id': disaggregate_concept_id,
                            'concept_class': self.DATIM_MER_MSP_COC_CONCEPT_CLASS,
                            'datatype': self.DATIM_MER_MSP_COC_DATATYPE,
                            'owner': self.DATIM_MER_MSP_ORG_ID,
                            'owner_type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION,
                            'source': self.DATIM_MER_MSP_SOURCE_ID,
                            'retired': False,
                            'descriptions': None,
                            'external_id': coc['id'],
                            'extras': None,
                            'names': [
                                {
                                    'name': coc['name'],
                                    'name_type': self.DATIM_MER_MSP_NAME_TYPE_FULL,
                                    'locale': 'en',
                                    'locale_preferred': True,
                                    'external_id': None,
                                }
                            ]
                        }
                        self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MER_MSP][
                            ocldev.oclconstants.OclConstants.RESOURCE_TYPE_CONCEPT][
                            disaggregate_concept_key] = disaggregate_concept
                        num_disaggregates += 1

                    # Build the mapping
                    disaggregate_mapping_key = datimsync.DatimSync.get_mapping_key(
                        mapping_owner_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION,
                        mapping_owner_id=self.DATIM_MER_MSP_ORG_ID, mapping_source_id=self.DATIM_MER_MSP_SOURCE_ID,
                        from_concept_url=de_concept_url, map_type=self.DATIM_MER_MSP_MAP_TYPE_DE_TO_COC,
                        to_concept_url=disaggregate_concept_url)
                    disaggregate_mapping = {
                        'type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_MAPPING,
                        'owner': self.DATIM_MER_MSP_ORG_ID,
                        'owner_type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION,
                        'source': self.DATIM_MER_MSP_SOURCE_ID,
                        'map_type': self.DATIM_MER_MSP_MAP_TYPE_DE_TO_COC,
                        'from_concept_url': de_concept_url,
                        'to_concept_url': disaggregate_concept_url,
                        'external_id': None,
                        'extras': None,
                        'retired': False,
                    }
                    self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MER_MSP][
                        ocldev.oclconstants.OclConstants.RESOURCE_TYPE_MAPPING][
                        disaggregate_mapping_key] = disaggregate_mapping
                    num_mappings_de_to_disag += 1

                # Iterate through DataSets to transform to build references
                # NOTE: References are created for the data elements as well as each of its disaggregates and mappings
                for dse in de['dataSetElements']:
                    ds = dse['dataSet']

                    # Confirm that this dataset is one of the ones that we're interested in
                    if ds['id'] not in active_dataset_keys:
                        continue
                    collection_id = ds['id']

                    """JP 2019-06-04: Tried this out, but current approach doesn't support auto-gen of collections
                    # Build the Collection
                    collection_key, collection_dict = self.get_collection_json(
                        owner_id=self.DATIM_MER_MSP_ORG_ID,
                        collection_owner_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION,
                        collection_id=collection_id, full_name=ds['name'], name=ds['name'], short_code=ds['shortName'],
                        external_id=ds['id'], collection_type='Subset')
                    self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MER_MSP][
                        ocldev.oclconstants.OclConstants.RESOURCE_TYPE_COLLECTION][collection_key] = collection_dict
                    num_collections += 1
                    """

                    # Build the Data Element Concept Reference - mapping references are added automatically
                    de_ref_key, de_ref = self.get_concept_reference_json(
                        collection_owner_id=self.DATIM_MER_MSP_ORG_ID,
                        collection_owner_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION,
                        collection_id=collection_id, concept_url=de_concept_url)
                    self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MER_MSP][
                        ocldev.oclconstants.OclConstants.RESOURCE_TYPE_CONCEPT_REF][de_ref_key] = de_ref
                    num_data_element_refs += 1

                    # Build the Disaggregate concept reference
                    for disaggregate_concept_url in de_disaggregate_concept_urls:
                        disaggregate_ref_key, disaggregate_ref = self.get_concept_reference_json(
                            collection_owner_id=self.DATIM_MER_MSP_ORG_ID,
                            collection_owner_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION,
                            collection_id=collection_id,
                            concept_url=disaggregate_concept_url)
                        if disaggregate_ref_key not in self.dhis2_diff[
                                datimconstants.DatimConstants.IMPORT_BATCH_MER_MSP][
                                ocldev.oclconstants.OclConstants.RESOURCE_TYPE_CONCEPT_REF]:
                            self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MER_MSP][
                                ocldev.oclconstants.OclConstants.RESOURCE_TYPE_CONCEPT_REF][
                                disaggregate_ref_key] = disaggregate_ref
                            num_disaggregate_refs += 1

            self.vlog(1, 'DHIS2 export "%s" successfully transformed to...\n'
                         '    %s Concepts: %s Indicators, %s Data Elements, %s Disaggregates\n'
                         '    %s Mappings: %s Indicators to Data Elements, %s Data Elements to Disaggregates\n'
                         '    %s Concept References: %s Data Elements, %s Disaggregates' % (
                            dhis2filename_export_new, (num_indicators + num_data_elements + num_disaggregates),
                            num_indicators, num_data_elements, num_disaggregates,
                            (num_mappings_indicator_to_de + num_mappings_de_to_disag),
                            num_mappings_indicator_to_de, num_mappings_de_to_disag,
                            (num_data_element_refs + num_disaggregate_refs),
                            num_data_element_refs, num_disaggregate_refs))
            return True
