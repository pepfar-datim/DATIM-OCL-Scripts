"""
Class to synchronize FY19 DATIM MOH Alignment definitions between DHIS2 and OCL.
The script runs 1 import batch, consisting of one query to DHIS2, which is synchronized with
repositories in OCL as described below.
|----------------|----------|----------------------------------------|
| ImportBatch    | DHIS2    | OCL                                    |
|----------------|----------|----------------------------------------|
| MOH-FY19       | MOH-FY19 | /orgs/PEPFAR/sources/DATIM-MOH-FY19/   |
|----------------|----------|----------------------------------------|

In order to run this script, the org and source in OCL must already exist (e.g.
/orgs/PEPFAR/sources/DATIM-MOH-FY19/). Refer to init/importinit.py for more information
and to import the required starter content.

TODO: Implement new repo versioning model (e.g. FY19.v0)
TODO: Add "indicator_category_code" attribute for each indicator (e.g. PMTCT_STAT)
"""

import json

from . import datimconstants, datimsync, datimsyncmohhelper


class DatimSyncMohFy19(datimsync.DatimSync):
    """ Class to synchronize FY19 DATIM MOH Alignment between DHIS2 and OCL """

    # Name of this sync script (used to name files and in logging)
    SYNC_NAME = 'MOH-FY19'

    # Dataset ID settings
    OCL_DATASET_ENDPOINT = datimconstants.DatimConstants.OCL_DATASET_ENDPOINT_MOH
    REPO_ACTIVE_ATTR = datimconstants.DatimConstants.REPO_ACTIVE_ATTR_MOH_FY19

    # ID of the org and source in OCL
    DATIM_MOH_ORG_ID = 'PEPFAR'
    DATIM_MOH_SOURCE_ID = 'DATIM-MOH-FY19'
    DATIM_MOH_DE_CONCEPT_CLASS = 'Data Element'
    DATIM_MOH_DE_DATATYPE = 'Numeric'
    DATIM_MOH_COC_CONCEPT_CLASS = 'Disaggregate'  # This is the DHIS2 categoryOptionCombo equivalent
    DATIM_MOH_COC_DATATYPE = 'None'
    DATIM_MOH_MAP_TYPE_DE_TO_COC = 'Has Option'

    # File names
    DATASET_REPOSITORIES_FILENAME = 'moh_fy19_ocl_dataset_repos_export.json'
    NEW_IMPORT_SCRIPT_FILENAME = 'moh_fy19_dhis2ocl_import_script.json'
    DHIS2_CONVERTED_EXPORT_FILENAME = 'moh_fy19_dhis2_converted_export.json'
    OCL_CLEANED_EXPORT_FILENAME = 'moh_fy19_ocl_cleaned_export.json'

    # Import batches
    IMPORT_BATCHES = [datimconstants.DatimConstants.IMPORT_BATCH_MOH_FY19]

    # DATIM DHIS2 Query Definitions
    DHIS2_QUERIES = datimconstants.DatimConstants.MOH_FY19_DHIS2_QUERIES

    # OCL Export Definitions
    OCL_EXPORT_DEFS = datimconstants.DatimConstants.MOH_FY19_OCL_EXPORT_DEFS

    def __init__(self, oclenv='', oclapitoken='', dhis2env='', dhis2uid='', dhis2pwd='',
                 compare2previousexport=True, run_dhis2_offline=False, run_ocl_offline=False,
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
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    @staticmethod
    def get_indicator_category_code(data_element_id):
        return '_'.join(data_element_id.split('_')[:2])

    def dhis2diff_moh(self, dhis2_query_def=None, conversion_attr=None):
        """
        Convert new DHIS2 MOH export to the diff format
        :param dhis2_query_def: DHIS2 query definition
        :param conversion_attr: Optional dictionary of attributes to pass to the conversion method
        :return: Boolean
        """
        dhis2filename_export_new = self.dhis2filename_export_new(dhis2_query_def['id'])
        with open(self.attach_absolute_data_path(dhis2filename_export_new), "rb") as input_file:
            self.vlog(1, 'Loading new DHIS2 export "%s"...' % dhis2filename_export_new)
            new_dhis2_export = json.load(input_file)
            ocl_dataset_repos = conversion_attr['ocl_dataset_repos']

            # Counts
            num_data_elements = 0
            num_disaggregates = 0
            num_mappings = 0
            num_data_element_refs = 0
            num_disaggregate_refs = 0

            # Iterate through each DataElement and transform to Data Element concepts
            for de in new_dhis2_export['dataElements']:
                de_concept_id = de['code']
                de_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                    self.DATIM_MOH_ORG_ID, self.DATIM_MOH_SOURCE_ID, de_concept_id)
                de_concept_key = de_concept_url
                de_concept = {
                    'type': 'Concept',
                    'id': de_concept_id,
                    'concept_class': self.DATIM_MOH_DE_CONCEPT_CLASS,
                    'datatype': self.DATIM_MOH_DE_DATATYPE,
                    'owner': self.DATIM_MOH_ORG_ID,
                    'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                    'source': self.DATIM_MOH_SOURCE_ID,
                    'retired': False,
                    'external_id': de['id'],  # dataelementuid
                    'descriptions': None,
                    'extras': {'indicator_category_code': DatimSyncMohFy19.get_indicator_category_code(de_concept_id)},
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
                    de_concept['descriptions'] = [
                        {
                            'description': de['description'],
                            'description_type': 'Description',
                            'locale': 'en',
                            'locale_preferred': True,
                            'external_id': None,
                        }
                    ]
                self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MOH_FY19][self.RESOURCE_TYPE_CONCEPT][
                    de_concept_key] = de_concept
                num_data_elements += 1

                # Build concepts and mappings for Disaggregates (i.e. categoryOptionCombos)
                de_disag_concept_urls = []
                for coc in de['categoryCombo']['categoryOptionCombos']:
                    # Classify the disag and skip if INVALID
                    disag_classification = datimsyncmohhelper.DatimSyncMohHelper.get_disag_classification_fy19(
                        de_code=de_concept_id, de_uid=de['id'], coc_name=coc['name'])
                    if disag_classification == datimconstants.DatimConstants.DISAG_CLASSIFICATION_INVALID:
                        continue

                    # Build disag key and URL
                    disaggregate_concept_id = coc['id']  # "id" is the same as "code", but "code" is sometimes missing
                    disaggregate_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
                        DatimSyncMohFy19.DATIM_MOH_ORG_ID, DatimSyncMohFy19.DATIM_MOH_SOURCE_ID,
                        disaggregate_concept_id)
                    disaggregate_concept_key = disaggregate_concept_url
                    de_disag_concept_urls.append(disaggregate_concept_url)

                    # Only build the disaggregate concept if it has not already been defined
                    # NOTE: A disag will appear multiple times if its DHIS2 Category is used on more than 1 dataElement
                    if disaggregate_concept_key not in self.dhis2_diff[
                            datimconstants.DatimConstants.IMPORT_BATCH_MOH_FY19][self.RESOURCE_TYPE_CONCEPT]:
                        disaggregate_concept = {
                            'type': 'Concept',
                            'id': disaggregate_concept_id,
                            'concept_class': self.DATIM_MOH_COC_CONCEPT_CLASS,
                            'datatype': self.DATIM_MOH_COC_DATATYPE,
                            'owner': self.DATIM_MOH_ORG_ID,
                            'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                            'source': self.DATIM_MOH_SOURCE_ID,
                            'retired': False,
                            'descriptions': None,
                            'external_id': coc['id'],
                            'extras': {'classification': disag_classification},
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
                        self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MOH_FY19][
                            self.RESOURCE_TYPE_CONCEPT][disaggregate_concept_key] = disaggregate_concept
                        num_disaggregates += 1

                    # Build the mapping
                    map_type = self.DATIM_MOH_MAP_TYPE_DE_TO_COC
                    disaggregate_mapping_key = datimsync.DatimSync.get_mapping_key(
                        mapping_owner_type=self.RESOURCE_TYPE_ORGANIZATION, mapping_owner_id=self.DATIM_MOH_ORG_ID,
                        mapping_source_id=self.DATIM_MOH_SOURCE_ID, from_concept_url=de_concept_url,
                        map_type=map_type, to_concept_url=disaggregate_concept_url)
                    disaggregate_mapping = {
                        'type': 'Mapping',
                        'owner': self.DATIM_MOH_ORG_ID,
                        'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                        'source': self.DATIM_MOH_SOURCE_ID,
                        'map_type': map_type,
                        'from_concept_url': de_concept_url,
                        'to_concept_url': disaggregate_concept_url,
                        'external_id': None,
                        'extras': None,
                        'retired': False,
                    }
                    self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MOH_FY19][
                        self.RESOURCE_TYPE_MAPPING][disaggregate_mapping_key] = disaggregate_mapping
                    num_mappings += 1

                # Iterate thru DataSets to collection references for each data element and its disags and mappings
                for dse in de['dataSetElements']:
                    ds = dse['dataSet']

                    # Confirm that this is a dataset we're interested in and get corresponding OCL collection ID
                    if ds['id'] not in ocl_dataset_repos:
                        continue
                    collection_id = ocl_dataset_repos[ds['id']]['id']

                    # Build the data element concept reference - mappings for this reference are added automatically
                    de_ref_key, de_ref = self.get_concept_reference_json(
                        collection_owner_id=self.DATIM_MOH_ORG_ID,
                        collection_owner_type=self.RESOURCE_TYPE_ORGANIZATION,
                        collection_id=collection_id, concept_url=de_concept_url)
                    self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MOH_FY19][
                        self.RESOURCE_TYPE_CONCEPT_REF][de_ref_key] = de_ref
                    num_data_element_refs += 1

                    # Build the Disaggregate concept references
                    # Note this automatically excludes 'INVALID' disags because they are not in de_disag_concept_urls
                    for disaggregate_concept_url in de_disag_concept_urls:
                        disaggregate_ref_key, disaggregate_ref = self.get_concept_reference_json(
                            collection_owner_id=self.DATIM_MOH_ORG_ID,
                            collection_owner_type=self.RESOURCE_TYPE_ORGANIZATION,
                            collection_id=collection_id, concept_url=disaggregate_concept_url)
                        if disaggregate_ref_key not in self.dhis2_diff[
                                datimconstants.DatimConstants.IMPORT_BATCH_MOH_FY19][self.RESOURCE_TYPE_CONCEPT_REF]:
                            self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MOH_FY19][
                                self.RESOURCE_TYPE_CONCEPT_REF][disaggregate_ref_key] = disaggregate_ref
                            num_disaggregate_refs += 1

            # End this process by logging a summary of the number and type of resources transformed
            self.vlog(1, 'DATIM-MOH FY19 DHIS2 export "%s" successfully transformed to %s data element concepts, '
                         '%s disaggregate concepts, %s mappings from data elements to disaggregates, '
                         '%s data element concept references, and %s disaggregate concept references' % (
                            dhis2filename_export_new, num_data_elements, num_disaggregates, num_mappings,
                            num_data_element_refs, num_disaggregate_refs))
            return True
