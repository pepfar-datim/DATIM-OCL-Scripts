"""
Class to synchronize DATIM DHIS2 MOH F717 Indicator definitions with OCL
The script runs 1 import batch, which consists of two queries to DHIS2, which are
synchronized with repositories in OCL as described below.
|-------------|--------|-------------------------------------------------|
| ImportBatch | DHIS2  | OCL                                             |
|-------------|--------|-------------------------------------------------|
| MOH         | MOH    | /orgs/PEPFAR/sources/DATIM-MOH/                 |
|-------------|--------|-------------------------------------------------|

TODO: This class (FY17) must be updated to the model used by FY18 and FY19 before using!
"""
from __future__ import with_statement
import json
import datimsync
import datimconstants


class DatimSyncMoh(datimsync.DatimSync):
    """ Class to manage DATIM MOH Indicators Synchronization """

    # Name of this sync script (used to name files and in logging)
    SYNC_NAME = 'MOH'

    # Dataset ID settings
    OCL_DATASET_ENDPOINT = '/orgs/PEPFAR/collections/?verbose=true&limit=200'
    REPO_ACTIVE_ATTR = 'datim_sync_moh'

    # File names
    DATASET_REPOSITORIES_FILENAME = 'moh_ocl_dataset_repos_export.json'
    NEW_IMPORT_SCRIPT_FILENAME = 'moh_dhis2ocl_import_script.json'
    DHIS2_CONVERTED_EXPORT_FILENAME = 'moh_dhis2_converted_export.json'
    OCL_CLEANED_EXPORT_FILENAME = 'moh_ocl_cleaned_export.json'

    # Import batches
    IMPORT_BATCHES = [datimconstants.DatimConstants.IMPORT_BATCH_MOH]

    # DATIM DHIS2 Query Definitions
    DHIS2_QUERIES = datimconstants.DatimConstants.MOH_DHIS2_QUERIES

    # OCL Export Definitions
    OCL_EXPORT_DEFS = datimconstants.DatimConstants.MOH_OCL_EXPORT_DEFS

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
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

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
            num_indicators = 0
            num_disaggregates = 0
            num_mappings = 0
            num_indicator_refs = 0
            num_disaggregate_refs = 0

            # Iterate through each DataElement and transform to an Indicator concept
            for de in new_dhis2_export['dataElements']:
                indicator_concept_id = de['code']
                indicator_concept_url = '/orgs/PEPFAR/sources/DATIM-MOH/concepts/' + indicator_concept_id + '/'
                indicator_concept_key = indicator_concept_url
                indicator_concept = {
                    'type': 'Concept',
                    'id': indicator_concept_id,
                    'concept_class': 'Indicator',
                    'datatype': 'Numeric',
                    'owner': 'PEPFAR',
                    'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                    'source': 'DATIM-MOH',
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
                self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MOH][self.RESOURCE_TYPE_CONCEPT][
                    indicator_concept_key] = indicator_concept
                num_indicators += 1

                # Build disaggregates concepts and mappings
                indicator_disaggregate_concept_urls = []
                for coc in de['categoryCombo']['categoryOptionCombos']:
                    disaggregate_concept_id = coc['id']  # "id" is the same as "code", but "code" is sometimes missing
                    disaggregate_concept_url = '/orgs/PEPFAR/sources/DATIM-MOH/concepts/' + disaggregate_concept_id + '/'
                    disaggregate_concept_key = disaggregate_concept_url
                    indicator_disaggregate_concept_urls.append(disaggregate_concept_url)

                    # Only build the disaggregate concept if it has not already been defined
                    if disaggregate_concept_key not in self.dhis2_diff[
                            datimconstants.DatimConstants.IMPORT_BATCH_MOH][self.RESOURCE_TYPE_CONCEPT]:
                        disaggregate_concept = {
                            'type': 'Concept',
                            'id': disaggregate_concept_id,
                            'concept_class': 'Disaggregate',
                            'datatype': 'None',
                            'owner': 'PEPFAR',
                            'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                            'source': 'DATIM-MOH',
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
                        self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MOH][
                            self.RESOURCE_TYPE_CONCEPT][disaggregate_concept_key] = disaggregate_concept
                        num_disaggregates += 1

                    # Build the mapping
                    map_type = 'Has Option'
                    disaggregate_mapping_key = self.get_mapping_key(
                        mapping_owner_type=self.RESOURCE_TYPE_ORGANIZATION, mapping_owner_id='PEPFAR',
                        mapping_source_id='DATIM-MOH', from_concept_url=indicator_concept_url, map_type=map_type,
                        to_concept_url=disaggregate_concept_url)
                    disaggregate_mapping = {
                        'type': 'Mapping',
                        'owner': 'PEPFAR',
                        'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                        'source': 'DATIM-MOH',
                        'map_type': map_type,
                        'from_concept_url': indicator_concept_url,
                        'to_concept_url': disaggregate_concept_url,
                        'external_id': None,
                        'extras': None,
                        'retired': False,
                    }
                    self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MOH][
                        self.RESOURCE_TYPE_MAPPING][disaggregate_mapping_key] = disaggregate_mapping
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
                        collection_owner_id='PEPFAR', collection_owner_type=self.RESOURCE_TYPE_ORGANIZATION,
                        collection_id=collection_id, concept_url=indicator_concept_url)
                    self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MOH][self.RESOURCE_TYPE_CONCEPT_REF][
                        indicator_ref_key] = indicator_ref
                    num_indicator_refs += 1

                    # Build the Disaggregate concept reference
                    for disaggregate_concept_url in indicator_disaggregate_concept_urls:
                        disaggregate_ref_key, disaggregate_ref = self.get_concept_reference_json(
                            collection_owner_id='PEPFAR', collection_owner_type=self.RESOURCE_TYPE_ORGANIZATION,
                            collection_id=collection_id, concept_url=disaggregate_concept_url)
                        if disaggregate_ref_key not in self.dhis2_diff[
                                datimconstants.DatimConstants.IMPORT_BATCH_MOH][self.RESOURCE_TYPE_CONCEPT_REF]:
                            self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_MOH][self.RESOURCE_TYPE_CONCEPT_REF][
                                disaggregate_ref_key] = disaggregate_ref
                            num_disaggregate_refs += 1

            self.vlog(1, 'DATIM-MOH DHIS2 export "%s" successfully transformed to %s indicator concepts, '
                         '%s disaggregate concepts, %s mappings from indicators to disaggregates, '
                         '%s indicator concept references, and %s disaggregate concept references' % (
                            dhis2filename_export_new, num_indicators, num_disaggregates, num_mappings,
                            num_indicator_refs, num_disaggregate_refs))
            return True
