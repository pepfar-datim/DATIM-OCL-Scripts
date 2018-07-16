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
import json
import datimsync
import datimconstants


class DatimSyncSims(datimsync.DatimSync):
    """ Class to manage DATIM SIMS Synchronization """

    # Name of this sync script (used to name files and in logging)
    SYNC_NAME = 'SIMS'

    # Dataset ID settings
    OCL_DATASET_ENDPOINT = '/orgs/PEPFAR/collections/?q=SIMS&verbose=true&limit=200'
    REPO_ACTIVE_ATTR = 'datim_sync_sims'

    # File names
    DATASET_REPOSITORIES_FILENAME = 'ocl_dataset_repos_export.json'
    NEW_IMPORT_SCRIPT_FILENAME = 'sims_dhis2ocl_import_script.json'
    DHIS2_CONVERTED_EXPORT_FILENAME = 'sims_dhis2_converted_export.json'
    OCL_CLEANED_EXPORT_FILENAME = 'sims_ocl_cleaned_export.json'

    # Import batches
    IMPORT_BATCHES = [datimconstants.DatimConstants.IMPORT_BATCH_SIMS]

    # DATIM DHIS2 Query Definitions
    DHIS2_QUERIES = datimconstants.DatimConstants.SIMS_DHIS2_QUERIES

    # OCL Export Definitions
    OCL_EXPORT_DEFS = datimconstants.DatimConstants.SIMS_OCL_EXPORT_DEFS

    def __init__(self, oclenv='', oclapitoken='', dhis2env='', dhis2uid='', dhis2pwd='', compare2previousexport=True,
                 run_dhis2_offline=False, run_ocl_offline=False, verbosity=0, import_limit=0):
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

    def dhis2diff_sims_option_sets(self, dhis2_query_def=None, conversion_attr=None):
        """
        Convert new DHIS2 SIMS Options export to the diff format
        :param dhis2_query_def:
        :param conversion_attr:
        :return:
        """
        dhis2filename_export_new = self.dhis2filename_export_new(dhis2_query_def['id'])
        with open(self.attach_absolute_data_path(dhis2filename_export_new), "rb") as input_file:
            new_dhis2_export = json.load(input_file)
            ocl_dataset_repos = conversion_attr['ocl_dataset_repos']
            num_concepts = 0
            num_references = 0

            # Iterate through each OptionSet and transform to an OCL-JSON concept
            for option_set in new_dhis2_export['optionSets']:
                for option in option_set['options']:
                    option_concept_id = option['id']
                    option_concept_url = '/orgs/PEPFAR/sources/SIMS/concepts/%s/' % option_concept_id
                    option_concept_key = option_concept_url
                    option_concept = {
                        'type': 'Concept',
                        'id': option_concept_id,
                        'concept_class': 'Option',
                        'datatype': 'None',
                        'owner': 'PEPFAR',
                        'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                        'source': 'SIMS',
                        'retired': False,
                        'descriptions': None,
                        'external_id': None,
                        'names': [
                            {
                                'name': option['name'],
                                'name_type': 'Fully Specified',
                                'locale': 'en',
                                'locale_preferred': True,
                                'external_id': None,
                            }
                        ],
                        'extras': {
                            'Option Set Name': option_set['name'],
                            'Option Set ID': option_set['id'],
                            'Option Code': option['code'],
                        }
                    }
                    self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_SIMS][self.RESOURCE_TYPE_CONCEPT][
                        option_concept_key] = option_concept
                    num_concepts += 1

                    # Add the concept to SIMS-Option-Sets collection
                    ocl_collection_id = 'SIMS-Option-Sets'
                    option_ref_key, option_ref = self.get_concept_reference_json(
                        collection_owner_id='PEPFAR', collection_owner_type=self.RESOURCE_TYPE_ORGANIZATION,
                        collection_id=ocl_collection_id, concept_url=option_concept_url)
                    self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_SIMS][self.RESOURCE_TYPE_CONCEPT_REF][
                        option_ref_key] = option_ref
                    num_references += 1

            self.vlog(1, 'DHIS2 export "%s" successfully transformed to %s option concepts and %s references' % (
                dhis2filename_export_new, num_concepts, num_references))
            return True

    def dhis2diff_sims_assessment_types(self, dhis2_query_def=None, conversion_attr=None):
        """
        Convert new DHIS2 SIMS Assessment Types export to the diff format
        :param dhis2_query_def: DHIS2 query definition
        :param conversion_attr: Optional dictionary of attributes to pass to the conversion method
        :return: Boolean
        """
        dhis2filename_export_new = self.dhis2filename_export_new(dhis2_query_def['id'])
        with open(self.attach_absolute_data_path(dhis2filename_export_new), "rb") as input_file:
            new_dhis2_export = json.load(input_file)
            ocl_dataset_repos = conversion_attr['ocl_dataset_repos']
            num_concepts = 0
            num_references = 0

            # Iterate through each DataElement and transform to an OCL-JSON concept
            for data_element in new_dhis2_export['dataElements']:
                sims_concept_id = data_element['code']
                sims_concept_url = '/orgs/PEPFAR/sources/SIMS/concepts/%s/' % sims_concept_id
                sims_concept_key = sims_concept_url
                sims_concept = {
                    'type': 'Concept',
                    'id': sims_concept_id,
                    'concept_class': 'Assessment Type',
                    'datatype': 'None',
                    'owner': 'PEPFAR',
                    'owner_type': self.RESOURCE_TYPE_ORGANIZATION,
                    'source': 'SIMS',
                    'retired': False,
                    'descriptions': None,
                    'external_id': data_element['id'],
                    'names': [
                        {
                            'name': data_element['name'],
                            'name_type': 'Fully Specified',
                            'locale': 'en',
                            'locale_preferred': True,
                            'external_id': None,
                        }
                    ],
                    'extras': {'Value Type': data_element['valueType']}
                }
                self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_SIMS][self.RESOURCE_TYPE_CONCEPT][
                    sims_concept_key] = sims_concept
                num_concepts += 1

                # Iterate through each DataElementGroup and transform to an OCL-JSON concept references
                for data_element_group in data_element['dataElementGroups']:
                    ocl_collection_id = ocl_dataset_repos[data_element_group['id']]['id']
                    sims_concept_ref_key, sims_concept_ref = self.get_concept_reference_json(
                        collection_owner_id='PEPFAR', collection_owner_type=self.RESOURCE_TYPE_ORGANIZATION,
                        collection_id=ocl_collection_id, concept_url=sims_concept_url)
                    self.dhis2_diff[datimconstants.DatimConstants.IMPORT_BATCH_SIMS][self.RESOURCE_TYPE_CONCEPT_REF][
                        sims_concept_ref_key] = sims_concept_ref
                    num_references += 1

            self.vlog(1, 'DHIS2 export "%s" successfully transformed to %s concepts + %s references (%s total)' % (
                dhis2filename_export_new, num_concepts, num_references, num_concepts + num_references))
            return True
