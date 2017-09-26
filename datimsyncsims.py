"""
Class to synchronize DATIM DHIS2 SIMS definitions with OCL
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

Import batches are imported in alphabetical order. Within a batch, resources are imported
in this order: concepts, mappings, concept references, mapping references.
Import batches should be structured as follows:
{
    1_ordered_import_batch: {
        'concepts': { concept_relative_url: { resource_field_1: value, ... },
        'mappings': { mapping_unique_url: { resource_field_1: value, ... },
        'references': { reference_unique_url: {resource_field_1: value, ...}
    },
    2_ordered_import_batch: { ... }
}
"""
from __future__ import with_statement
import os
import sys
import json
from oclfleximporter import OclFlexImporter
from datimbase import DatimBase


class DatimSyncSims(DatimBase):
    """ Class to manage DATIM SIMS Synchronization """

    # URLs
    url_sims_filtered_endpoint = '/orgs/PEPFAR/collections/?q=SIMS&verbose=true'

    # Filenames
    sims_collections_filename = 'ocl_sims_collections_export.json'
    new_import_script_filename = 'sims_dhis2ocl_import_script.json'
    dhis2_converted_export_filename = 'dhis2_sims_converted_export.json'
    ocl_cleaned_export_filename = 'ocl_sims_cleaned_export.json'

    # Import batches
    IMPORT_BATCH_SIMS = 'SIMS'
    IMPORT_BATCHES = [IMPORT_BATCH_SIMS]

    # DATIM DHIS2 Query Definitions
    DHIS2_QUERIES = {
        'SimsAssessmentTypes': {
            'name': 'DATIM-DHIS2 SIMS Assessment Types',
            'query': 'api/dataElements.json?fields=name,code,id,valueType,lastUpdated,dataElementGroups[id,name]&'
                     'order=code:asc&paging=false&filter=dataElementGroups.id:in:[{{active_dataset_ids}}]',
            'new_export_filename': 'new_dhis2_sims_export_raw.json',
            'old_export_filename': 'old_dhis2_sims_export_raw.json',
            'converted_export_filename': 'new_dhis2_sims_export_converted.json',
            'conversion_method': 'dhis2diff_sims_assessment_types'
        }
    }
    DHIS2_QUERIES_INACTIVE = {
        'SimsOptions': {
            'name': 'DATIM-DHIS2 SIMS Options',
            'query': '',
            'new_export_filename': 'new_dhis2_sims_options_export_raw.json',
            'old_export_filename': 'old_dhis2_sims_options_export_raw.json',
            'converted_export_filename': 'new_dhis2_sims_options_export_converted.json',
            'conversion_method': 'dhis2diff_sims_options'
        }
    }

    # OCL Export Definitions
    OCL_EXPORT_DEFS = {
        'sims_source': {
            'endpoint': '/orgs/PEPFAR/sources/SIMS/',
            'tarfilename': 'ocl_sims_source_export.tar',
            'jsonfilename': 'ocl_sims_source_export_raw.json',
            'jsoncleanfilename': 'ocl_sims_source_export_clean.json',
            'cleaning_method': 'clean_ocl_export_sims_source'
        },
        'sims2_above_site': {
            'endpoint': '/orgs/PEPFAR/collections/SIMS2-Above-Site/',
            'tarfilename': 'ocl_pepfar_sims2_above_site_export.tar',
            'jsonfilename': 'ocl_pepfar_sims2_above_site_export_raw.json',
            'jsoncleanfilename': 'ocl_pepfar_sims2_above_site_export_clean.json',
            'cleaning_method': 'clean_ocl_export_sims_collection'
        },
        'sims2_community': {
            'endpoint': '/orgs/PEPFAR/collections/SIMS2-Community/',
            'tarfilename': 'ocl_pepfar_sims2_community_export.tar',
            'jsonfilename': 'ocl_pepfar_sims2_community_export_raw.json',
            'jsoncleanfilename': 'ocl_pepfar_sims2_community_export_clean.json',
            'cleaning_method': 'clean_ocl_export_sims_collection'
        },
        'sims2_facility': {
            'endpoint': '/orgs/PEPFAR/collections/SIMS2-Facility/',
            'tarfilename': 'ocl_pepfar_sims2_facility_export.tar',
            'jsonfilename': 'ocl_pepfar_sims2_facility_export_raw.json',
            'jsoncleanfilename': 'ocl_pepfar_sims2_facility_export_clean.json',
            'cleaning_method': 'clean_ocl_export_sims_collection'
        },
        'sims3_above_site': {
            'endpoint': '/orgs/PEPFAR/collections/SIMS3-Above-Site/',
            'tarfilename': 'ocl_pepfar_sims3_above_site_export.tar',
            'jsonfilename': 'ocl_pepfar_sims3_above_site_export_raw.json',
            'jsoncleanfilename': 'ocl_pepfar_sims3_above_site_export_clean.json',
            'cleaning_method': 'clean_ocl_export_sims_collection'
        },
        'sims3_community': {
            'endpoint': '/orgs/PEPFAR/collections/SIMS3-Community/',
            'tarfilename': 'ocl_pepfar_sims3_community_export.tar',
            'jsonfilename': 'ocl_pepfar_sims3_community_export_raw.json',
            'jsoncleanfilename': 'ocl_pepfar_sims3_community_export_clean.json',
            'cleaning_method': 'clean_ocl_export_sims_collection'
        },
        'sims3_facility': {
            'endpoint': '/orgs/PEPFAR/collections/SIMS3-Facility/',
            'tarfilename': 'ocl_pepfar_sims3_facility_export.tar',
            'jsonfilename': 'ocl_pepfar_sims3_facility_export_raw.json',
            'jsoncleanfilename': 'ocl_pepfar_sims3_facility_export_clean.json',
            'cleaning_method': 'clean_ocl_export_sims_collection'
        },
    }

    # Some other attributes that need to be modeled better!
    SIMS_CONCEPT_FIELDS_TO_REMOVE = ['version_created_by', 'created_on', 'updated_on',
                                     'version_created_on', 'created_by', 'updated_by', 'display_name',
                                     'display_locale', 'uuid', 'version', 'owner_url', 'source_url',
                                     'mappings', 'url', 'version_url', 'is_latest_version', 'locale']
    SIMS_MAPPING_FIELDS_TO_REMOVE = []
    SIMS_NAME_FIELDS_TO_REMOVE = ['uuid', 'type']

    def __init__(self, oclenv='', oclapitoken='',
                 dhis2env='', dhis2uid='', dhis2pwd='',
                 compare2previousexport=True,
                 runoffline=False, verbosity=0,
                 import_test_mode=False, import_limit=0):
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
        self.dhis2_diff = {}
        self.ocl_diff = {}
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
        with open(self.attach_absolute_path(dhis2_query_def['new_export_filename']), "rb") as ifile:
            new_sims = json.load(ifile)
            sims_collections = conversion_attr['sims_collections']
            num_concepts = 0
            num_references = 0

            # Iterate through each DataElement and transform to an OCL-JSON concept
            for de in new_sims['dataElements']:
                concept_id = de['code']
                concept_key = '/orgs/PEPFAR/sources/SIMS/concepts/' + concept_id + '/'
                c = {
                    'type': 'Concept',
                    'id': concept_id,
                    'concept_class': 'Assessment Type',
                    'datatype': 'None',
                    'owner': 'PEPFAR',
                    'owner_type': 'Organization',
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
                    collection_id = sims_collections[deg['id']]['id']
                    concept_url = '/orgs/PEPFAR/sources/SIMS/concepts/' + concept_id + '/'
                    concept_ref_key = ('/orgs/PEPFAR/collections/' + collection_id +
                                       '/references/?concept=' + concept_url)
                    r = {
                        'type': 'Reference',
                        'owner': 'PEPFAR',
                        'owner_type': 'Organization',
                        'collection': collection_id,
                        'data': {"expressions": [concept_url]}
                    }
                    self.dhis2_diff[self.IMPORT_BATCH_SIMS][self.RESOURCE_TYPE_CONCEPT_REF][concept_ref_key] = r
                    num_references += 1

            if self.verbosity:
                self.log('DHIS2 export "%s" successfully transformed to %s concepts + %s references (%s total)' % (
                    dhis2_query_def['new_export_filename'], num_concepts,
                    num_references, num_concepts + num_references))
            return True

    def clean_ocl_export_sims_source(self, ocl_export_def, cleaning_attr=None):
        """
        Clean the SIMS Source export from OCL to prepare it for a diff
        :param ocl_export_def:
        :param cleaning_attr:
        :return:
        """
        with open(self.attach_absolute_path(ocl_export_def['jsonfilename']), 'rb') as input_file:
            ocl_export_raw = json.load(input_file)
            num_concepts = 0
            for c in ocl_export_raw['concepts']:
                concept_key = c['url']
                # Remove core fields not involved in the diff
                for f in self.SIMS_CONCEPT_FIELDS_TO_REMOVE:
                    if f in c:
                        del c[f]
                # Remove name fields
                if 'names' in c:
                    for i, name in enumerate(c['names']):
                        for f in self.SIMS_NAME_FIELDS_TO_REMOVE:
                            if f in name:
                                del name[f]
                self.ocl_diff[self.IMPORT_BATCH_SIMS][self.RESOURCE_TYPE_CONCEPT][concept_key] = c
                num_concepts += 1
            self.log('Cleaned %s concepts' % num_concepts)

    def clean_ocl_export_sims_collection(self, ocl_export_def, cleaning_attr=None):
        """
        Cleans the OCL SIMS Collections to prepare for the diff
        :param ocl_export_def:
        :param cleaning_attr:
        :return:
        """
        with open(self.attach_absolute_path(ocl_export_def['jsonfilename']), 'rb') as input_file:
            ocl_export_raw = json.load(input_file)
            num_concept_refs = 0
            for r in ocl_export_raw['references']:
                concept_ref_key = r['url']
                self.ocl_diff[self.IMPORT_BATCH_SIMS][self.RESOURCE_TYPE_CONCEPT_REF][concept_ref_key] = r
                num_concept_refs += 1
            self.log('Cleaned %s concept references' % num_concept_refs)

    def log_settings(self):
        """ Write settings to console """
        self.log(
            '**** SIMS Sync Script Settings:',
            'verbosity:', self.verbosity,
            ', dhis2env:', self.dhis2env,
            ', dhis2uid + dhis2pwd: <hidden>',
            ', oclenv:', self.oclenv,
            ', oclapitoken: <hidden>',
            ', compare2previousexport:', self.compare2previousexport)
        if self.runoffline:
            self.log('**** RUNNING IN OFFLINE MODE ****')

    def run(self):
        """ Runs the entire synchronization process """
        if self.verbosity:
            self.log_settings()

        # STEP 1: Fetch OCL Collections for SIMS Assessment Types
        # Collections that have 'SIMS' in the name, __datim_sync==true, and external_id not empty
        if self.verbosity:
            self.log('**** STEP 1 of 12: Fetch OCL Collections for SIMS Assessment Types')
        if not self.runoffline:
            url_sims_collections = self.oclenv + self.url_sims_filtered_endpoint
            if self.verbosity:
                self.log('Request URL:', url_sims_collections)
            sims_collections = self.get_ocl_repositories(url=url_sims_collections, key_field='external_id')
            with open(self.attach_absolute_path(self.sims_collections_filename), 'wb') as output_file:
                output_file.write(json.dumps(sims_collections))
            if self.verbosity:
                self.log('Repositories retrieved from OCL and stored in memory:', len(sims_collections))
                self.log('Repositories successfully written to "%s"' % self.sims_collections_filename)
        else:
            if self.verbosity:
                self.log('OFFLINE: Loading repositories from "%s"' % self.sims_collections_filename)
            with open(self.attach_absolute_path(self.sims_collections_filename), 'rb') as handle:
                sims_collections = json.load(handle)
            if self.verbosity:
                self.log('OFFLINE: Repositories successfully loaded:', len(sims_collections))
        # Extract list of DHIS2 dataset IDs from collection external_id
        if sims_collections:
            str_active_dataset_ids = ','.join(sims_collections.keys())
            if self.verbosity:
                self.log('SIMS Assessment Type Dataset IDs:', str_active_dataset_ids)
        else:
            if self.verbosity:
                self.log('No collections returned. Exiting...')
            sys.exit(1)

        # STEP 2: Fetch new exports from DATIM-DHIS2
        if self.verbosity:
            self.log('**** STEP 2 of 12: Fetch new exports from DATIM DHIS2')
        for dhis2_query_key, dhis2_query_def in self.DHIS2_QUERIES.iteritems():
            if self.verbosity:
                self.log(dhis2_query_key + ':')
            if not self.runoffline:
                query_attr = {'active_dataset_ids': str_active_dataset_ids}
                content_length = self.save_dhis2_query_to_file(
                    query=dhis2_query_def['query'], query_attr=query_attr,
                    outputfilename=dhis2_query_def['new_export_filename'])
                if self.verbosity:
                    self.log('%s bytes retrieved from DHIS2 and written to file "%s"' % (
                        content_length, dhis2_query_def['new_export_filename']))
            else:
                if self.verbosity:
                    self.log('OFFLINE: Using local file: "%s"' % (dhis2_query_def['new_export_filename']))
                if os.path.isfile(self.attach_absolute_path(dhis2_query_def['new_export_filename'])):
                    if self.verbosity:
                        self.log('OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                            dhis2_query_def['new_export_filename'],
                            os.path.getsize(self.attach_absolute_path(dhis2_query_def['new_export_filename']))))
                else:
                    self.log('Could not find offline file "%s". Exiting...' % (dhis2_query_def['new_export_filename']))
                    sys.exit(1)

        # STEP 3: Quick comparison of current and previous DHIS2 exports
        # Compares new DHIS2 export to most recent previous export from a successful sync that is available
        # NOTE: This section should be skipped if doing the OCL/DHIS2 data validation check
        if self.verbosity:
            self.log('**** STEP 3 of 12: Quick comparison of current and previous DHIS2 exports')
        complete_match = True
        if self.compare2previousexport:
            # Compare files for each of the DHIS2 queries
            for dhis2_query_key, dhis2_query_def in self.DHIS2_QUERIES.iteritems():
                if self.verbosity:
                    self.log(dhis2_query_key + ':')
                if self.filecmp(self.attach_absolute_path(dhis2_query_def['old_export_filename']),
                                self.attach_absolute_path(dhis2_query_def['new_export_filename'])):
                    if self.verbosity:
                        self.log('"%s" and "%s" are identical' % (
                            dhis2_query_def['old_export_filename'], dhis2_query_def['new_export_filename']))
                else:
                    complete_match = True
                    if self.verbosity:
                        self.log('"%s" and "%s" are NOT identical' % (
                            dhis2_query_def['old_export_filename'], dhis2_query_def['new_export_filename']))

            # Exit if complete match, because there is no import to perform
            if complete_match:
                if self.verbosity:
                    self.log('All old and new DHIS2 exports are identical so there is no import to perform. Exiting...')
                sys.exit()
            else:
                if self.verbosity:
                    self.log('At least one DHIS2 export does not match, so continue...')
        else:
            if self.verbosity:
                self.log("Skipping (due to settings)...")

        # STEP 4: Fetch latest versions of relevant OCL exports
        if self.verbosity:
            self.log('**** STEP 4 of 12: Fetch latest versions of relevant OCL exports')
        for ocl_export_def_key in self.OCL_EXPORT_DEFS:
            if self.verbosity:
                self.log('%s:' % ocl_export_def_key)
            export_def = self.OCL_EXPORT_DEFS[ocl_export_def_key]
            if not self.runoffline:
                self.get_ocl_export(
                    endpoint=export_def['endpoint'], version='latest',
                    tarfilename=export_def['tarfilename'], jsonfilename=export_def['jsonfilename'])
            else:
                if self.verbosity:
                    self.log('OFFLINE: Using local file "%s"...' % (export_def['jsonfilename']))
                if os.path.isfile(self.attach_absolute_path(export_def['jsonfilename'])):
                    if self.verbosity:
                        self.log('OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                            export_def['jsonfilename'], os.path.getsize(
                                self.attach_absolute_path(export_def['jsonfilename']))))
                else:
                    self.log('Could not find offline file "%s". Exiting...' % (export_def['jsonfilename']))
                    sys.exit(1)

        # STEP 5: Transform new DHIS2 export to diff format
        # Diff format is OCL-Formatted JSON for concepts and mappings, list of unique URLs for references
        if self.verbosity:
            self.log('**** STEP 5 of 12: Transform DHIS2 exports to OCL-formatted JSON')
        self.dhis2_diff = {self.IMPORT_BATCH_SIMS: {self.RESOURCE_TYPE_CONCEPT: {}, self.RESOURCE_TYPE_CONCEPT_REF: {}}}
        self.transform_dhis2_exports(conversion_attr={'sims_collections': sims_collections})

        # STEP 6: Prepare OCL exports for diff
        # Concepts/mappings in OCL exports have extra attributes that should be removed before the diff
        if self.verbosity:
            self.log('**** STEP 6 of 12: Prepare OCL exports for diff')
        self.ocl_diff = {self.IMPORT_BATCH_SIMS: {self.RESOURCE_TYPE_CONCEPT: {}, self.RESOURCE_TYPE_CONCEPT_REF: {}}}
        self.prepare_ocl_exports(cleaning_attr={})

        # STEP 7: Perform deep diff
        # One deep diff is performed per resource type in each import batch
        # OCL/DHIS2 exports reloaded from file to eliminate unicode type_change diff -- but that may be short sighted!
        if self.verbosity:
            self.log('**** STEP 7 of 12: Perform deep diff')
        with open(self.attach_absolute_path(self.ocl_cleaned_export_filename), 'rb') as file_sims_ocl,\
                open(self.attach_absolute_path(self.dhis2_converted_export_filename), 'rb') as file_sims_dhis2:
            sims_ocl = json.load(file_sims_ocl)
            sims_dhis2 = json.load(file_sims_dhis2)
            diff = self.perform_diff(ocl_diff=sims_ocl, dhis2_diff=sims_dhis2)

        # STEP 8: Determine action based on diff result
        if self.verbosity:
            self.log('**** STEP 8 of 12: Determine action based on diff result')
        if diff:
            self.log('One or more differences identified between DHIS2 and OCL...')
        else:
            self.log('No diff, exiting...')
            exit()

        # STEP 9: Generate one OCL import script per import batch by processing the diff results
        # Note that OCL import scripts are JSON-lines files
        if self.verbosity:
            self.log('**** STEP 9 of 12: Generate import scripts')
        self.generate_import_scripts(diff)

        # STEP 10: Perform the import in OCL
        if self.verbosity:
            self.log('**** STEP 10 of 12: Perform the import in OCL')
        ocl_importer = OclFlexImporter(
            file_path=self.attach_absolute_path(self.new_import_script_filename),
            api_token=self.oclapitoken, api_url_root=self.oclenv, test_mode=self.import_test_mode,
            do_update_if_exists=False, verbosity=self.verbosity, limit=self.import_limit)
        import_result = ocl_importer.process()
        if self.verbosity:
            self.log('Import records processed:', import_result)

        # STEP 11: Save new DHIS2 export for the next sync attempt
        if self.verbosity:
            self.log('**** STEP 11 of 12: Save the DHIS2 export')
        if import_result and not self.import_test_mode:
            self.cache_dhis2_exports()
        else:
            if self.verbosity:
                self.log('Skipping, because import failed or import test mode enabled...')

        # STEP 12: Manage OCL repository versions
        if self.verbosity:
            self.log('**** STEP 12 of 12: Manage OCL repository versions')
        if self.import_test_mode:
            if self.verbosity:
                self.log('Skipping, because import test mode enabled...')
        elif import_result:
            self.increment_ocl_versions()
        else:
            if self.verbosity:
                self.log('Skipping because no records imported...')


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
    import_limit = 1
    import_test_mode = False
    compare2previousexport = False
    runoffline = False
    dhis2env = 'https://dev-de.datim.org/'
    dhis2uid = 'jpayne'
    dhis2pwd = 'Johnpayne1!'
    oclenv = 'https://api.showcase.openconceptlab.org'
    oclapitoken = '2da0f46b7d29aa57970c0b3a535121e8e479f881'
    # oclapitoken = 'a61ba53ed7b8b26ece8fcfc53022b645de0ec055'
    # oclenv = 'https://ocl-stg.openmrs.org'

# Create SIMS sync object and run
sims_sync = DatimSyncSims(oclenv=oclenv, oclapitoken=oclapitoken,
                          dhis2env=dhis2env, dhis2uid=dhis2uid, dhis2pwd=dhis2pwd,
                          compare2previousexport=compare2previousexport,
                          runoffline=runoffline, verbosity=verbosity,
                          import_test_mode=import_test_mode,
                          import_limit=import_limit)
sims_sync.run()
