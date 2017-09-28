"""
DatimSync base class to be used by all DATIM sync scripts

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
from datimbase import DatimBase
import json


class DatimSync(DatimBase):

    OCL_EXPORT_DEFS = {}

    DEFAULT_OCL_EXPORT_CLEANING_METHOD = 'clean_ocl_export'

    # Default fields to strip from OCL exports before performing deep diffs
    DEFAULT_CONCEPT_FIELDS_TO_REMOVE = ['version_created_by', 'created_on', 'updated_on',
                                        'version_created_on', 'created_by', 'updated_by', 'display_name',
                                        'display_locale', 'uuid', 'version', 'owner_url', 'source_url',
                                        'mappings', 'url', 'version_url', 'is_latest_version', 'locale']
    DEFAULT_CONCEPT_NAME_FIELDS_TO_REMOVE = ['uuid', 'type']
    DEFAULT_MAPPING_FIELDS_TO_REMOVE = []

    def __init__(self):
        DatimBase.__init__(self)

        self.dhis2_diff = {}
        self.ocl_diff = {}
        self.ocl_collections = []
        self.str_dataset_ids = ''
        self.data_check_only = False

    def log_settings(self):
        """ Write settings to console """
        self.log(
            '**** Sync Script Settings:',
            'verbosity:', self.verbosity,
            ', dhis2env:', self.dhis2env,
            ', dhis2uid + dhis2pwd: <hidden>',
            ', oclenv:', self.oclenv,
            ', oclapitoken: <hidden>',
            ', compare2previousexport:', self.compare2previousexport)
        if self.runoffline:
            self.log('**** RUNNING IN OFFLINE MODE ****')

    def _convert_endpoint_to_filename_fmt(self, endpoint):
        filename = endpoint.replace('/', '-')
        if filename[0] == '-':
            filename = filename[1:]
        if filename[-1] == '-':
            filename = filename[:-1]
        return filename

    def endpoint2filename_ocl_export_tar(self, endpoint):
        return 'ocl-' + self._convert_endpoint_to_filename_fmt(endpoint) + '.tar'

    def endpoint2filename_ocl_export_json(self, endpoint):
        return 'ocl-' + self._convert_endpoint_to_filename_fmt(endpoint) + '-raw.json'

    def endpoint2filename_ocl_export_cleaned(self, endpoint):
        return 'ocl-' + self._convert_endpoint_to_filename_fmt(endpoint) + '-cleaned.json'

    def prepare_ocl_exports(self, cleaning_attr=None):
        """
        Convert OCL exports into the diff format
        :param cleaning_attr: Optional cleaning attributes that are made available to each cleaning method
        :return: None
        """
        for ocl_export_def_key, export_def in self.OCL_EXPORT_DEFS.iteritems():
            if self.verbosity:
                self.log('%s:' % ocl_export_def_key)
            cleaning_method_name = export_def.get('cleaning_method', self.DEFAULT_OCL_EXPORT_CLEANING_METHOD)
            getattr(self, cleaning_method_name)(export_def, cleaning_attr=cleaning_attr)
        with open(self.attach_absolute_path(self.OCL_CLEANED_EXPORT_FILENAME), 'wb') as output_file:
            output_file.write(json.dumps(self.ocl_diff))
            if self.verbosity:
                self.log('Cleaned OCL exports successfully written to "%s"' % (
                    self.OCL_CLEANED_EXPORT_FILENAME))

    def clean_ocl_export(self, ocl_export_def, cleaning_attr=None):
        """
        Default method for cleaning an OCL export to prepare it for a diff
        :param ocl_export_def:
        :param cleaning_attr:
        :return:
        """
        jsonfilename = self.endpoint2filename_ocl_export_json(ocl_export_def['endpoint'])
        with open(self.attach_absolute_path(jsonfilename), 'rb') as input_file:
            ocl_export_raw = json.load(input_file)

            if ocl_export_raw['type'] == 'Source':
                num_concepts = 0
                for c in ocl_export_raw['concepts']:
                    concept_key = c['url']
                    # Remove core fields not involved in the diff
                    for f in self.DEFAULT_CONCEPT_FIELDS_TO_REMOVE:
                        if f in c:
                            del c[f]
                    # Remove name fields
                    if 'names' in c:
                        for i, name in enumerate(c['names']):
                            for f in self.DEFAULT_CONCEPT_NAME_FIELDS_TO_REMOVE:
                                if f in name:
                                    del name[f]
                    self.ocl_diff[self.IMPORT_BATCH_MER][self.RESOURCE_TYPE_CONCEPT][concept_key] = c
                    num_concepts += 1
                self.log('Cleaned %s concepts' % num_concepts)

            elif ocl_export_raw['type'] == 'Collection':
                num_concept_refs = 0
                for r in ocl_export_raw['references']:
                    concept_ref_key = r['url']
                    self.ocl_diff[self.IMPORT_BATCH_MER][self.RESOURCE_TYPE_CONCEPT_REF][concept_ref_key] = r
                    num_concept_refs += 1
                self.log('Cleaned %s concept references' % num_concept_refs)

    def data_check(self):
        self.data_check_only = True
        return self.run()

    def run(self):
        """ Runs the entire synchronization process """
        if self.verbosity:
            self.log_settings()

        # STEP 1: Load OCL Collections for Dataset IDs
        if self.verbosity:
            self.log('**** STEP 1 of 12: Load OCL Collections for Dataset IDs')
        self.load_datasets_from_ocl()

        # STEP 2: Load new exports from DATIM-DHIS2
        if self.verbosity:
            self.log('**** STEP 2 of 12: Load new exports from DATIM DHIS2')
        for dhis2_query_key, dhis2_query_def in self.DHIS2_QUERIES.iteritems():
            if self.verbosity:
                self.log(dhis2_query_key + ':')
            if not self.runoffline:
                query_attr = {'active_dataset_ids': self.str_active_dataset_ids}
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
        if self.verbosity:
            self.log('**** STEP 3 of 12: Quick comparison of current and previous DHIS2 exports')
        complete_match = True
        if self.compare2previousexport and not self.data_check_only:
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
        elif self.data_check_only:
            if self.verbosity:
                self.log("Skipping: data check only...")
        else:
            if self.verbosity:
                self.log("Skipping: compare2previousexport == false")

        # STEP 4: Fetch latest versions of relevant OCL exports
        if self.verbosity:
            self.log('**** STEP 4 of 12: Fetch latest versions of relevant OCL exports')
        for ocl_export_def_key in self.OCL_EXPORT_DEFS:
            if self.verbosity:
                self.log('%s:' % ocl_export_def_key)
            export_def = self.OCL_EXPORT_DEFS[ocl_export_def_key]
            tarfilename = self.endpoint2filename_ocl_export_tar(export_def['endpoint'])
            jsonfilename = self.endpoint2filename_ocl_export_json(export_def['endpoint'])
            if not self.runoffline:
                self.get_ocl_export(
                    endpoint=export_def['endpoint'],
                    version='latest',
                    tarfilename=tarfilename,
                    jsonfilename=jsonfilename)
            else:
                if self.verbosity:
                    self.log('OFFLINE: Using local file "%s"...' % jsonfilename)
                if os.path.isfile(self.attach_absolute_path(jsonfilename)):
                    if self.verbosity:
                        self.log('OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                            jsonfilename, os.path.getsize(self.attach_absolute_path(jsonfilename))))
                else:
                    self.log('Could not find offline file "%s". Exiting...' % jsonfilename)
                    sys.exit(1)

        # STEP 5: Transform new DHIS2 export to diff format
        # Diff format is OCL-Formatted JSON for concepts and mappings, list of unique URLs for references
        if self.verbosity:
            self.log('**** STEP 5 of 12: Transform DHIS2 exports to OCL-formatted JSON')
        self.dhis2_diff = {}
        for import_batch_key in self.IMPORT_BATCHES:
            self.dhis2_diff[import_batch_key] = {
                self.RESOURCE_TYPE_CONCEPT: {},
                self.RESOURCE_TYPE_MAPPING: {},
                self.RESOURCE_TYPE_CONCEPT_REF: {},
                self.RESOURCE_TYPE_MAPPING_REF: {},
            }
        self.transform_dhis2_exports(conversion_attr={'ocl_dataset_repos': self.ocl_dataset_repos})

        exit()

        # STEP 6: Prepare OCL exports for diff
        # Concepts/mappings in OCL exports have extra attributes that should be removed before the diff
        if self.verbosity:
            self.log('**** STEP 6 of 12: Prepare OCL exports for diff')
        for import_batch_key in self.IMPORT_BATCHES:
            self.ocl_diff[import_batch_key] = {
                self.RESOURCE_TYPE_CONCEPT: {},
                self.RESOURCE_TYPE_MAPPING: {},
                self.RESOURCE_TYPE_CONCEPT_REF: {},
                self.RESOURCE_TYPE_MAPPING_REF: {},
            }
        self.prepare_ocl_exports(cleaning_attr={})

        # STEP 7: Perform deep diff
        # One deep diff is performed per resource type in each import batch
        # OCL/DHIS2 exports reloaded from file to eliminate unicode type_change diff -- but that may be short sighted!
        if self.verbosity:
            self.log('**** STEP 7 of 12: Perform deep diff')
        with open(self.attach_absolute_path(self.OCL_CLEANED_EXPORT_FILENAME), 'rb') as file_ocl_diff,\
                open(self.attach_absolute_path(self.DHIS2_CONVERTED_EXPORT_FILENAME), 'rb') as file_dhis2_diff:
            ocl_diff = json.load(file_ocl_diff)
            dhis2_diff = json.load(file_dhis2_diff)
            self.diff_result = self.perform_diff(ocl_diff=ocl_diff, dhis2_diff=dhis2_diff)

        # STEP 8: Determine action based on diff result
        if self.verbosity:
            self.log('**** STEP 8 of 12: Determine action based on diff result')
        if self.diff_result:
            self.log('One or more differences identified between DHIS2 and OCL...')
        else:
            self.log('No diff between DHIS2 and OCL...')
            return

        # STEP 9: Generate one OCL import script per import batch by processing the diff results
        # Note that OCL import scripts are JSON-lines files
        if self.verbosity:
            self.log('**** STEP 9 of 12: Generate import scripts')
        self.generate_import_scripts(self.diff_result)

        # STEP 10: Perform the import in OCL
        if self.verbosity:
            self.log('**** STEP 10 of 12: Perform the import in OCL')
        num_import_rows_processed = 0
        if self.data_check_only:
            self.log('Skipping: data check only...')
        else:
            ocl_importer = OclFlexImporter(
                file_path=self.attach_absolute_path(self.NEW_IMPORT_SCRIPT_FILENAME),
                api_token=self.oclapitoken, api_url_root=self.oclenv,test_mode=self.import_test_mode,
                do_update_if_exists=False, verbosity=self.verbosity, limit=self.import_limit)
            num_import_rows_processed = ocl_importer.process()
            if self.verbosity:
                self.log('Import records processed:', num_import_rows_processed)

        # STEP 11: Save new DHIS2 export for the next sync attempt
        if self.verbosity:
            self.log('**** STEP 11 of 12: Save the DHIS2 export')
        if self.data_check_only:
            self.log('Skipping: data check only...')
        else:
            if num_import_rows_processed and not self.import_test_mode:
                self.cache_dhis2_exports()
            else:
                if self.verbosity:
                    self.log('Skipping, because import failed or import test mode enabled...')

        # STEP 12: Manage OCL repository versions
        if self.verbosity:
            self.log('**** STEP 12 of 12: Manage OCL repository versions')
        if self.data_check_only:
            self.log('Skipping: data check only...')
        elif self.import_test_mode:
            if self.verbosity:
                self.log('Skipping, because import test mode enabled...')
        elif num_import_rows_processed:
            self.increment_ocl_versions(import_results=ocl_importer.results)
        else:
            if self.verbosity:
                self.log('Skipping because no records imported...')
