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
import json
import requests
import os
import sys
from requests.auth import HTTPBasicAuth
from shutil import copyfile
from datimbase import DatimBase
from oclfleximporter import OclFlexImporter
from pprint import pprint
from deepdiff import DeepDiff


class DatimSync(DatimBase):

    # Data check return values
    DATIM_SYNC_NO_DIFF = 0
    DATIM_SYNC_DIFF = 1

    OCL_EXPORT_DEFS = {}
    DHIS2_QUERIES = {}
    IMPORT_BATCHES = []

    DEFAULT_OCL_EXPORT_CLEANING_METHOD = 'clean_ocl_export'

    # Default fields to strip from OCL exports before performing deep diffs
    DEFAULT_CONCEPT_FIELDS_TO_REMOVE = ['version_created_by', 'created_on', 'updated_on',
                                        'version_created_on', 'created_by', 'updated_by', 'display_name',
                                        'display_locale', 'uuid', 'version', 'owner_url', 'source_url',
                                        'mappings', 'url', 'version_url', 'is_latest_version', 'locale']
    DEFAULT_CONCEPT_NAME_FIELDS_TO_REMOVE = ['uuid', 'type']
    DEFAULT_CONCEPT_DESC_FIELDS_TO_REMOVE = ['uuid', 'type']
    DEFAULT_MAPPING_ALLOWED_FIELDS = ['external_id', 'extras', 'id', 'from_source_url', 'map_type',
                                      'owner', 'owner_type', 'retired', 'source', 'to_concept_code',
                                      'to_source_url', 'to_concept_url', 'type', 'url']
    DEFAULT_MAPPING_FIELDS_TO_REMOVE = ['created_at', 'created_by', 'from_concept_code', 'from_concept_name',
                                        'from_source_name', 'from_source_owner', 'from_source_owner_type',
                                        'from_source_url', 'is_direct_mapping', 'is_external_mapping',
                                        'is_internal_mapping', 'is_inverse_mapping', 'updated_at', 'updated_by',
                                        'to_concept_name', 'to_source_name', 'to_source_owner',
                                        'to_source_owner_type']

    def __init__(self):
        DatimBase.__init__(self)

        self.dhis2_diff = {}
        self.ocl_diff = {}
        self.ocl_collections = []
        self.str_dataset_ids = ''
        self.data_check_only = False
        self.runoffline = False
        self.compare2previousexport = True
        self.import_test_mode = False
        self.import_limit = 0
        self.diff_result = None

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

    def dhis2filename_export_new(self, dhis2_query_id):
        return 'dhis2-' + dhis2_query_id + '-export-new-raw.json'

    def dhis2filename_export_old(self, dhis2_query_id):
        return 'dhis2-' + dhis2_query_id + '-export-old-raw.json'

    def dhis2filename_export_converted(self, dhis2_query_id):
        return 'dhis2-' + dhis2_query_id + '-export-converted.json'

    def prepare_ocl_exports(self, cleaning_attr=None):
        """
        Convert OCL exports into the diff format
        :param cleaning_attr: Optional cleaning attributes that are made available to each cleaning method
        :return: None
        """
        for ocl_export_def_key, export_def in self.OCL_EXPORT_DEFS.iteritems():
            self.vlog(1, '** %s:' % ocl_export_def_key)
            cleaning_method_name = export_def.get('cleaning_method', self.DEFAULT_OCL_EXPORT_CLEANING_METHOD)
            getattr(self, cleaning_method_name)(export_def, cleaning_attr=cleaning_attr)
        with open(self.attach_absolute_path(self.OCL_CLEANED_EXPORT_FILENAME), 'wb') as output_file:
            output_file.write(json.dumps(self.ocl_diff))
            self.vlog(1, 'Cleaned OCL exports successfully written to "%s"' % (
                self.OCL_CLEANED_EXPORT_FILENAME))

    def clean_ocl_export(self, ocl_export_def, cleaning_attr=None):
        """
        Default method for cleaning an OCL export to prepare it for a diff
        :param ocl_export_def:
        :param cleaning_attr:
        :return:
        """
        import_batch_key = ocl_export_def['import_batch']
        jsonfilename = self.endpoint2filename_ocl_export_json(ocl_export_def['endpoint'])
        with open(self.attach_absolute_path(jsonfilename), 'rb') as input_file:
            ocl_export_raw = json.load(input_file)

            if ocl_export_raw['type'] in ['Source', 'Source Version']:
                num_concepts = 0
                for c in ocl_export_raw['concepts']:
                    concept_key = c['url']
                    # Remove core fields not involved in the diff
                    for f in self.DEFAULT_CONCEPT_FIELDS_TO_REMOVE:
                        if f in c:
                            del c[f]
                    # Remove name fields
                    if 'names' in c and type(c['names']) is list:
                        for i, name in enumerate(c['names']):
                            for f in self.DEFAULT_CONCEPT_NAME_FIELDS_TO_REMOVE:
                                if f in name:
                                    del name[f]
                    # Remove description fields
                    if 'descriptions' in c and type(c['descriptions']) is list:
                        for i, description in enumerate(c['descriptions']):
                            for f in self.DEFAULT_CONCEPT_DESC_FIELDS_TO_REMOVE:
                                if f in description:
                                    del description[f]
                    self.ocl_diff[import_batch_key][self.RESOURCE_TYPE_CONCEPT][concept_key] = c
                    num_concepts += 1
                self.vlog(1, 'Cleaned %s concepts' % num_concepts)

            elif ocl_export_raw['type'] in ['Collection', 'Collection Version']:
                num_concept_refs = 0
                for r in ocl_export_raw['references']:
                    concept_ref_key = r['url']
                    self.ocl_diff[import_batch_key][self.RESOURCE_TYPE_CONCEPT_REF][concept_ref_key] = r
                    num_concept_refs += 1
                self.vlog(1, 'Cleaned %s concept references' % num_concept_refs)

    def cache_dhis2_exports(self):
        """
        Delete old DHIS2 cached files if there
        :return: None
        """
        for dhis2_query_key in self.DHIS2_QUERIES:
            # Delete old file if it exists
            dhis2filename_export_new = self.dhis2filename_export_new(self.DHIS2_QUERIES[dhis2_query_key]['id'])
            dhis2filename_export_old = self.dhis2filename_export_old(self.DHIS2_QUERIES[dhis2_query_key]['id'])
            if os.path.isfile(self.attach_absolute_path(dhis2filename_export_old)):
                os.remove(self.attach_absolute_path(dhis2filename_export_old))
            copyfile(self.attach_absolute_path(dhis2filename_export_new),
                     self.attach_absolute_path(dhis2filename_export_old))
            self.vlog(1, 'DHIS2 export successfully copied to "%s"' % dhis2filename_export_old)

    def transform_dhis2_exports(self, conversion_attr=None):
        """
        Transforms DHIS2 exports into the diff format
        :param conversion_attr: Optional conversion attributes that are made available to each conversion method
        :return: None
        """
        for dhis2_query_key, dhis2_query_def in self.DHIS2_QUERIES.iteritems():
            self.vlog(1, '** %s:' % dhis2_query_key)
            getattr(self, dhis2_query_def['conversion_method'])(dhis2_query_def, conversion_attr=conversion_attr)
        with open(self.attach_absolute_path(self.DHIS2_CONVERTED_EXPORT_FILENAME), 'wb') as output_file:
            output_file.write(json.dumps(self.dhis2_diff))
            self.vlog(1, 'Transformed DHIS2 exports successfully written to "%s"' % (
                self.DHIS2_CONVERTED_EXPORT_FILENAME))

    def save_dhis2_query_to_file(self, query='', query_attr=None, outputfilename=''):
        """ Execute DHIS2 query and save to file """

        # Replace query attribute names with values and build the query URL
        if query_attr:
            for attr_name in query_attr:
                query = query.replace('{{'+attr_name+'}}', query_attr[attr_name])
        url_dhis2_query = self.dhis2env + query

        # Execute the query
        self.vlog(1, 'Request URL:', url_dhis2_query)
        r = requests.get(url_dhis2_query, auth=HTTPBasicAuth(self.dhis2uid, self.dhis2pwd))
        r.raise_for_status()
        with open(self.attach_absolute_path(outputfilename), 'wb') as handle:
            for block in r.iter_content(1024):
                handle.write(block)
        return r.headers['Content-Length']

    def perform_diff(self, ocl_diff=None, dhis2_diff=None):
        """
        Performs deep diff on the prepared OCL and DHIS2 resources
        :param ocl_diff:
        :param dhis2_diff:
        :return:
        """
        diff = {}
        for import_batch_key in self.IMPORT_BATCHES:
            diff[import_batch_key] = {}
            for resource_type in self.RESOURCE_TYPES:
                if resource_type in ocl_diff[import_batch_key] and resource_type in dhis2_diff[import_batch_key]:
                    diff[import_batch_key][resource_type] = DeepDiff(
                        ocl_diff[import_batch_key][resource_type],
                        dhis2_diff[import_batch_key][resource_type],
                        ignore_order=True, verbose_level=2)
                    if self.verbosity:
                        str_log = 'IMPORT_BATCH["%s"]["%s"]: ' % (import_batch_key, resource_type)
                        for k in diff[import_batch_key][resource_type]:
                            str_log += '%s: %s; ' % (k, len(diff[import_batch_key][resource_type][k]))
                        self.log(str_log)
        return diff

    def generate_import_scripts(self, diff):
        """
        Generate import scripts
        :param diff:
        :return:
        """
        with open(self.attach_absolute_path(self.NEW_IMPORT_SCRIPT_FILENAME), 'wb') as output_file:
            for import_batch in self.IMPORT_BATCHES:
                for resource_type in self.RESOURCE_TYPES:
                    if resource_type not in diff[import_batch]:
                        continue

                    # Process new items
                    if 'dictionary_item_added' in diff[import_batch][resource_type]:
                        for k, r in diff[import_batch][resource_type]['dictionary_item_added'].iteritems():
                            if resource_type == self.RESOURCE_TYPE_CONCEPT and r['type'] == self.RESOURCE_TYPE_CONCEPT:
                                output_file.write(json.dumps(r))
                                output_file.write('\n')
                            elif resource_type == self.RESOURCE_TYPE_MAPPING and r['type'] == self.RESOURCE_TYPE_MAPPING:
                                output_file.write(json.dumps(r))
                                output_file.write('\n')
                            elif resource_type == self.RESOURCE_TYPE_CONCEPT_REF and r['type'] == self.RESOURCE_TYPE_REFERENCE:
                                output_file.write(json.dumps(r))
                                output_file.write('\n')
                            elif resource_type == self.RESOURCE_TYPE_MAPPING_REF and r['type'] == self.RESOURCE_TYPE_REFERENCE:
                                output_file.write(json.dumps(r))
                                output_file.write('\n')
                            else:
                                self.log('ERROR: Unrecognized resource_type "%s": {%s}' % (resource_type, str(r)))
                                sys.exit(1)

                    # Process updated items
                    if 'value_changed' in diff[import_batch][resource_type]:
                        self.vlog(1, 'WARNING: Updates are not yet supported. Skipping %s updates...' % len(
                            diff[import_batch][resource_type]))

                    # Process deleted items
                    if 'dictionary_item_removed' in diff[import_batch][resource_type]:
                        self.vlog(
                            1, 'WARNING: Retiring and deletes are not yet supported. Skipping %s removals...' % len(
                                diff[import_batch][resource_type]))

        self.vlog(1, 'New import script written to file "%s"' % self.NEW_IMPORT_SCRIPT_FILENAME)

    def get_concept_reference_json(self, owner_id='', owner_type='',
                                   collection_id='', concept_url=''):
        """ Returns an "importable" python dictionary for an OCL Reference with the specified attributes """
        if not owner_type:
            owner_type = self.RESOURCE_TYPE_ORGANIZATION
        if owner_type == self.RESOURCE_TYPE_USER:
            owner_stem = 'users'
        elif owner_type == self.RESOURCE_TYPE_ORGANIZATION:
            owner_stem = 'orgs'
        else:
            self.log('ERROR: Invalid owner_type "%s"' % owner_type)
            sys.exit(1)
        reference_key = '/%s/%s/collections/%s/references/?concept=%s' % (
            owner_stem, owner_id, collection_id, concept_url)
        reference_json = {
            'type': 'Reference',
            'owner': owner_id,
            'owner_type': owner_type,
            'collection': collection_id,
            'data': {"expressions": [concept_url]}
        }
        return reference_key, reference_json

    def data_check(self):
        self.data_check_only = True
        return self.run()

    def run(self):
        """ Runs the entire synchronization process """
        if self.verbosity:
            self.log_settings()

        # STEP 1: Load OCL Collections for Dataset IDs
        self.vlog(1, '**** STEP 1 of 12: Load OCL Collections for Dataset IDs')
        self.load_datasets_from_ocl()

        # STEP 2: Load new exports from DATIM-DHIS2
        self.vlog(1, '**** STEP 2 of 12: Load new exports from DATIM DHIS2')
        for dhis2_query_key, dhis2_query_def in self.DHIS2_QUERIES.iteritems():
            self.vlog(1, '** %s:' % dhis2_query_key)
            dhis2filename_export_new = self.dhis2filename_export_new(dhis2_query_def['id'])
            if not self.runoffline:
                query_attr = {'active_dataset_ids': self.str_active_dataset_ids}
                content_length = self.save_dhis2_query_to_file(
                    query=dhis2_query_def['query'], query_attr=query_attr,
                    outputfilename=dhis2filename_export_new)
                self.vlog(1, '%s bytes retrieved from DHIS2 and written to file "%s"' % (
                    content_length, dhis2filename_export_new))
            else:
                self.vlog(1, 'OFFLINE: Using local file: "%s"' % dhis2filename_export_new)
                if os.path.isfile(self.attach_absolute_path(dhis2filename_export_new)):
                    self.vlog(1, 'OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                        dhis2filename_export_new,
                        os.path.getsize(self.attach_absolute_path(dhis2filename_export_new))))
                else:
                    self.log('ERROR: Could not find offline file "%s". Exiting...' % dhis2filename_export_new)
                    sys.exit(1)

        # STEP 3: Quick comparison of current and previous DHIS2 exports
        # Compares new DHIS2 export to most recent previous export from a successful sync that is available
        self.vlog(1, '**** STEP 3 of 12: Quick comparison of current and previous DHIS2 exports')
        complete_match = True
        if self.compare2previousexport and not self.data_check_only:
            # Compare files for each of the DHIS2 queries
            for dhis2_query_key, dhis2_query_def in self.DHIS2_QUERIES.iteritems():
                self.vlog(1, dhis2_query_key + ':')
                dhis2filename_export_new = self.dhis2filename_export_new(dhis2_query_def['id'])
                dhis2filename_export_old = self.dhis2filename_export_old(dhis2_query_def['id'])
                if self.filecmp(self.attach_absolute_path(dhis2filename_export_old),
                                self.attach_absolute_path(dhis2filename_export_new)):
                    self.vlog(1, '"%s" and "%s" are identical' % (
                        dhis2filename_export_old, dhis2filename_export_new))
                else:
                    complete_match = True
                    self.vlog(1, '"%s" and "%s" are NOT identical' % (
                        dhis2filename_export_old, dhis2filename_export_new))

            # Exit if complete match, because there is no import to perform
            if complete_match:
                self.vlog(1, 'All old and new DHIS2 exports are identical so there is no import to perform. Exiting...')
                sys.exit()
            else:
                self.vlog(1, 'At least one DHIS2 export does not match, so continue...')
        elif self.data_check_only:
            self.vlog(1, "SKIPPING: Data check only...")
        else:
            self.vlog(1, "SKIPPING: compare2previousexport == false")

        # STEP 4: Fetch latest versions of relevant OCL exports
        self.vlog(1, '**** STEP 4 of 12: Fetch latest versions of relevant OCL exports')
        for ocl_export_def_key in self.OCL_EXPORT_DEFS:
            self.vlog(1, '** %s:' % ocl_export_def_key)
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
                self.vlog(1, 'OFFLINE: Using local file "%s"...' % jsonfilename)
                if os.path.isfile(self.attach_absolute_path(jsonfilename)):
                    self.vlog(1, 'OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                        jsonfilename, os.path.getsize(self.attach_absolute_path(jsonfilename))))
                else:
                    self.log('ERROR: Could not find offline file "%s". Exiting...' % jsonfilename)
                    sys.exit(1)

        # STEP 5: Transform new DHIS2 export to diff format
        # Diff format is OCL-Formatted JSON for concepts and mappings, list of unique URLs for references
        self.vlog(1, '**** STEP 5 of 12: Transform DHIS2 exports to OCL-formatted JSON')
        self.dhis2_diff = {}
        for import_batch_key in self.IMPORT_BATCHES:
            self.dhis2_diff[import_batch_key] = {
                self.RESOURCE_TYPE_CONCEPT: {},
                self.RESOURCE_TYPE_MAPPING: {},
                self.RESOURCE_TYPE_CONCEPT_REF: {},
                self.RESOURCE_TYPE_MAPPING_REF: {},
            }
        self.transform_dhis2_exports(conversion_attr={'ocl_dataset_repos': self.ocl_dataset_repos})

        # STEP 6: Prepare OCL exports for diff
        # Concepts/mappings in OCL exports have extra attributes that should be removed before the diff
        self.vlog(1, '**** STEP 6 of 12: Prepare OCL exports for diff')
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
        self.vlog(1, '**** STEP 7 of 12: Perform deep diff')
        with open(self.attach_absolute_path(self.OCL_CLEANED_EXPORT_FILENAME), 'rb') as file_ocl_diff,\
                open(self.attach_absolute_path(self.DHIS2_CONVERTED_EXPORT_FILENAME), 'rb') as file_dhis2_diff:
            ocl_diff = json.load(file_ocl_diff)
            dhis2_diff = json.load(file_dhis2_diff)
            self.diff_result = self.perform_diff(ocl_diff=ocl_diff, dhis2_diff=dhis2_diff)

        # STEP 8: Determine action based on diff result
        self.vlog(1, '**** STEP 8 of 12: Determine action based on diff result')
        if self.diff_result:
            self.vlog(1, 'One or more differences identified between DHIS2 and OCL...')
            if self.data_check_only:
                return self.DATIM_SYNC_DIFF
        else:
            self.vlog(1, 'No diff between DHIS2 and OCL...')
            return self.DATIM_SYNC_NO_DIFF

        # STEP 9: Generate one OCL import script per import batch by processing the diff results
        # Note that OCL import scripts are JSON-lines files
        self.vlog(1, '**** STEP 9 of 12: Generate import scripts')
        self.generate_import_scripts(self.diff_result)

        # STEP 10: Perform the import in OCL
        self.vlog(1, '**** STEP 10 of 12: Perform the import in OCL')
        num_import_rows_processed = 0
        ocl_importer = None
        if self.data_check_only:
            self.vlog(1, 'SKIPPING: Data check only...')
        else:
            ocl_importer = OclFlexImporter(
                file_path=self.attach_absolute_path(self.NEW_IMPORT_SCRIPT_FILENAME),
                api_token=self.oclapitoken, api_url_root=self.oclenv, test_mode=self.import_test_mode,
                do_update_if_exists=False, verbosity=self.verbosity, limit=self.import_limit)
            num_import_rows_processed = ocl_importer.process()
            self.vlog(1, 'Import records processed:', num_import_rows_processed)

        # STEP 11: Save new DHIS2 export for the next sync attempt
        self.vlog(1, '**** STEP 11 of 12: Save the DHIS2 export')
        if self.data_check_only:
            self.vlog(1, 'SKIPPING: Data check only...')
        elif self.import_test_mode:
            self.vlog(1, 'SKIPPING: Import test mode enabled...')
        elif num_import_rows_processed:
            self.cache_dhis2_exports()
        else:
            self.vlog(1, 'SKIPPING: No records imported (possibly due to error)...')

        # STEP 12: Manage OCL repository versions
        self.vlog(1, '**** STEP 12 of 12: Manage OCL repository versions')
        if self.data_check_only:
            self.vlog(1, 'SKIPPING: Data check only...')
        elif self.import_test_mode:
            self.vlog(1, 'SKIPPING: Import test mode enabled...')
        elif num_import_rows_processed:
            self.increment_ocl_versions(import_results=ocl_importer.results)
        else:
            self.vlog(1, 'Skipping because no records imported...')

        # Display debug info
        if self.verbosity >= 2:
            self.log('**** DEBUG INFO')
            if ocl_importer:
                print('ocl_importer.results:')
                pprint(ocl_importer.results)
