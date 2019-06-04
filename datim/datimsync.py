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
import ocldev.oclfleximporter
import deepdiff
import datimbase
import pprint


class DatimSync(datimbase.DatimBase):

    # Mode constants
    SYNC_MODE_DIFF_ONLY = 'diff'
    SYNC_MODE_BUILD_IMPORT_SCRIPT = 'script-only'
    SYNC_MODE_TEST_IMPORT = 'test'
    SYNC_MODE_FULL_IMPORT = 'full'
    SYNC_MODES = [
        SYNC_MODE_DIFF_ONLY,
        SYNC_MODE_BUILD_IMPORT_SCRIPT,
        SYNC_MODE_TEST_IMPORT,
        SYNC_MODE_FULL_IMPORT
    ]

    # Data check return values
    DATIM_SYNC_NO_DIFF = 0
    DATIM_SYNC_DIFF = 1

    OCL_EXPORT_DEFS = {}
    DHIS2_QUERIES = {}
    IMPORT_BATCHES = []

    # Set this to false if no OCL repositories are loaded initially to get dataset_ids
    SYNC_LOAD_DATASETS = True

    DEFAULT_OCL_EXPORT_CLEANING_METHOD = 'clean_ocl_export'

    # Sets an upper limit for the number of concept references to include in a single API request
    CONSOLIDATED_REFERENCE_BATCH_LIMIT = 25

    # Default fields to strip from OCL exports before performing deep diffs
    DEFAULT_CONCEPT_FIELDS_TO_REMOVE = ['version_created_by', 'created_on', 'updated_on',
                                        'version_created_on', 'created_by', 'updated_by', 'display_name',
                                        'display_locale', 'uuid', 'version', 'owner_url', 'source_url',
                                        'mappings', 'url', 'version_url', 'is_latest_version', 'locale']
    DEFAULT_CONCEPT_NAME_FIELDS_TO_REMOVE = ['uuid', 'type']
    DEFAULT_CONCEPT_DESC_FIELDS_TO_REMOVE = ['uuid', 'type']
    DEFAULT_MAPPING_ALLOWED_FIELDS = ['external_id', 'extras', 'id', 'from_source_url', 'map_type',
                                      'owner', 'owner_type', 'retired', 'source', 'to_concept_code',
                                      'to_source_url', 'to_concept_url', 'url', 'type']
    DEFAULT_MAPPING_FIELDS_TO_REMOVE = ['id', 'created_at', 'created_by', 'from_concept_code', 'from_concept_name',
                                        'from_source_name', 'from_source_owner', 'from_source_owner_type',
                                        'from_source_url', 'is_direct_mapping', 'is_external_mapping',
                                        'is_internal_mapping', 'is_inverse_mapping', 'updated_at', 'updated_by',
                                        'to_concept_name', 'to_source_name', 'to_source_owner',
                                        'to_source_owner_type', 'is_latest_version', 'update_comment', 'url',
                                        'version', 'versioned_object_id', 'versioned_object_url']

    def __init__(self):
        datimbase.DatimBase.__init__(self)

        self.dhis2_diff = {}
        self.ocl_diff = {}
        self.ocl_collections = []
        self.str_dataset_ids = ''
        self.run_dhis2_offline = False
        self.run_ocl_offline = False
        self.compare2previousexport = True
        self.import_limit = 0
        self.import_delay = 0
        self.diff_result = None
        self.sync_resource_types = None
        self.write_diff_to_file = True

        # Instructs the sync script to combine reference imports to the same source and within the same
        # import batch to a single API request. This results in a significant increase in performance.
        self.consolidate_references = True

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
        if self.run_dhis2_offline:
            self.log('**** RUNNING DHIS2 IN OFFLINE MODE ****')
        if self.run_ocl_offline:
            self.log('**** RUNNING OCL IN OFFLINE MODE ****')

    def prepare_ocl_exports(self, cleaning_attr=None):
        """
        Convert OCL exports into the diff format
        :param cleaning_attr: Optional cleaning attributes that are made available to each cleaning method
        :return: None
        """
        cnt = 0
        num_total = len(self.OCL_EXPORT_DEFS)
        for ocl_export_def_key, export_def in self.OCL_EXPORT_DEFS.iteritems():
            cnt += 1
            self.vlog(1, '** [OCL Export %s of %s] %s:' % (cnt, num_total, ocl_export_def_key))
            cleaning_method_name = export_def.get('cleaning_method', self.DEFAULT_OCL_EXPORT_CLEANING_METHOD)
            getattr(self, cleaning_method_name)(export_def, cleaning_attr=cleaning_attr)
        with open(self.attach_absolute_data_path(self.OCL_CLEANED_EXPORT_FILENAME), 'wb') as output_file:
            output_file.write(json.dumps(self.ocl_diff))
            self.vlog(1, 'Cleaned OCL exports successfully written to "%s"' % (
                self.OCL_CLEANED_EXPORT_FILENAME))

    def get_mapping_key(self, mapping_source_url='', mapping_owner_type='', mapping_owner_id='', mapping_source_id='',
                        from_concept_url='', map_type='', to_concept_url='',
                        to_source_url='', to_concept_code=''):
        # Handle the source url
        if not mapping_source_url:
            mapping_owner_stem = datimbase.DatimBase.owner_type_to_stem(mapping_owner_type)
            if not mapping_owner_stem:
                self.log('ERROR: Invalid mapping_owner_type "%s"' % mapping_owner_type)
                sys.exit(1)
            mapping_source_url = '/%s/%s/sources/%s/' % (mapping_owner_stem, mapping_owner_id, mapping_source_id)

        # Build the key
        if not to_concept_url:
            to_concept_url = '%s%s' % (to_source_url, to_concept_code)
        key = ('%smappings/?from=%s&maptype=%s&to=%s' % (
            mapping_source_url, from_concept_url, map_type, to_concept_url))
        return key

    def clean_ocl_export(self, ocl_export_def, cleaning_attr=None):
        """
        Default method for cleaning an OCL export to prepare it for a diff
        :param ocl_export_def:
        :param cleaning_attr:
        :return:
        """
        import_batch_key = ocl_export_def['import_batch']
        jsonfilename = self.endpoint2filename_ocl_export_json(ocl_export_def['endpoint'])
        with open(self.attach_absolute_data_path(jsonfilename), 'rb') as input_file:
            ocl_repo_export_raw = json.load(input_file)

            if ocl_repo_export_raw['type'] in ['Source', 'Source Version']:

                # Concepts
                num_concepts = 0
                for c in ocl_repo_export_raw['concepts']:
                    concept_key = c['url']
                    # Remove core concept fields not involved in the diff
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

                # Mappings
                num_mappings = 0
                for m in ocl_repo_export_raw['mappings']:
                    mapping_key = self.get_mapping_key(
                        mapping_owner_type=m['owner_type'], mapping_owner_id=m['owner'], mapping_source_id=m['source'],
                        from_concept_url=m['from_concept_url'], map_type=m['map_type'],
                        to_concept_url=m['to_concept_url'], to_source_url=m['to_source_url'],
                        to_concept_code=m['to_concept_code'])
                    # Remove core mapping fields not involved in the diff
                    for f in self.DEFAULT_MAPPING_FIELDS_TO_REMOVE:
                        if f in m:
                            del m[f]
                    # Transform some fields
                    if m['type'] == 'MappingVersion':
                        # Note that this is an error in
                        m['type'] = 'Mapping'
                    if m['to_concept_url']:
                        # Internal mapping, so remove to_concept_code and to_source_url
                        del m['to_source_url']
                        del m['to_concept_code']
                    else:
                        # External mapping, so remove to_concept_url
                        del m['to_concept_url']
                    self.ocl_diff[import_batch_key][self.RESOURCE_TYPE_MAPPING][mapping_key] = m
                    num_mappings += 1

                self.vlog(1, 'Cleaned %s concepts and %s mappings' % (num_concepts, num_mappings))

            elif ocl_repo_export_raw['type'] in ['Collection', 'Collection Version']:

                # References for concepts and mappings
                num_concept_refs = 0
                num_mapping_refs = 0
                for ref in ocl_repo_export_raw['references']:
                    collection_url = ocl_export_def['endpoint']
                    if ref['reference_type'] == 'concepts':
                        concept_ref_key, concept_ref_json = self.get_concept_reference_json(
                            collection_url=collection_url, concept_url=ref['expression'], strip_concept_version=True)
                        self.ocl_diff[import_batch_key][self.RESOURCE_TYPE_CONCEPT_REF][
                            concept_ref_key] = concept_ref_json
                        num_concept_refs += 1
                    elif ref['reference_type'] == 'mappings':
                        mapping_ref_key, mapping_ref_json = self.get_mapping_reference_json_from_export(
                            full_collection_export_dict=ocl_repo_export_raw, collection_url=collection_url,
                            mapping_url=ref['expression'], strip_mapping_version=True)
                        self.ocl_diff[import_batch_key][self.RESOURCE_TYPE_MAPPING_REF][
                            mapping_ref_key] = mapping_ref_json
                        num_mapping_refs += 1
                        pass

                self.vlog(1, 'Cleaned %s concept references and skipped %s mapping references' % (
                    num_concept_refs, num_mapping_refs))

    def cache_dhis2_exports(self):
        """
        Delete old DHIS2 cached files if there
        :return: None
        """
        for dhis2_query_key in self.DHIS2_QUERIES:
            # Delete old file if it exists
            dhis2filename_export_new = self.dhis2filename_export_new(self.DHIS2_QUERIES[dhis2_query_key]['id'])
            dhis2filename_export_old = self.dhis2filename_export_old(self.DHIS2_QUERIES[dhis2_query_key]['id'])
            if os.path.isfile(self.attach_absolute_data_path(dhis2filename_export_old)):
                os.remove(self.attach_absolute_data_path(dhis2filename_export_old))
            copyfile(self.attach_absolute_data_path(dhis2filename_export_new),
                     self.attach_absolute_data_path(dhis2filename_export_old))
            self.vlog(1, 'DHIS2 export successfully copied to "%s"' % dhis2filename_export_old)

    def transform_dhis2_exports(self, conversion_attr=None):
        """
        Transforms DHIS2 exports into the diff format
        :param conversion_attr: Optional conversion attributes that are made available to each conversion method
        :return: None
        """
        cnt = 0
        for dhis2_query_key, dhis2_query_def in self.DHIS2_QUERIES.iteritems():
            cnt += 1
            self.vlog(1, '** [DHIS2 Export %s of %s] %s:' % (cnt, len(self.DHIS2_QUERIES), dhis2_query_key))
            getattr(self, dhis2_query_def['conversion_method'])(dhis2_query_def, conversion_attr=conversion_attr)
        with open(self.attach_absolute_data_path(self.DHIS2_CONVERTED_EXPORT_FILENAME), 'wb') as output_file:
            output_file.write(json.dumps(self.dhis2_diff))
            self.vlog(1, 'Transformed DHIS2 exports successfully written to "%s"' % (
                self.DHIS2_CONVERTED_EXPORT_FILENAME))

    def save_dhis2_query_to_file(self, query='', query_attr=None, outputfilename=''):
        """ Execute DHIS2 query and save to file """

        # Replace query attribute names with values and build the query URL
        url_dhis2_query = self.dhis2env + self.replace_attr(query, query_attr)

        # Execute the query
        self.vlog(1, 'Request URL:', url_dhis2_query)
        r = requests.get(url_dhis2_query, auth=HTTPBasicAuth(self.dhis2uid, self.dhis2pwd))
        r.raise_for_status()
        content_length = 0
        with open(self.attach_absolute_data_path(outputfilename), 'wb') as handle:
            for block in r.iter_content(1024):
                handle.write(block)
                content_length += len(block)
        return content_length

    def perform_diff(self, ocl_diff=None, dhis2_diff=None):
        """
        Performs deep diff on the prepared OCL and DHIS2 resources
        :param ocl_diff: Content from OCL for the diff
        :param dhis2_diff: Content from DHIS2 for the diff
        :return:
        """
        diff = {}
        for import_batch_key in self.IMPORT_BATCHES:
            diff[import_batch_key] = {}
            for resource_type in self.sync_resource_types:
                if resource_type in ocl_diff[import_batch_key] and resource_type in dhis2_diff[import_batch_key]:
                    resource_specific_diff = deepdiff.DeepDiff(
                        ocl_diff[import_batch_key][resource_type],
                        dhis2_diff[import_batch_key][resource_type],
                        ignore_order=True, verbose_level=2)
                    if resource_type in [self.RESOURCE_TYPE_CONCEPT, self.RESOURCE_TYPE_MAPPING] and 'dictionary_item_removed' in resource_specific_diff:
                        # Remove resources retired in OCL from the diff results - no action needed
                        keys = resource_specific_diff['dictionary_item_removed'].keys()
                        for key in keys:
                            if 'retired' in resource_specific_diff['dictionary_item_removed'][key] and resource_specific_diff['dictionary_item_removed'][key]['retired']:
                                del(resource_specific_diff['dictionary_item_removed'][key])
                    diff[import_batch_key][resource_type] = resource_specific_diff
                    if self.verbosity:
                        str_log = 'IMPORT_BATCH["%s"]["%s"]: ' % (import_batch_key, resource_type)
                        for k in diff[import_batch_key][resource_type]:
                            str_log += '%s: %s; ' % (k, len(diff[import_batch_key][resource_type][k]))
                        self.log(str_log)
        return diff

    def generate_import_scripts(self, diff):
        """
        Generate import scripts
        :param diff: Diff results used to generate the import script
        :return:
        """
        with open(self.attach_absolute_data_path(self.NEW_IMPORT_SCRIPT_FILENAME), 'wb') as output_file:
            for import_batch in self.IMPORT_BATCHES:
                for resource_type in self.sync_resource_types:
                    if resource_type not in diff[import_batch]:
                        continue

                    # Process new items
                    consolidated_concept_refs = {}
                    consolidated_mapping_refs = {}
                    if 'dictionary_item_added' in diff[import_batch][resource_type]:
                        for k, r in diff[import_batch][resource_type]['dictionary_item_added'].iteritems():
                            if resource_type == self.RESOURCE_TYPE_COLLECTION and r['type'] == self.RESOURCE_TYPE_COLLECTION:
                                output_file.write(json.dumps(r))
                                output_file.write('\n')
                            elif resource_type == self.RESOURCE_TYPE_CONCEPT and r['type'] == self.RESOURCE_TYPE_CONCEPT:
                                output_file.write(json.dumps(r))
                                output_file.write('\n')
                            elif resource_type == self.RESOURCE_TYPE_MAPPING and r['type'] == self.RESOURCE_TYPE_MAPPING:
                                output_file.write(json.dumps(r))
                                output_file.write('\n')
                            elif resource_type == self.RESOURCE_TYPE_CONCEPT_REF and r['type'] == self.RESOURCE_TYPE_REFERENCE:
                                r['__cascade'] = 'sourcemappings'
                                if self.consolidate_references:
                                    if r['collection_url'] in consolidated_concept_refs:
                                        consolidated_concept_refs[r['collection_url']]['data']['expressions'].append(
                                            r['data']['expressions'][0])
                                        # Go ahead and write if reached the reference limit
                                        if len(consolidated_concept_refs[r['collection_url']]['data']['expressions']) >= self.CONSOLIDATED_REFERENCE_BATCH_LIMIT:
                                            output_file.write(json.dumps(consolidated_concept_refs[r['collection_url']]))
                                            output_file.write('\n')
                                            del(consolidated_concept_refs[r['collection_url']])
                                    else:
                                        consolidated_concept_refs[r['collection_url']] = r.copy()
                                else:
                                    output_file.write(json.dumps(r))
                                    output_file.write('\n')
                            elif resource_type == self.RESOURCE_TYPE_MAPPING_REF and r['type'] == self.RESOURCE_TYPE_REFERENCE:
                                r['__cascade'] = 'sourcemappings'
                                if self.consolidate_references:
                                    if r['collection_url'] in consolidated_mapping_refs:
                                        consolidated_mapping_refs[r['collection_url']]['data']['expressions'].append(
                                            r['data']['expressions'][0])
                                    else:
                                        consolidated_mapping_refs[r['collection_url']] = r.copy()
                                else:
                                    output_file.write(json.dumps(r))
                                    output_file.write('\n')
                            else:
                                self.log('ERROR: Unrecognized resource_type "%s": {%s}' % (resource_type, str(r)))
                                sys.exit(1)

                    # Write consolidated references for new items
                    if self.consolidate_references:
                        for collection_url in consolidated_concept_refs:
                            output_file.write(json.dumps(consolidated_concept_refs[collection_url]))
                            output_file.write('\n')
                        for collection_url in consolidated_mapping_refs:
                            output_file.write(json.dumps(consolidated_mapping_refs[collection_url]))
                            output_file.write('\n')

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

    def get_mapping_reference_json_from_export(
            self, full_collection_export_dict=None, collection_url='', collection_owner_id='',
            collection_owner_type='', collection_id='', mapping_url='', strip_mapping_version=False):

        # Build the collection_url
        if collection_url:
            # all good...
            pass
        elif collection_owner_id and collection_id:
            collection_owner_stem = datimbase.DatimBase.owner_type_to_stem(collection_owner_type, self.OWNER_STEM_ORGS)
            collection_url = '/%s/%s/collections/%s/' % (collection_owner_stem, collection_owner_id, collection_id)
        else:
            self.log('ERROR: Must provide "collection_owner_type", "collection_owner_id", and "collection_id" '
                     'parameters or "collection_url" parameter for get_mapping_reference_json()')
            sys.exit(1)

        # Optionally strip the mapping version
        # Ex: '/orgs/PEPFAR/sources/MER/mappings/59d10e440855590040f0e528/1/' ==>
        # '/orgs/PEPFAR/sources/MER/mappings/59d10e440855590040f0e528/'
        if strip_mapping_version and mapping_url.count('/') == 8:
            mapping_url = mapping_url[:self.find_nth(mapping_url, '/', 7)+1]

        # Find the related mapping from the full collection export
        mapping_from_export = (item for item in full_collection_export_dict['mappings'] if str(
            item["versioned_object_url"]) == str(mapping_url)).next()
        if not mapping_from_export:
            self.log('something really wrong happened here...')
            sys.exit(1)
        mapping_owner_stem = datimbase.DatimBase.owner_type_to_stem(mapping_from_export['owner_type'])
        mapping_owner = mapping_from_export['owner']
        mapping_source = mapping_from_export['source']
        mapping_source_url = '/%s/%s/sources/%s/' % (mapping_owner_stem, mapping_owner, mapping_source)
        from_concept_url = mapping_from_export['from_concept_url']
        map_type = mapping_from_export['map_type']
        if mapping_from_export['to_concept_url']:
            to_concept_url = mapping_from_export['to_concept_url']
        else:
            to_concept_url = '%s%s/' % (mapping_from_export['to_source_url'], mapping_from_export['to_concept_code'])

        # Build the mapping reference key and reference object
        key = ('%smappings/?from=%s&maptype=%s&to=%s' % (
            mapping_source_url, from_concept_url, map_type, to_concept_url))
        reference_key = '%sreferences/?source=%s&from=%s&maptype=%s&to=%s' % (
            collection_url, mapping_source_url, from_concept_url, map_type, to_concept_url)
        reference_json = {
            'type': 'Reference',
            'collection_url': collection_url,
            'data': {"expressions": [mapping_url]}
        }

        return reference_key, reference_json

    def get_collection_json(self, owner_id='', collection_owner_type='', collection_id='', name='', full_name='',
                            short_code='', default_locale='en', external_id='', collection_type='',
                            public_access='View', supported_locales='en', extras=None):
        collection_owner_stem = datimbase.DatimBase.owner_type_to_stem(collection_owner_type, self.OWNER_STEM_ORGS)
        collection_url = '/%s/%s/collections/%s/' % (collection_owner_stem, owner_id, collection_id)
        collection_key = collection_url
        collection_dict = {
            'type': self.RESOURCE_TYPE_COLLECTION,
            'id': collection_id,
            'name': name,
            'default_locale': default_locale,
            'short_code': short_code,
            'external_id': external_id,
            'public_access': public_access,
            'full_name': full_name,
            'collection_type': collection_type,
            'supported_locales': supported_locales,
            'owner': owner_id,
            'owner_type': collection_owner_type
        }
        if extras:
            collection_dict['extras'] = extras
        return collection_key, collection_dict

    def get_concept_reference_json(self, collection_owner_id='', collection_owner_type='', collection_id='',
                                   collection_url='', concept_url='', strip_concept_version=False):
        """ Returns an "importable" python dictionary for an OCL Reference with the specified attributes """

        # Build the collection_url
        if collection_url:
            # all good...
            pass
        elif collection_owner_id and collection_id:
            collection_owner_stem = datimbase.DatimBase.owner_type_to_stem(collection_owner_type, self.OWNER_STEM_ORGS)
            collection_url = '/%s/%s/collections/%s/' % (collection_owner_stem, collection_owner_id, collection_id)
        else:
            self.log('ERROR: Must provide "collection_owner_type", "collection_owner_id", and "collection_id" '
                     'parameters or "collection_url" parameter for get_concept_reference_json()')
            sys.exit(1)

        # Optionally strip the concept version
        # Ex: '/orgs/PEPFAR/sources/MER/concepts/T4GeVmTlku0/59ce97540855590040f0aaf6/' ==>
        # '/orgs/PEPFAR/sources/MER/concepts/T4GeVmTlku0/'
        if strip_concept_version and concept_url.count('/') == 8:
            concept_url = concept_url[:self.find_nth(concept_url, '/', 7)+1]

        # Build the concept reference key and reference object
        reference_key = '%sreferences/?concept=%s' % (collection_url, concept_url)
        reference_json = {
            'type': 'Reference',
            'collection_url': collection_url,
            'data': {"expressions": [concept_url]}
        }
        return reference_key, reference_json

    def load_dhis2_exports(self):
        """ Load the DHIS2 export files """
        cnt = 0
        for dhis2_query_key, dhis2_query_def in self.DHIS2_QUERIES.iteritems():
            cnt += 1
            self.vlog(1, '** [DHIS2 Export %s of %s] %s:' % (cnt, len(self.DHIS2_QUERIES), dhis2_query_key))
            dhis2filename_export_new = self.dhis2filename_export_new(dhis2_query_def['id'])
            if not self.run_dhis2_offline:
                query_attr = {'active_dataset_ids': self.str_active_dataset_ids}
                content_length = self.save_dhis2_query_to_file(
                    query=dhis2_query_def['query'], query_attr=query_attr,
                    outputfilename=dhis2filename_export_new)
                self.vlog(1, '%s bytes retrieved from DHIS2 and written to file "%s"' % (
                    content_length, dhis2filename_export_new))
            else:
                self.vlog(1, 'DHIS2-OFFLINE: Using local file: "%s"' % dhis2filename_export_new)
                if os.path.isfile(self.attach_absolute_data_path(dhis2filename_export_new)):
                    self.vlog(1, 'DHIS2-OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                        dhis2filename_export_new,
                        os.path.getsize(self.attach_absolute_data_path(dhis2filename_export_new))))
                else:
                    self.log('ERROR: Could not find offline dhis2 file "%s". Exiting...' % dhis2filename_export_new)
                    sys.exit(1)

    def bulk_import_references(self):
        self.consolidate_references = True
        self.compare2previousexport = False
        return self.run(resource_types=[self.RESOURCE_TYPE_CONCEPT_REF])

    def run(self, sync_mode=None, resource_types=None):
        """
        Performs a diff between DATIM DHIS2 and OCL and imports the differences into OCL
        :param sync_mode: Mode to run the sync operation. See SYNC_MODE constants
        :param resource_types: List of resource types to include in the sync operation. See RESOURCE_TYPE constants
        :return:
        """

        # Make sure sync_mode is valid
        if sync_mode not in self.SYNC_MODES:
            self.log('ERROR: Invalid sync_mode "%s"' % sync_mode)
            sys.exit(1)

        # Determine which resource types will be processed during this run
        if resource_types:
            self.sync_resource_types = resource_types
        else:
            self.sync_resource_types = self.DEFAULT_SYNC_RESOURCE_TYPES

        # Log the settings
        if self.verbosity:
            self.log_settings()

        # STEP 1 of 12: Load OCL Collections for Dataset IDs
        # NOTE: This step occurs regardless of sync mode
        self.vlog(1, '**** STEP 1 of 12: Load OCL Collections for Dataset IDs')
        if self.SYNC_LOAD_DATASETS:
            self.load_datasets_from_ocl()
        else:
            self.vlog(1, 'SKIPPING: SYNC_LOAD_DATASETS set to "False"')
            if self.DHIS2_QUERIES:
                for dhis2_query_key in self.DHIS2_QUERIES:
                    if 'active_dataset_ids' in self.DHIS2_QUERIES[dhis2_query_key]:
                        self.active_dataset_keys = self.DHIS2_QUERIES[dhis2_query_key]['active_dataset_ids']
                        self.str_active_dataset_ids = ','.join(self.active_dataset_keys)
                        self.vlog(1, 'INFO: Using hardcoded active dataset IDs: %s' % self.str_active_dataset_ids)
                        break

        # STEP 2 of 12: Load new exports from DATIM-DHIS2
        # NOTE: This step occurs regardless of sync mode
        self.vlog(1, '**** STEP 2 of 12: Load latest exports from DHIS2 using Dataset IDs returned from OCL in Step 1')
        self.load_dhis2_exports()

        # STEP 3 of 12: Quick comparison of current and previous DHIS2 exports
        # Compares new DHIS2 export to most recent previous export from a successful sync that is available
        # NOTE: This step is skipped if in DIFF mode or compare2previousexport is set to False
        self.vlog(1, '**** STEP 3 of 12: Quick comparison of current and previous DHIS2 exports')
        complete_match = True
        if self.compare2previousexport and sync_mode != DatimSync.SYNC_MODE_DIFF_ONLY:
            # Compare files for each of the DHIS2 queries
            for dhis2_query_key, dhis2_query_def in self.DHIS2_QUERIES.iteritems():
                self.vlog(1, dhis2_query_key + ':')
                dhis2filename_export_new = self.dhis2filename_export_new(dhis2_query_def['id'])
                dhis2filename_export_old = self.dhis2filename_export_old(dhis2_query_def['id'])
                if self.filecmp(self.attach_absolute_data_path(dhis2filename_export_old),
                                self.attach_absolute_data_path(dhis2filename_export_new)):
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
        elif sync_mode == DatimSync.SYNC_MODE_DIFF_ONLY:
            self.vlog(1, "SKIPPING: Diff check only...")
        else:
            self.vlog(1, "SKIPPING: compare2previousexport == false")

        # STEP 4 of 12: Fetch latest versions of relevant OCL exports
        # NOTE: This step occurs regardless of sync mode
        self.vlog(1, '**** STEP 4 of 12: Fetch latest versions of relevant OCL exports')
        cnt = 0
        num_total = len(self.OCL_EXPORT_DEFS)
        for ocl_export_def_key in self.OCL_EXPORT_DEFS:
            cnt += 1
            self.vlog(1, '** [OCL Export %s of %s] %s:' % (cnt, num_total, ocl_export_def_key))
            export_def = self.OCL_EXPORT_DEFS[ocl_export_def_key]
            zipfilename = self.endpoint2filename_ocl_export_zip(export_def['endpoint'])
            jsonfilename = self.endpoint2filename_ocl_export_json(export_def['endpoint'])
            if not self.run_ocl_offline:
                self.get_ocl_export(endpoint=export_def['endpoint'], version='latest', zipfilename=zipfilename,
                                    jsonfilename=jsonfilename)
            else:
                self.vlog(1, 'OCL-OFFLINE: Using local file "%s"...' % jsonfilename)
                if os.path.isfile(self.attach_absolute_data_path(jsonfilename)):
                    self.vlog(1, 'OCL-OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                        jsonfilename, os.path.getsize(self.attach_absolute_data_path(jsonfilename))))
                else:
                    self.log('ERROR: Could not find offline OCL file "%s". Exiting...' % jsonfilename)
                    sys.exit(1)

        # STEP 5 of 12: Transform new DHIS2 export to diff format
        # NOTE: This step occurs regardless of sync mode
        self.vlog(1, '**** STEP 5 of 12: Transform DHIS2 exports to OCL-formatted JSON')
        self.dhis2_diff = {}
        for import_batch_key in self.IMPORT_BATCHES:
            self.dhis2_diff[import_batch_key] = {}
            for resource_type in self.sync_resource_types:
                self.dhis2_diff[import_batch_key][resource_type] = {}
        conversion_attr = {
            'ocl_dataset_repos': self.ocl_dataset_repos,
            'active_dataset_keys': self.active_dataset_keys,
        }
        self.transform_dhis2_exports(conversion_attr=conversion_attr)

        # STEP 6 of 12: Prepare OCL exports for diff
        # NOTE: This step occurs regardless of sync mode
        self.vlog(1, '**** STEP 6 of 12: Prepare OCL exports for diff')
        self.ocl_diff = {}
        for import_batch_key in self.IMPORT_BATCHES:
            self.ocl_diff[import_batch_key] = {}
            for resource_type in self.sync_resource_types:
                self.ocl_diff[import_batch_key][resource_type] = {}
        self.prepare_ocl_exports(cleaning_attr={})

        # STEP 7 of 12: Perform deep diff
        # One deep diff is performed per resource type in each import batch
        # OCL/DHIS2 exports reloaded from file to eliminate unicode type_change diff -- but that may be short sighted!
        # NOTE: This step occurs regardless of sync mode
        self.vlog(1, '**** STEP 7 of 12: Perform deep diff')
        with open(self.attach_absolute_data_path(self.OCL_CLEANED_EXPORT_FILENAME), 'rb') as file_ocl_diff,\
                open(self.attach_absolute_data_path(self.DHIS2_CONVERTED_EXPORT_FILENAME), 'rb') as file_dhis2_diff:
            local_ocl_diff = json.load(file_ocl_diff)
            local_dhis2_diff = json.load(file_dhis2_diff)
            self.diff_result = self.perform_diff(ocl_diff=local_ocl_diff, dhis2_diff=local_dhis2_diff)

        # TODO: Remove the diff_result display after final testing of DATIM FY18 content
        pprint.pprint(self.diff_result)

        '''
        # JP (2018-08-29): Unable to serialize NoneType or sets as JSON
        if self.write_diff_to_file:
            filename_diff_results = self.filename_diff_result(self.SYNC_NAME)
            with open(self.attach_absolute_data_path(filename_diff_results), 'wb') as ofile:
                ofile.write(json.dumps(self.diff_result))
            self.vlog(1, 'Diff results successfully written to "%s"' % filename_diff_results)
        '''

        # STEP 8 of 12: Determine action based on diff result
        # NOTE: This step occurs regardless of sync mode -- processing terminates here if DIFF mode
        self.vlog(1, '**** STEP 8 of 12: Determine action based on diff result')
        if self.diff_result:
            self.vlog(1, 'One or more differences identified between DHIS2 and OCL...')
        else:
            self.vlog(1, 'No diff between DHIS2 and OCL...')

        # STEP 9 of 12: Generate one OCL import script per import batch by processing the diff results
        # Note that OCL import scripts are JSON-lines files
        # NOTE: This step occurs unless in DIFF mode
        self.vlog(1, '**** STEP 9 of 12: Generate import scripts')
        if sync_mode != DatimSync.SYNC_MODE_DIFF_ONLY:
            self.generate_import_scripts(self.diff_result)
        else:
            self.vlog(1, 'SKIPPING: Diff check only')

        # STEP 10 of 12: Perform the import in OCL
        # NOTE: This step occurs regardless of sync mode
        self.vlog(1, '**** STEP 10 of 12: Perform the import in OCL')
        num_import_rows_processed = 0
        ocl_importer = None
        if sync_mode in [DatimSync.SYNC_MODE_TEST_IMPORT, DatimSync.SYNC_MODE_FULL_IMPORT]:
            test_mode = False
            if sync_mode == DatimSync.SYNC_MODE_TEST_IMPORT:
                test_mode = True
            ocl_importer = ocldev.oclfleximporter.OclFlexImporter(
                file_path=self.attach_absolute_data_path(self.NEW_IMPORT_SCRIPT_FILENAME),
                api_token=self.oclapitoken, api_url_root=self.oclenv, test_mode=test_mode,
                do_update_if_exists=False, verbosity=self.verbosity, limit=self.import_limit,
                import_delay=self.import_delay)
            num_import_rows_processed = ocl_importer.process()
            self.vlog(1, 'Import records processed:', num_import_rows_processed)
        elif sync_mode == DatimSync.SYNC_MODE_DIFF_ONLY:
            self.vlog(1, 'SKIPPING: Diff check only...')
        elif sync_mode == DatimSync.SYNC_MODE_BUILD_IMPORT_SCRIPT:
            self.vlog(1, 'SKIPPING: Building import script only...')

        # STEP 11 of 12: Save new DHIS2 export for the next sync attempt
        self.vlog(1, '**** STEP 11 of 12: Save the DHIS2 export')
        if sync_mode == DatimSync.SYNC_MODE_FULL_IMPORT:
            if num_import_rows_processed:
                self.cache_dhis2_exports()
            else:
                self.vlog(1, 'SKIPPING: No records imported (possibly due to error)...')
        elif sync_mode == DatimSync.SYNC_MODE_DIFF_ONLY:
            self.vlog(1, 'SKIPPING: Diff check only...')
        elif sync_mode == DatimSync.SYNC_MODE_BUILD_IMPORT_SCRIPT:
            self.vlog(1, 'SKIPPING: Building import script only...')
        elif sync_mode == DatimSync.SYNC_MODE_TEST_IMPORT:
            self.vlog(1, 'SKIPPING: Import test mode enabled...')

        # STEP 12 of 12: Manage OCL repository versions
        self.vlog(1, '**** STEP 12 of 12: Manage OCL repository versions')
        if sync_mode == DatimSync.SYNC_MODE_FULL_IMPORT:
            if num_import_rows_processed:
                self.increment_ocl_versions(import_results=ocl_importer.import_results)
            else:
                self.vlog(1, 'Skipping because no records imported...')
        elif sync_mode == DatimSync.SYNC_MODE_DIFF_ONLY:
            self.vlog(1, 'SKIPPING: Diff check only...')
        elif sync_mode == DatimSync.SYNC_MODE_TEST_IMPORT:
            self.vlog(1, 'SKIPPING: Import test mode enabled...')

        # Display debug info
        if self.verbosity >= 2:
            self.log('**** DEBUG INFO')
            if ocl_importer and ocl_importer.import_results:
                print(ocl_importer.import_results.get_detailed_summary())

        # Return the diff result (may return something else in the end)
        if self.diff_result:
            return self.DATIM_SYNC_DIFF
        else:
            return self.DATIM_SYNC_NO_DIFF
