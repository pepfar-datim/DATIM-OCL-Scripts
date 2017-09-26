from __future__ import with_statement
import os
import itertools
import functools
import operator
import requests
import sys
import tarfile
from datetime import datetime
import json
from deepdiff import DeepDiff
from requests.auth import HTTPBasicAuth
from shutil import copyfile


class DatimBase:
    """ Shared base class for DATIM synchronization and presentation """

    # Resource type constants
    RESOURCE_TYPE_CONCEPT = 'Concept'
    RESOURCE_TYPE_MAPPING = 'Mapping'
    RESOURCE_TYPE_CONCEPT_REF = 'concept_ref'
    RESOURCE_TYPE_MAPPING_REF = 'mapping_ref'
    RESOURCE_TYPE_REFERENCE = 'Reference'
    RESOURCE_TYPES = [
        RESOURCE_TYPE_CONCEPT,
        RESOURCE_TYPE_MAPPING,
        RESOURCE_TYPE_CONCEPT_REF,
        RESOURCE_TYPE_MAPPING_REF
    ]

    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    def __init__(self):
        pass

    def log(self, *args):
        """ Output log information """
        sys.stdout.write('[' + str(datetime.now()) + '] ')
        for arg in args:
            sys.stdout.write(str(arg))
            sys.stdout.write(' ')
        sys.stdout.write('\n')
        sys.stdout.flush()

    def attach_absolute_path(self, filename):
        """ Adds full absolute path to the filename """
        return os.path.join(self.__location__, filename)

    def get_ocl_repositories(self, url='', key_field='id', require_external_id=True,
                             active_attr_name='__datim_sync'):
        """
        Gets repositories from OCL using the provided URL, optionally filtering
        by external_id and a custom attribute indicating active status
        """
        r = requests.get(url, headers=self.oclapiheaders)
        r.raise_for_status()
        repos = r.json()
        filtered_repos = {}
        for r in repos:
            if (not require_external_id or ('external_id' in r and r['external_id'])) and (not active_attr_name or (r['extras'] and active_attr_name in r['extras'] and r['extras'][active_attr_name])):
                filtered_repos[r[key_field]] = r
        return filtered_repos

    def transform_dhis2_exports(self, conversion_attr=None):
        """
        Transforms DHIS2 exports into the diff format
        :param conversion_attr: Optional conversion attributes that are made available to each conversion method
        :return: None
        """
        for dhis2_query_key, dhis2_query_def in self.DHIS2_QUERIES.iteritems():
            if self.verbosity:
                self.log('%s:' % dhis2_query_key)
            getattr(self, dhis2_query_def['conversion_method'])(dhis2_query_def, conversion_attr=conversion_attr)
        with open(self.attach_absolute_path(self.dhis2_converted_export_filename), 'wb') as output_file:
            output_file.write(json.dumps(self.dhis2_diff))
            if self.verbosity:
                self.log('Transformed DHIS2 exports successfully written to "%s"' % (
                    self.dhis2_converted_export_filename))

    def prepare_ocl_exports(self, cleaning_attr=None):
        """
        Convert OCL exports into the diff format
        :param cleaning_attr: Optional cleaning attributes that are made available to each cleaning method
        :return: None
        """
        for ocl_export_def_key, export_def in self.OCL_EXPORT_DEFS.iteritems():
            if self.verbosity:
                self.log('%s:' % ocl_export_def_key)
            getattr(self, export_def['cleaning_method'])(export_def, cleaning_attr=cleaning_attr)
        with open(self.attach_absolute_path(self.ocl_cleaned_export_filename), 'wb') as output_file:
            output_file.write(json.dumps(self.ocl_diff))
            if self.verbosity:
                self.log('Cleaned OCL exports successfully written to "%s"' % (
                    self.ocl_cleaned_export_filename))

    def save_dhis2_query_to_file(self, query='', query_attr=None, outputfilename=''):
        """ Execute DHIS2 query and save to file """

        # Replace query attribute names with values and build the query URL
        if query_attr:
            for attr_name in query_attr:
                query = query.replace('{{'+attr_name+'}}', query_attr[attr_name])
        url_dhis2_query = self.dhis2env + query

        # Execute the query
        if self.verbosity:
            self.log('Request URL:', url_dhis2_query)
        r = requests.get(url_dhis2_query, auth=HTTPBasicAuth(self.dhis2uid, self.dhis2pwd))
        r.raise_for_status()
        with open(self.attach_absolute_path(outputfilename), 'wb') as handle:
            for block in r.iter_content(1024):
                handle.write(block)
        return r.headers['Content-Length']

    def filecmp(self, filename1, filename2):
        """
        Do the two files have exactly the same size and contents?
        :param filename1:
        :param filename2:
        :return: Boolean
        """
        try:
            with open(filename1, "rb") as fp1, open(filename2, "rb") as fp2:
                if os.fstat(fp1.fileno()).st_size != os.fstat(fp2.fileno()).st_size:
                    return False  # different sizes therefore not equal
                fp1_reader = functools.partial(fp1.read, 4096)
                fp2_reader = functools.partial(fp2.read, 4096)
                cmp_pairs = itertools.izip(iter(fp1_reader, ''), iter(fp2_reader, ''))
                inequalities = itertools.starmap(operator.ne, cmp_pairs)
                return not any(inequalities)
        except:
            return False

    def cache_dhis2_exports(self):
        """
        Delete old DHIS2 cached files if there
        :return: None
        """
        for dhis2_query_key in self.DHIS2_QUERIES:
            # Delete old file if it exists
            if 'old_export_filename' not in self.DHIS2_QUERIES[dhis2_query_key]:
                continue
            if os.path.isfile(self.attach_absolute_path(self.DHIS2_QUERIES[dhis2_query_key]['old_export_filename'])):
                os.remove(self.attach_absolute_path(self.DHIS2_QUERIES[dhis2_query_key]['old_export_filename']))
            copyfile(self.attach_absolute_path(self.DHIS2_QUERIES[dhis2_query_key]['new_export_filename']),
                     self.attach_absolute_path(self.DHIS2_QUERIES[dhis2_query_key]['old_export_filename']))
            if self.verbosity:
                self.log('DHIS2 export successfully copied to "%s"' %
                         self.DHIS2_QUERIES[dhis2_query_key]['old_export_filename'])

    def increment_ocl_versions(self):
        dt = datetime.utcnow()
        for ocl_export_key, ocl_export_def in self.OCL_EXPORT_DEFS.iteritems():
            # TODO: first check if any changes were made to the repository
            num_processed = 0
            new_repo_version_data = {
                'id': 'v' + dt.strftime('%Y-%m-%dT%H%M%S.%f'),
                'description': 'Automatically generated by DATIM-Sync - %s import record(s) processed' % num_processed,
                'released': True,
            }
            repo_version_endpoint = ocl_export_def['endpoint'] + 'versions/'
            new_repo_version_url = self.oclenv + repo_version_endpoint
            if self.verbosity:
                self.log('Create new repo version request URL:', new_repo_version_url)
                self.log(json.dumps(new_repo_version_data))
            r = requests.post(new_repo_version_url,
                              data=json.dumps(new_repo_version_data),
                              headers=self.oclapiheaders)
            r.raise_for_status()

    def get_ocl_export(self, endpoint='', version='', tarfilename='', jsonfilename=''):
        """
        Fetches an export of the specified repository version and saves to file.
        Use version="latest" to fetch the most recent released repo version.
        Note that the export must already exist before using this method.
        :param endpoint: endpoint must point to the repo endpoint only, e.g. '/orgs/myorg/sources/mysource/'
        :param version: repo version ID or "latest"
        :param tarfilename: Filename to save the compressed OCL export to
        :param jsonfilename: Filename to save the decompressed OCL-JSON export to
        :return: bool True upon success; False otherwise
        """
        # Get the latest version of the repo
        # TODO: error handling for case when no repo version is returned
        if version == 'latest':
            url_latest_version = self.oclenv + endpoint + 'latest/'
            if self.verbosity:
                self.log('Latest version request URL:', url_latest_version)
            r = requests.get(url_latest_version)
            r.raise_for_status()
            latest_version_attr = r.json()
            repo_version_id = latest_version_attr['id']
            if self.verbosity:
                self.log('Latest version ID:', repo_version_id)
        else:
            repo_version_id = version

        # Get the export
        url_ocl_export = self.oclenv + endpoint + repo_version_id + '/export/'
        if self.verbosity:
            self.log('Export URL:', url_ocl_export)
        r = requests.get(url_ocl_export)
        r.raise_for_status()
        if r.status_code == 204:
            self.log('ERROR: Export does not exist for "%s"' % url_ocl_export)
            sys.exit(1)

        # Write tar'd export to file
        with open(self.attach_absolute_path(tarfilename), 'wb') as handle:
            for block in r.iter_content(1024):
                handle.write(block)
        if self.verbosity:
            self.log('%s bytes saved to "%s"' % (r.headers['Content-Length'], tarfilename))

        # Decompress the tar and rename
        tar = tarfile.open(self.attach_absolute_path(tarfilename))
        tar.extractall(self.__location__)
        tar.close()
        os.rename(self.attach_absolute_path('export.json'), self.attach_absolute_path(jsonfilename))
        if self.verbosity:
            self.log('Export decompressed to "%s"' % jsonfilename)

        return True

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
        with open(self.attach_absolute_path(self.new_import_script_filename), 'wb') as output_file:
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
                                self.log('oh shit! havent implemented mappings')
                            elif resource_type == self.RESOURCE_TYPE_CONCEPT_REF and r['type'] == self.RESOURCE_TYPE_REFERENCE:
                                output_file.write(json.dumps(r))
                                output_file.write('\n')
                            elif resource_type == self.RESOURCE_TYPE_MAPPING_REF and r['type'] == self.RESOURCE_TYPE_REFERENCE:
                                self.log('oh shit! havent implemented mapping refs')
                            else:
                                self.log('oh shit! what is this? resource_type:"%s" and resource:{%s}' % (resource_type, str(r)))

                    # Process updated items
                    if 'value_changed' in diff[import_batch][resource_type]:
                        self.log('oh shit! havent implemented value_changed')

                    # Process deleted items
                    if 'dictionary_item_removed' in diff[import_batch][resource_type]:
                        self.log('oh shit! havent implemented dictionary_item_removed')

        if self.verbosity:
            self.log('New import script written to file "%s"' % self.new_import_script_filename)
