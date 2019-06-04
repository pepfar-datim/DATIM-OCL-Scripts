from __future__ import with_statement
import os
import itertools
import functools
import operator
import requests
import sys
import zipfile
import time
import datetime
import json
import settings


class DatimBase(object):
    """ Shared base class for DATIM synchronization and presentation """

    # Resource type constants
    RESOURCE_TYPE_USER = 'User'
    RESOURCE_TYPE_ORGANIZATION = 'Organization'
    RESOURCE_TYPE_SOURCE = 'Source'
    RESOURCE_TYPE_COLLECTION = 'Collection'
    RESOURCE_TYPE_CONCEPT = 'Concept'
    RESOURCE_TYPE_MAPPING = 'Mapping'
    RESOURCE_TYPE_CONCEPT_REF = 'Concept_Ref'
    RESOURCE_TYPE_MAPPING_REF = 'Mapping_Ref'
    RESOURCE_TYPE_REFERENCE = 'Reference'
    RESOURCE_TYPE_SOURCE_VERSION = 'Source Version'
    RESOURCE_TYPE_COLLECTION_VERSION = 'Collection Version'
    DEFAULT_SYNC_RESOURCE_TYPES = [
        RESOURCE_TYPE_CONCEPT,
        RESOURCE_TYPE_MAPPING,
        RESOURCE_TYPE_CONCEPT_REF,
        RESOURCE_TYPE_MAPPING_REF
    ]

    OWNER_STEM_USERS = 'users'
    OWNER_STEM_ORGS = 'orgs'
    REPO_STEM_SOURCES = 'sources'
    REPO_STEM_COLLECTIONS = 'collections'

    DATA_FOLDER_NAME = 'data'

    DATIM_IMAP_OPERATION_ADD = 'ADD OPERATION'
    DATIM_IMAP_OPERATION_ADD_HALF = 'ADD HALF OPERATION'
    DATIM_IMAP_OPERATION_SUBTRACT = 'SUBTRACT OPERATION'
    DATIM_IMAP_OPERATION_SUBTRACT_HALF = 'SUBTRACT HALF OPERATION'
    DATIM_IMAP_OPERATIONS = [
        DATIM_IMAP_OPERATION_ADD,
        DATIM_IMAP_OPERATION_ADD_HALF,
        DATIM_IMAP_OPERATION_SUBTRACT,
        DATIM_IMAP_OPERATION_SUBTRACT_HALF
    ]

    DATIM_IMAP_FORMAT_CSV = 'CSV'
    DATIM_IMAP_FORMAT_JSON = 'JSON'
    DATIM_IMAP_FORMAT_HTML = 'HTML'
    DATIM_IMAP_FORMATS = [
        DATIM_IMAP_FORMAT_CSV,
        DATIM_IMAP_FORMAT_JSON,
        DATIM_IMAP_FORMAT_HTML
    ]

    # TODO: Delete this. It is a duplicate -- see DatimImap.IMAP_FIELD_NAMES
    imap_fields = [
        'DATIM_Indicator_Category',
        'DATIM_Indicator_ID',
        'DATIM_Disag_ID',
        'DATIM_Disag_Name',
        'Operation',
        'MOH_Indicator_ID',
        'MOH_Indicator_Name',
        'MOH_Disag_ID',
        'MOH_Disag_Name',
    ]

    # DATIM MOH Alignment Variables
    datim_owner_id = 'PEPFAR'
    datim_owner_type = 'Organization'
    datim_source_id = 'DATIM-MOH'
    country_owner = 'DATIM-MOH-xx'
    country_owner_type = 'Organization'
    country_source_id = 'DATIM-Alignment-Indicators'
    concept_class_indicator = 'Indicator'
    concept_class_disaggregate = 'Disaggregate'
    map_type_datim_has_option = 'Has Option'
    map_type_country_has_option = 'DATIM HAS OPTION'

    # DATIM Default DISAG
    DATIM_DEFAULT_DISAG_ID = 'HllvX50cXC0'
    DATIM_DEFAULT_DISAG_REPLACEMENT_NAME = 'Total'

    # NULL Disag Attributes
    NULL_DISAG_OWNER_TYPE = 'Organization'
    NULL_DISAG_OWNER_ID = 'PEPFAR'
    NULL_DISAG_SOURCE_ID = 'DATIM-MOH'
    NULL_DISAG_ID = 'null-disag'
    NULL_DISAG_ENDPOINT = '/orgs/PEPFAR/sources/DATIM-MOH/concepts/null-disag/'
    NULL_DISAG_NAME = 'Null Disaggregate'

    # Set the root directory
    if settings and settings.ROOT_DIR:
        __location__ = settings.ROOT_DIR
    else:
        __location__ = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))

    def __init__(self):
        self.verbosity = 1
        self.oclenv = ''
        self.oclapitoken = ''
        self.oclapiheaders = {}
        self.dhis2env = ''
        self.dhis2uid = ''
        self.dhis2pwd = ''
        self.ocl_dataset_repos = None
        self.active_dataset_keys = []
        self.str_active_dataset_ids = ''
        self.run_ocl_offline = False

    def vlog(self, verbose_level=0, *args):
        """ Output log information if verbosity setting is equal or greater than this verbose level """
        if self.verbosity < verbose_level:
            return
        sys.stdout.write('[' + str(datetime.datetime.now()) + '] ')
        for arg in args:
            sys.stdout.write(str(arg))
            sys.stdout.write(' ')
        sys.stdout.write('\n')
        sys.stdout.flush()

    def log(self, *args):
        """ Output log information ignoring verbosity level """
        sys.stdout.write('[' + str(datetime.datetime.now()) + '] ')
        for arg in args:
            sys.stdout.write(str(arg))
            sys.stdout.write(' ')
        sys.stdout.write('\n')
        sys.stdout.flush()

    def _convert_endpoint_to_filename_fmt(self, endpoint):
        filename = endpoint.replace('/', '-')
        if filename[0] == '-':
            filename = filename[1:]
        if filename[-1] == '-':
            filename = filename[:-1]
        return filename

    def endpoint2filename_ocl_export_zip(self, endpoint):
        return 'ocl-' + self._convert_endpoint_to_filename_fmt(endpoint) + '.zip'

    def endpoint2filename_ocl_export_json(self, endpoint):
        return 'ocl-' + self._convert_endpoint_to_filename_fmt(endpoint) + '-raw.json'

    def endpoint2filename_ocl_export_intermediate_json(self, endpoint):
        return 'ocl-' + self._convert_endpoint_to_filename_fmt(endpoint) + '-intermediate.json'

    def endpoint2filename_ocl_export_cleaned(self, endpoint):
        return 'ocl-' + self._convert_endpoint_to_filename_fmt(endpoint) + '-cleaned.json'

    def dhis2filename_export_new(self, dhis2_query_id):
        return 'dhis2-' + dhis2_query_id + '-export-new-raw.json'

    def dhis2filename_export_old(self, dhis2_query_id):
        return 'dhis2-' + dhis2_query_id + '-export-old-raw.json'

    def dhis2filename_export_converted(self, dhis2_query_id):
        return 'dhis2-' + dhis2_query_id + '-export-converted.json'

    def filename_diff_result(self, import_batch_name):
        return '%s-diff-results-%s.json' % (import_batch_name, datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

    def repo_type_to_stem(self, repo_type, default_repo_stem=None):
        """ Get a repo stem (e.g. sources, collections) given a fully specified repo type (e.g. Source, Collection) """
        if repo_type == self.RESOURCE_TYPE_SOURCE:
            return self.REPO_STEM_SOURCES
        elif repo_type == self.RESOURCE_TYPE_COLLECTION:
            return self.REPO_STEM_COLLECTIONS
        else:
            return default_repo_stem

    @staticmethod
    def owner_type_to_stem(owner_type, default_owner_stem=None):
        """
        Get an owner stem (e.g. orgs, users) given a fully specified
        owner type (e.g. Organization, User)
        """
        if owner_type == DatimBase.RESOURCE_TYPE_USER:
            return DatimBase.OWNER_STEM_USERS
        elif owner_type == DatimBase.RESOURCE_TYPE_ORGANIZATION:
            return DatimBase.OWNER_STEM_ORGS
        else:
            return default_owner_stem

    def attach_absolute_path(self, filename):
        """ Adds full absolute path to the filename """
        return os.path.join(self.__location__, filename)

    def attach_absolute_data_path(self, filename):
        """ Adds full absolute path to the filename """
        return os.path.join(self.__location__, self.DATA_FOLDER_NAME, filename)

    def get_ocl_repositories(self, endpoint=None, key_field='id', require_external_id=True,
                             active_attr_name='__datim_sync'):
        """
        Gets repositories from OCL using the provided URL, optionally filtering by external_id and a
        custom attribute indicating active status. Note that only one repository is returned per unique
        value of key_field. Meaning, if key_field='external_id' and more than one repository is returned
        by OCL with the same value for external_id, only one of those repositories will be returned by
        this method.
        """
        filtered_repos = {}
        next_url = self.oclenv + endpoint
        while next_url:
            response = requests.get(next_url, headers=self.oclapiheaders)
            response.raise_for_status()
            repos = response.json()
            for repo in repos:
                if (not require_external_id or ('external_id' in repo and repo['external_id'])) and (
                            not active_attr_name or (repo['extras'] and active_attr_name in repo['extras'] and repo[
                            'extras'][active_attr_name])):
                    filtered_repos[repo[key_field]] = repo
            next_url = ''
            if 'next' in response.headers and response.headers['next'] and response.headers['next'] != 'None':
                next_url = response.headers['next']
        return filtered_repos

    def load_datasets_from_ocl(self):
        """ Fetch the OCL repositories corresponding to the DHIS2 datasets defined in each sync object """

        # Load the datasets using OCL_DATASET_ENDPOINT
        if not self.run_ocl_offline:
            # Fetch the repositories from OCL
            self.vlog(1, 'Request URL:', self.oclenv + self.OCL_DATASET_ENDPOINT)
            self.ocl_dataset_repos = self.get_ocl_repositories(
                endpoint=self.OCL_DATASET_ENDPOINT, key_field='external_id', active_attr_name=self.REPO_ACTIVE_ATTR)
            with open(self.attach_absolute_data_path(self.DATASET_REPOSITORIES_FILENAME), 'wb') as output_file:
                output_file.write(json.dumps(self.ocl_dataset_repos))
            self.vlog(1, 'Repositories retrieved from OCL matching key "%s": %s' % (
                self.REPO_ACTIVE_ATTR, len(self.ocl_dataset_repos)))
            self.vlog(1, 'Repositories cached in memory and successfully written to "%s"' % (
                self.DATASET_REPOSITORIES_FILENAME))
        else:
            # Load the files offline (from a local cache) if they exist
            self.vlog(1, 'OCL-OFFLINE: Loading repositories from "%s"' % self.DATASET_REPOSITORIES_FILENAME)
            with open(self.attach_absolute_data_path(self.DATASET_REPOSITORIES_FILENAME), 'rb') as handle:
                self.ocl_dataset_repos = json.load(handle)
            self.vlog(1, 'OCL-OFFLINE: Repositories successfully loaded:', len(self.ocl_dataset_repos))

        # Extract list of DHIS2 dataset IDs from the repository attributes
        if self.ocl_dataset_repos:
            self.active_dataset_keys = self.ocl_dataset_repos.keys()
            self.str_active_dataset_ids = ','.join(self.ocl_dataset_repos.keys())
            self.vlog(1, 'Dataset IDs returned from OCL:', self.str_active_dataset_ids)
        else:
            msg = 'ERROR: No dataset IDs returned from OCL. Exiting...'
            self.vlog(1, msg)
            raise Exception(msg)

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

    def increment_ocl_versions(self, import_results=None):
        """
        Increment version for OCL repositories that were modified according to the provided import results object
        :param import_results:
        :return:
        """
        dt = datetime.datetime.utcnow()
        cnt = 0
        for ocl_export_key, ocl_export_def in self.OCL_EXPORT_DEFS.iteritems():
            cnt += 1

            # First check if any changes were made to the repository
            str_import_results = ''
            ocl_export_endpoint = self.OCL_EXPORT_DEFS[ocl_export_key]['endpoint']
            if import_results.has(root_key=ocl_export_endpoint, limit_to_success_codes=True):
                str_import_results = import_results.get_detailed_summary(
                    root_key=ocl_export_endpoint, limit_to_success_codes=True)
            else:
                self.vlog(1, '[OCL Export %s of %s] %s: No changes to this repository...' % (
                    cnt, len(self.OCL_EXPORT_DEFS), ocl_export_key))
                continue

            # Prepare to create new version
            new_repo_version_data = {
                'id': 'v' + dt.strftime('%Y-%m-%dT%H%M%S.%f'),
                'description': 'Automatically generated by DATIM-Sync: %s' % str_import_results,
                'released': True,
            }
            repo_version_endpoint = ocl_export_def['endpoint'] + 'versions/'
            new_repo_version_url = self.oclenv + repo_version_endpoint
            self.vlog(1, 'Create new repo version request URL:', new_repo_version_url)
            self.vlog(1, json.dumps(new_repo_version_data))
            r = requests.post(new_repo_version_url,
                              data=json.dumps(new_repo_version_data),
                              headers=self.oclapiheaders)
            r.raise_for_status()
            repo_version_endpoint = str(ocl_export_def['endpoint']) + str(new_repo_version_data['id']) + '/'
            self.vlog(1, '[OCL Export %s of %s] %s: Created new repository version "%s"' % (
                cnt, len(self.OCL_EXPORT_DEFS), ocl_export_key, repo_version_endpoint))

    def get_latest_version_for_period(self, repo_endpoint='', period=''):
        """
        Fetch the latest version of a repository for the specified period
        For example, if period is 'FY17', and 'FY17.v0' and 'FY17.v1' versions have been defined,
        then 'FY17.v1' would be returned.
        """

        repo_versions_endpoint = '/orgs/%s/sources/%s/versions/' % (self.datim_owner_id, self.datim_source_id)
        repo_versions_url = '%s%sversions/?limit=0' % (self.oclenv, repo_endpoint)
        self.vlog(1, 'Fetching latest repository version for period "%s": %s' % (period, repo_versions_url))
        r = requests.get(repo_versions_url, headers=self.oclapiheaders)
        repo_versions = r.json()
        for repo_version in repo_versions:
            if repo_version['released'] == True and len(repo_version['id']) > len(period) and repo_version['id'][:len(period)] == period:
                return repo_version['id']
        return None

    def get_ocl_export(self, endpoint='', version='', zipfilename='', jsonfilename=''):
        """
        Fetches an export of the specified repository version and saves to file.
        Use version="latest" to fetch the most recent released repo version.
        Note that if the export is not already cached, it will attempt to generate the export and
        wait for 30 seconds before trying again. If the export still does not exist, this method will fail.
        :param endpoint: endpoint must point to the repo endpoint only, e.g. '/orgs/myorg/sources/mysource/'
        :param version: repo version ID or "latest"
        :param zipfilename: Filename to save the compressed OCL export to
        :param jsonfilename: Filename to save the decompressed OCL-JSON export to
        :return: bool True upon success; False otherwise
        """
        # Get the latest version of the repo
        if version == 'latest':
            url_latest_version = self.oclenv + endpoint + 'latest/'
            self.vlog(1, 'Latest version request URL:', url_latest_version)
            r = requests.get(url_latest_version, headers=self.oclapiheaders)
            r.raise_for_status()
            latest_version_attr = r.json()
            repo_version_id = latest_version_attr['id']
            self.vlog(1, 'Latest version ID:', repo_version_id)
        else:
            repo_version_id = version

        # Get the export
        url_ocl_export = self.oclenv + endpoint + repo_version_id + '/export/'
        self.vlog(1, 'Export URL:', url_ocl_export)
        r = requests.get(url_ocl_export, headers=self.oclapiheaders)
        r.raise_for_status()
        if r.status_code == 204:
            # Create the export and try one more time...
            self.vlog(1, 'WARNING: Export does not exist for "%s". Creating export...' % url_ocl_export)
            new_export_request = requests.post(url_ocl_export, headers=self.oclapiheaders)
            if new_export_request.status_code == 202:
                # Wait for export to be processed then try to fetch it
                self.vlog(1, 'INFO: Waiting 30 seconds while export is being generated...')
                time.sleep(30)
                r = requests.get(url_ocl_export, headers=self.oclapiheaders)
                r.raise_for_status()
            else:
                msg = 'ERROR: Unable to generate export for "%s"' % url_ocl_export
                self.vlog(1, msg)
                raise Exception(msg)

        # Write compressed export to file
        with open(self.attach_absolute_data_path(zipfilename), 'wb') as handle:
            for block in r.iter_content(1024):
                handle.write(block)
        self.vlog(1, '%s bytes saved to "%s"' % (r.headers['Content-Length'], zipfilename))

        # Decompress the export file and rename
        zipref = zipfile.ZipFile(self.attach_absolute_data_path(zipfilename))
        zipref.extractall(os.path.join(self.__location__, self.DATA_FOLDER_NAME))
        zipref.close()
        os.rename(self.attach_absolute_data_path('export.json'), self.attach_absolute_data_path(jsonfilename))
        self.vlog(1, 'Export decompressed to "%s"' % jsonfilename)

        return True

    def find_nth(self, haystack, needle, n):
        """ Find nth occurence of a substring within a string """
        start = haystack.find(needle)
        while start >= 0 and n > 1:
            start = haystack.find(needle, start+len(needle))
            n -= 1
        return start

    def replace_attr(self, str_input, attributes):
        """
        Replaces attributes in the string designated by double curly brackets with values passed
        in the attributes dictionary
        """
        if attributes:
            for attr_name in attributes:
                str_input = str_input.replace('{{' + attr_name + '}}', attributes[attr_name])
        return str_input
