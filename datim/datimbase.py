"""
Base class providing common functionality for DATIM indicator and
country mapping synchronization and presentation.
"""
from __future__ import with_statement
import os
import itertools
import functools
import operator
import sys
import zipfile
import time
import datetime
import json
from StringIO import StringIO
import requests
import grequests
import settings
import ocldev.oclconstants
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class DatimBase(object):
    """ Shared base class for DATIM synchronization and presentation """

    # Resource type constants
    # TODO: Grab these from ocldev constants instead of redefining here
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
    OWNER_STEM_USERS = 'users'
    OWNER_STEM_ORGS = 'orgs'
    REPO_STEM_SOURCES = 'sources'
    REPO_STEM_COLLECTIONS = 'collections'

    # Default list of resource types evaluated by the sync scripts
    DEFAULT_SYNC_RESOURCE_TYPES = [
        RESOURCE_TYPE_CONCEPT,
        RESOURCE_TYPE_MAPPING,
        RESOURCE_TYPE_CONCEPT_REF,
        RESOURCE_TYPE_MAPPING_REF
    ]

    # DATIM IMAP Operations
    DATIM_IMAP_OPERATION_ADD = 'ADD OPERATION'
    DATIM_IMAP_OPERATION_ADD_HALF = 'ADD HALF OPERATION'  # Not used
    DATIM_IMAP_OPERATION_SUBTRACT = 'SUBTRACT OPERATION'
    DATIM_IMAP_OPERATION_SUBTRACT_HALF = 'SUBTRACT HALF OPERATION'  # Not used
    DATIM_IMAP_OPERATIONS = [
        DATIM_IMAP_OPERATION_ADD,
        DATIM_IMAP_OPERATION_ADD_HALF,
        DATIM_IMAP_OPERATION_SUBTRACT,
        DATIM_IMAP_OPERATION_SUBTRACT_HALF
    ]

    # DATIM IMAP supported format types for exports
    # TODO: These are duplicated in DatimImap -- delete this one!
    DATIM_IMAP_FORMAT_CSV = 'CSV'
    DATIM_IMAP_FORMAT_JSON = 'JSON'
    DATIM_IMAP_FORMAT_HTML = 'HTML'
    DATIM_IMAP_FORMATS = [
        DATIM_IMAP_FORMAT_CSV,
        DATIM_IMAP_FORMAT_JSON,
        DATIM_IMAP_FORMAT_HTML
    ]

    # DATIM MOH Alignment Constants (e.g. /orgs/PEPFAR/sources/DATIM-MOH-FY18)
    DATIM_MOH_OWNER_ID = 'PEPFAR'
    DATIM_MOH_OWNER_TYPE = 'Organization'
    DATIM_MOH_SOURCE_ID_BASE = 'DATIM-MOH'  # Fiscal year is appended to this, e.g. 'DATIM-MOH-FY18'

    # DATIM MOH Country Constants (e.g. /orgs/DATIM-MOH-RW-FY18/sources/DATIM-Alignment-Indicators/)
    DATIM_MOH_COUNTRY_OWNER = 'DATIM-MOH-xx'  # xx replaced by country code & FY, eg RW-FY18
    DATIM_MOH_COUNTRY_OWNER_TYPE = 'Organization'
    DATIM_MOH_COUNTRY_SOURCE_ID = 'DATIM-Alignment-Indicators'
    DATIM_MOH_CONCEPT_CLASS_DE = 'Data Element'
    DATIM_MOH_DATATYPE_DE = 'Numeric'
    DATIM_MOH_CONCEPT_CLASS_DISAGGREGATE = 'Disaggregate'
    DATIM_MOH_DATATYPE_DISAGGREGATE = 'None'
    DATIM_MOH_MAP_TYPE_HAS_OPTION = 'Has Option'
    DATIM_MOH_MAP_TYPE_COUNTRY_OPTION = 'DATIM HAS OPTION'

    # DATIM-MOH NULL Disag ID and Name (used only by DATIM-MOH)
    NULL_DISAG_ID = 'null-disag'
    NULL_DISAG_NAME = 'Null Disaggregate'

    # DATIM-MOH Default/Total disag values (used only by DATIM-MOH for auto-replacement)
    DATIM_DEFAULT_DISAG_ID = 'HllvX50cXC0'
    DATIM_DEFAULT_DISAG_REPLACEMENT_NAME = 'Total'

    # Location to save temporary data files
    # NOTE: File system permissions must be set for this project to read/write from this subfolder
    DATA_SUBFOLDER_NAME = 'data'

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
        self.datim_moh_source_id = ''

    def vlog(self, verbose_level=0, *args):
        """
        Output log information if verbosity setting is equal or greater than this verbose level
        """
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

    @staticmethod
    def _convert_endpoint_to_filename_fmt(endpoint):
        filename = endpoint.replace('/', '-')
        if filename[0] == '-':
            filename = filename[1:]
        if filename[-1] == '-':
            filename = filename[:-1]
        return filename

    @staticmethod
    def endpoint2filename_ocl_export_zip(endpoint):
        return 'ocl-%s.zip' % DatimBase._convert_endpoint_to_filename_fmt(endpoint)

    @staticmethod
    def endpoint2filename_ocl_export_json(endpoint):
        return 'ocl-%s-raw.json' % DatimBase._convert_endpoint_to_filename_fmt(endpoint)

    @staticmethod
    def endpoint2filename_ocl_export_intermediate_json(endpoint):
        return 'ocl-%s-intermediate.json' % DatimBase._convert_endpoint_to_filename_fmt(endpoint)

    @staticmethod
    def endpoint2filename_ocl_export_cleaned(endpoint):
        return 'ocl-%s-cleaned.json' % DatimBase._convert_endpoint_to_filename_fmt(endpoint)

    @staticmethod
    def dhis2filename_export_new(dhis2_query_id):
        return 'dhis2-%s-export-new-raw.json' % dhis2_query_id

    @staticmethod
    def dhis2filename_export_old(dhis2_query_id):
        return 'dhis2-%s-export-old-raw.json' % dhis2_query_id

    @staticmethod
    def dhis2filename_export_converted(dhis2_query_id):
        return 'dhis2-%s-export-converted.json' % dhis2_query_id

    @staticmethod
    def filename_diff_result(import_batch_name):
        return '%s-diff-results-%s.json' % (
            import_batch_name, datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

    @staticmethod
    def repo_type_to_stem(repo_type, default_repo_stem=None):
        """
        Get a repo stem (e.g. sources, collections) given a fully specified repo type
        (e.g. Source, Collection)
        """
        if repo_type == ocldev.oclconstants.OclConstants.RESOURCE_TYPE_SOURCE:
            return ocldev.oclconstants.OclConstants.REPO_STEM_SOURCES
        elif repo_type == ocldev.oclconstants.OclConstants.RESOURCE_TYPE_COLLECTION:
            return ocldev.oclconstants.OclConstants.REPO_STEM_COLLECTIONS
        return default_repo_stem

    @staticmethod
    def owner_type_to_stem(owner_type, default_owner_stem=None):
        """
        Get an owner stem (e.g. orgs, users) given a fully specified
        owner type (e.g. Organization, User)
        """
        if owner_type == ocldev.oclconstants.OclConstants.RESOURCE_TYPE_USER:
            return ocldev.oclconstants.OclConstants.OWNER_STEM_USERS
        elif owner_type == ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION:
            return ocldev.oclconstants.OclConstants.OWNER_STEM_ORGS
        return default_owner_stem

    def attach_absolute_path(self, filename):
        """ Adds full absolute root path to the filename """
        return os.path.join(self.__location__, filename)

    def attach_absolute_data_path(self, filename):
        """ Adds full absolute path to the filename """
        return os.path.join(self.__location__, self.DATA_SUBFOLDER_NAME, filename)

    def does_offline_data_file_exist(self, filename, exit_if_missing=True):
        """
        Check if data file exists. Optionally exit with error if missing.
        :param filename:
        :param exit_if_missing:
        :return:
        """
        self.vlog(1, 'INFO: Offline mode: Checking for local file "%s"...' % filename)
        if os.path.isfile(self.attach_absolute_data_path(filename)):
            self.vlog(1, 'INFO: Offline mode: "%s" found containing %s bytes. Continuing...' % (
                filename, os.path.getsize(self.attach_absolute_data_path(filename))))
            return True
        elif exit_if_missing:
            msg = 'ERROR: Offline mode: Could not find offline data file "%s"' % filename
            self.vlog(1, msg)
            raise Exception(msg)
        return False

    def get_ocl_repositories(self, endpoint=None, key_field='id', require_external_id=True,
                             active_attr_name='__datim_sync', limit=110):
        """
        Gets repositories from OCL using the provided URL, optionally filtering by
        external_id and a custom attribute indicating active status. Note that only
        one repository is returned per unique value of key_field. Meaning, if
        key_field='external_id' and more than one repository is returned by OCL
        with the same value for external_id, only one of those repositories will
        be returned by this method.
        """
        filtered_repos = {}
        next_url = self.oclenv + endpoint
        while next_url:
            response = requests.get(
                next_url, headers=self.oclapiheaders, params={"limit": str(limit)})
            self.vlog(2, "Fetching repositories for '%s' from OCL: %s" % (endpoint, response.url))
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
        """
        Fetch the OCL repositories corresponding to the DHIS2 datasets defined in each sync object
        """
        # TODO: Refactor so that object references are valid

        # Load the datasets using OCL_DATASET_ENDPOINT
        if not self.run_ocl_offline:
            # Fetch the repositories from OCL
            self.vlog(1, 'Request URL:', self.oclenv + self.OCL_DATASET_ENDPOINT)
            self.ocl_dataset_repos = self.get_ocl_repositories(
                endpoint=self.OCL_DATASET_ENDPOINT,
                key_field='external_id',
                active_attr_name=self.REPO_ACTIVE_ATTR)
            with open(self.attach_absolute_data_path(self.DATASET_REPOSITORIES_FILENAME), 'wb') as output_file:
                output_file.write(json.dumps(self.ocl_dataset_repos))
            self.vlog(1, 'Repositories retrieved from OCL matching key "%s": %s' % (
                self.REPO_ACTIVE_ATTR, len(self.ocl_dataset_repos)))
            self.vlog(1, 'Repositories cached in memory and successfully written to "%s"' % (
                self.DATASET_REPOSITORIES_FILENAME))
        else:
            # Load the files offline (from a local cache) if they exist
            self.vlog(1, 'OCL-OFFLINE: Loading repositories from "%s"' % (
                self.DATASET_REPOSITORIES_FILENAME))
            with open(self.attach_absolute_data_path(self.DATASET_REPOSITORIES_FILENAME), 'rb') as handle:
                self.ocl_dataset_repos = json.load(handle)
            self.vlog(
                1, 'OCL-OFFLINE: Repositories successfully loaded:', len(self.ocl_dataset_repos))

        # Extract list of DHIS2 dataset IDs from the repository attributes
        if self.ocl_dataset_repos:
            self.active_dataset_keys = self.ocl_dataset_repos.keys()
            self.str_active_dataset_ids = ','.join(self.ocl_dataset_repos.keys())
            self.vlog(1, 'Dataset IDs returned from OCL:', self.str_active_dataset_ids)
        else:
            msg = 'ERROR: No dataset IDs returned from OCL. Exiting...'
            self.vlog(1, msg)
            raise Exception(msg)

    @staticmethod
    def filecmp(filename1, filename2):
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
        Increment version for OCL repositories that were modified according to the provided
        import results object
        TODO: Refactor so that object references are valid
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

            # Create new version
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

    def get_ocl_exports_async(self, endpoint='', period='', version=''):
        """
        Retrieves all matching exports at the specified 'collections' or 'sources' endpoint
        :param endpoint: e.g. /orgs/DATIM-MOH-UA-FY19/collections/
        :param period: e.g. FY18, FY19
        :param version: Required, and does not support "latest" (e.g. v2, v3)
        :return: <dict> repository_version_url: repository_version_export
        """

        # Generate list of all collection export URLs for the org
        country_version_id = '%s.%s' % (period, version)
        country_collections = self.get_ocl_repositories(
            endpoint=endpoint, require_external_id=False, active_attr_name='')
        self.vlog(1, '%s repositories returned for endpoint "%s"' % (
            len(country_collections), endpoint))
        export_urls = []
        for collection_id, collection in country_collections.items():
            url_ocl_export = '%s%s%s/export/' % (self.oclenv, collection['url'], country_version_id)
            self.vlog(1, 'Export URL:', url_ocl_export)
            export_urls.append(url_ocl_export)

        # Define exception handler for async requests
        def export_exception_handler(request, exception):
            print('Request failed:', str(request), str(exception))

        # Submit sync export requests with auto-retry in case of connection pooling errors
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.2)
        s.mount('http://', HTTPAdapter(max_retries=retries))
        s.mount('https://', HTTPAdapter(max_retries=retries))
        export_rs = (
            grequests.get(url, headers=self.oclapiheaders, session=s) for url in export_urls)
        export_responses = grequests.map(
            export_rs, size=2, exception_handler=export_exception_handler)
        self.vlog(1, 'Results of async query:\n%s' % export_responses)

        # Process collection export results
        collection_results = {}
        for export_response in export_responses:
            if export_response is None:
                self.vlog(1, 'WARNING: Export value is None')
                continue
            original_export_url = export_response.url
            if export_response.history and export_response.history[0] and export_response.history[0].url:
                original_export_url = export_response.history[0].url
            if export_response.status_code == 404:
                # Repository version does not exist, so we can safely skip this one
                self.vlog(1, '[%s NOT FOUND] %s -- Current IMAP %s has no mapping for this data element, so we can safely skip' % (
                    export_response.status_code, export_response.url, country_version_id))
                continue
            elif export_response.status_code == 204:
                # Export not cached for this repository version, so we need to generate it first
                self.vlog(1, '[%s MISSING EXPORT] %s -- Export not yet cached. Generating...' % (
                    export_response.status_code, export_response.url))
                export_response = self.generate_repository_version_export(original_export_url)
            else:
                export_response.raise_for_status()

            if export_response.status_code == 200:
                # Cached export successfully retrieved for this repository version
                self.vlog(2, '[%s FOUND] %s' % (export_response.status_code, original_export_url))
                export_string_handle = StringIO(export_response.content)
                zipref = zipfile.ZipFile(export_string_handle, "r")
                if 'export.json' in zipref.namelist():
                    collection_results[original_export_url] = json.loads(zipref.read('export.json'))
                    zipref.close()
                else:
                    zipref.close()
                    errmsg = 'ERROR: Invalid export for "%s": export.json not found.' % (
                        original_export_url)
                    self.vlog(1, errmsg)
                    raise Exception(errmsg)
        self.vlog(1, '%s repository exports for version "%s" retrieved at endpoint "%s"' % (
            len(collection_results), country_version_id, endpoint))
        return collection_results

    def get_ocl_export(self, endpoint='', version='', zipfilename='', jsonfilename='',
                       delay_seconds=10, max_wait_seconds=120):
        """
        Fetches an export of the specified repository version and saves to file.
        Use version="latest" to fetch the most recent released repo version.
        Note that if the export is not already cached, it will attempt to generate
        the export and wait for 30 seconds before trying again. If the export still
        does not exist, this method will fail.
        :param endpoint: endpoint for repo only, e.g. '/orgs/myorg/sources/mysource/'
        :param version: repo version ID or "latest"
        :param zipfilename: Filename to save the compressed OCL export to
        :param jsonfilename: Filename to save the decompressed OCL-JSON export to
        :return: bool True upon success; False otherwise
        """
        # Get the latest version of the repo
        if version == 'latest':
            url_latest_version = self.oclenv + endpoint + 'latest/'
            self.vlog(1, 'Latest version request URL:', url_latest_version)
            response = requests.get(url_latest_version, headers=self.oclapiheaders)
            response.raise_for_status()
            latest_version_attr = response.json()
            repo_version_id = latest_version_attr['id']
            self.vlog(1, 'Latest version ID:', repo_version_id)
        else:
            repo_version_id = version

        # Get the export
        url_ocl_export = self.oclenv + endpoint + repo_version_id + '/export/'
        self.vlog(1, 'Export URL:', url_ocl_export)
        r = requests.get(url_ocl_export, headers=self.oclapiheaders)
        r.raise_for_status()
        if r.status_code == 200:
            # Export successfully retrieved
            pass
        elif r.status_code == 204:
            # Export does not exist, so let's attempt to generate the export and retrieve it...
            self.vlog(
                1, 'WARNING: Export does not exist for "%s". Creating export...' % url_ocl_export)
            r = self.generate_repository_version_export(
                repo_export_url=url_ocl_export, delay_seconds=delay_seconds,
                max_wait_seconds=max_wait_seconds)
        else:
            msg = 'ERROR: Unrecognized response from OCL: %s' % str(r.status_code)
            self.vlog(1, msg)
            raise Exception(msg)

        # Write compressed export to file
        with open(self.attach_absolute_data_path(zipfilename), 'wb') as handle:
            for block in r.iter_content(1024):
                handle.write(block)
        self.vlog(1, '%s bytes saved to "%s"' % (r.headers['Content-Length'], zipfilename))

        # Decompress the export file and rename
        zipref = zipfile.ZipFile(self.attach_absolute_data_path(zipfilename))
        zipref.extractall(os.path.join(self.__location__, self.DATA_SUBFOLDER_NAME))
        zipref.close()
        os.rename(self.attach_absolute_data_path('export.json'),
                  self.attach_absolute_data_path(jsonfilename))
        self.vlog(1, 'Export decompressed to "%s"' % jsonfilename)

        return True

    def generate_repository_version_export(self, repo_export_url, do_wait_until_cached=True,
                                           delay_seconds=10, max_wait_seconds=120):
        """
        Generate a cached repository version export in OCL and optionally wait until processing
        of the export is completed. Returns True if the request is submitted successfully and
        "do_wait_until_cached"==False. Returns the Response object with the cached export if
        "do_wait_until_cached"==True. Otherwise fails with exception.
        :param repo_export_url:
        :param do_wait_until_cached:
        :param delay_seconds:
        :param max_wait_seconds:
        :return: <Response>
        """
        # Make the initial request
        request_create_export = requests.post(
            repo_export_url, headers=self.oclapiheaders, allow_redirects=True)
        do_delay_on_first_loop = True
        if request_create_export.status_code == 409:
            # 409 conflict means that repo export is already being processed, so go ahead
            if not do_wait_until_cached:
                self.vlog(1, 'INFO: Unable to generate export: 409 conflict: %s.' % repo_export_url)
                return None
            else:
                self.vlog(1, 'INFO: Repository export already processing: %s.' % repo_export_url)
        elif request_create_export.status_code == 202:
            self.vlog(1, 'INFO: Generating repository export: %s.' % repo_export_url)
            do_delay_on_first_loop = False
        elif request_create_export.status_code == 303:
            self.vlog(1, 'INFO: Repository export already exists: %s.' % repo_export_url)
            do_delay_on_first_loop = False
        else:
            msg = 'ERROR: %s error generating export for "%s"' % (
                request_create_export.status_code, repo_export_url)
            self.vlog(1, msg)
            raise Exception(msg)

        # Optionally wait for export to be processed and then retrieve it
        if do_wait_until_cached:
            start_time = time.time()
            while time.time() - start_time + delay_seconds < max_wait_seconds:
                self.vlog(1, 'INFO: Delaying %s seconds while export is being generated...' % str(
                    delay_seconds))
                if do_delay_on_first_loop:
                    time.sleep(delay_seconds)
                    do_delay_on_first_loop = False
                r = requests.get(repo_export_url, headers=self.oclapiheaders)
                r.raise_for_status()
                if r.status_code == 200:
                    return r
                elif r.status_code == 204:
                    continue
                else:
                    msg = 'ERROR: %s error generating export for "%s"' % (
                        request_create_export.status_code, repo_export_url)
                    self.vlog(1, msg)
                    raise Exception(msg)
            msg = 'ERROR: Export taking too long to process. Exiting...'
            self.vlog(1, msg)
            raise Exception(msg)
        else:
            # Otherwise just return True that the request was successfully submitted
            return True

    @staticmethod
    def find_nth(haystack, needle, n):
        """ Find nth occurrence of a substring within a string """
        start = haystack.find(needle)
        while start >= 0 and n > 1:
            start = haystack.find(needle, start+len(needle))
            n -= 1
        return start

    @staticmethod
    def replace_attr(str_input, attributes):
        """
        Replaces attributes in the string designated by double curly brackets with values passed
        in the attributes dictionary
        """
        if attributes:
            for attr_name in attributes:
                str_input = str_input.replace('{{' + attr_name + '}}', attributes[attr_name])
        return str_input

    def get_latest_version_for_period(self, repo_endpoint='', period=''):
        """
        For DATIM-MOH, fetch the latest version (including subversion) of a repository
        for the specified period. For example, if period is 'FY17', and 'FY17.v0' and
        'FY17.v1' versions have been defined in OCL, then 'FY17.v1' would be returned.
        Note that this method requires that OCL return the versions ordered by
        date_created descending.
        """
        repo_versions_url = '%s%sversions/?limit=0' % (self.oclenv, repo_endpoint)
        self.vlog(1, 'Fetching latest repository version for period "%s": %s' % (
            period, repo_versions_url))
        r = requests.get(repo_versions_url, headers=self.oclapiheaders)
        repo_versions = r.json()
        for repo_version in repo_versions:
            if repo_version['id'] == 'HEAD' or repo_version['released'] is not True:
                continue
            if (len(repo_version['id']) > len(period) and
                    repo_version['id'][:len(period)] == period):
                return repo_version['id']
        return None

    @staticmethod
    def get_datim_moh_source_id(period):
        """
        Get the DATIM-MOH source ID given a period (e.g. DATIM-MOH-FY18)
        :param period:
        :return:
        """
        return '%s-%s' % (DatimBase.DATIM_MOH_SOURCE_ID_BASE, period)

    @staticmethod
    def get_datim_moh_source_endpoint(period):
        """
        Get the DATIM-MOH source endpoint for a period (eg /orgs/PEPFAR/sources/DATIM-MOH-FY18/)
        :param period:
        :return:
        """
        return '/%s/%s/sources/%s/' % (
            DatimBase.owner_type_to_stem(DatimBase.DATIM_MOH_OWNER_TYPE),
            DatimBase.DATIM_MOH_OWNER_ID, DatimBase.get_datim_moh_source_id(period))

    @staticmethod
    def get_datim_moh_null_disag_endpoint(period):
        """
        Get the DATIM-MOH null disag endpoint for the given period (e.g.
        /orgs/PEPFAR/sources/DATIM-MOH-FY18/concepts/null_disag/)
        :param period:
        :return:
        """
        return '%sconcepts/%s/' % (
            DatimBase.get_datim_moh_source_endpoint(period), DatimBase.NULL_DISAG_ID)
