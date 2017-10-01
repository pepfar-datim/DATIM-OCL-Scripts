from __future__ import with_statement
import os
import itertools
import functools
import operator
import requests
import sys
import tarfile
import time
from datetime import datetime
import json


class DatimBase:
    """ Shared base class for DATIM synchronization and presentation """

    # Resource type constants
    RESOURCE_TYPE_USER = 'User'
    RESOURCE_TYPE_ORGANIZATION = 'Organization'
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
        self.str_active_dataset_ids = ''

    def vlog(self, verbose_level=0, *args):
        """ Output log information if verbosity setting is equal or greater than this verbose level """
        if self.verbosity < verbose_level:
            return
        sys.stdout.write('[' + str(datetime.now()) + '] ')
        for arg in args:
            sys.stdout.write(str(arg))
            sys.stdout.write(' ')
        sys.stdout.write('\n')
        sys.stdout.flush()

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

    def get_ocl_repositories(self, endpoint=None, key_field='id', require_external_id=True,
                             active_attr_name='__datim_sync'):
        """
        Gets repositories from OCL using the provided URL, optionally filtering
        by external_id and a custom attribute indicating active status
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
        # Fetch the repositories from OCL
        if not self.run_ocl_offline:
            self.vlog(1, 'Request URL:', self.oclenv + self.OCL_DATASET_ENDPOINT)
            self.ocl_dataset_repos = self.get_ocl_repositories(endpoint=self.OCL_DATASET_ENDPOINT,
                                                               key_field='external_id',
                                                               active_attr_name=self.REPO_ACTIVE_ATTR)
            with open(self.attach_absolute_path(self.DATASET_REPOSITORIES_FILENAME), 'wb') as output_file:
                output_file.write(json.dumps(self.ocl_dataset_repos))
            self.vlog(1, 'Repositories retrieved from OCL and stored in memory:', len(self.ocl_dataset_repos))
            self.vlog(1, 'Repositories successfully written to "%s"' % self.DATASET_REPOSITORIES_FILENAME)
        else:
            self.vlog(1, 'OCL-OFFLINE: Loading repositories from "%s"' % self.DATASET_REPOSITORIES_FILENAME)
            with open(self.attach_absolute_path(self.DATASET_REPOSITORIES_FILENAME), 'rb') as handle:
                self.ocl_dataset_repos = json.load(handle)
            self.vlog(1, 'OCL-OFFLINE: Repositories successfully loaded:', len(self.ocl_dataset_repos))

        # Extract list of DHIS2 dataset IDs from the repository attributes
        if self.ocl_dataset_repos:
            self.str_active_dataset_ids = ','.join(self.ocl_dataset_repos.keys())
            self.vlog(1, 'Dataset IDs returned from OCL:', self.str_active_dataset_ids)
        else:
            self.log('ERROR: No dataset IDs returned from OCL. Exiting...')
            sys.exit(1)

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
        dt = datetime.utcnow()
        for ocl_export_key, ocl_export_def in self.OCL_EXPORT_DEFS.iteritems():
            # First check if any changes were made to the repository
            str_import_results = ''
            ocl_export_endpoint = self.OCL_EXPORT_DEFS[ocl_export_key]['endpoint']
            if ocl_export_endpoint in import_results:
                for action_type in import_results[ocl_export_endpoint]:
                    str_import_results += '(%s) %s,' % (
                        len(import_results[ocl_export_endpoint][action_type]), action_type)
            else:
                self.vlog(1, '%s: No changes to this repository...' % ocl_export_key)
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
            self.vlog(1, '%s: Created new repository version "%s"' % (ocl_export_key, repo_version_endpoint))

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
        if version == 'latest':
            url_latest_version = self.oclenv + endpoint + 'latest/'
            self.vlog(1, 'Latest version request URL:', url_latest_version)
            r = requests.get(url_latest_version)
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
            self.log('WARNING: Export does not exist for "%s". Creating export...' % url_ocl_export)
            new_export_request = requests.post(url_ocl_export, headers=self.oclapiheaders)
            if new_export_request.status_code == 202:
                # Wait for export to be processed then try to fetch it
                time.sleep(5)
                r = requests.get(url_ocl_export, headers=self.oclapiheaders)
                r.raise_for_status()
            else:
                self.log('ERROR: Unable to generate export for "%s"' % url_ocl_export)
                sys.exit(1)

        # Write tar'd export to file
        with open(self.attach_absolute_path(tarfilename), 'wb') as handle:
            for block in r.iter_content(1024):
                handle.write(block)
        self.vlog(1, '%s bytes saved to "%s"' % (r.headers['Content-Length'], tarfilename))

        # Decompress the tar and rename
        tar = tarfile.open(self.attach_absolute_path(tarfilename))
        tar.extractall(self.__location__)
        tar.close()
        os.rename(self.attach_absolute_path('export.json'), self.attach_absolute_path(jsonfilename))
        self.vlog(1, 'Export decompressed to "%s"' % jsonfilename)

        return True

