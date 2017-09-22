from __future__ import with_statement
import os
import itertools, functools, operator
import requests
import sys
import tarfile
from datetime import datetime
import json
import csv
from xml.etree.ElementTree import Element, SubElement, tostring
from deepdiff import DeepDiff
from requests.auth import HTTPBasicAuth
from json_flex_import import ocl_json_flex_import
from shutil import copyfile


class DatimBase:
    """ Shared base class for DATIM synchronization and presentation """

    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    def log(self, *args):
        ''' Output log information '''
        sys.stdout.write('[' + str(datetime.now()) + '] ')
        for arg in args:
            sys.stdout.write(str(arg))
            sys.stdout.write(' ')
        sys.stdout.write('\n')
        sys.stdout.flush()

    def attachAbsolutePath(self, filename):
        ''' Adds full absolute path to the filename '''
        return os.path.join(self.__location__, filename)

    def getOclRepositories(self, url='', key_field='id', require_external_id=True,
                        active_attr_name='__datim_sync'):
        '''
        Gets repositories from OCL using the provided URL, optionally filtering
        by external_id and a custom attribute indicating active status
        '''
        r = requests.get(url, headers=self.oclapiheaders)
        r.raise_for_status()
        repos = r.json()
        filtered_repos = {}
        for r in repos:
            if (not require_external_id or ('external_id' in r and r['external_id'])) and (not active_attr_name or (r['extras'] and active_attr_name in r['extras'] and r['extras'][active_attr_name])):
                filtered_repos[r[key_field]] = r
        return filtered_repos

    # Perform quick comparison of two files to determine if they have exactly the same size and contents
    def filecmp(self, filename1, filename2):
        ''' Do the two files have exactly the same size and contents? '''
        try:
            with open(filename1, "rb") as fp1, open(filename2, "rb") as fp2:
                if os.fstat(fp1.fileno()).st_size != os.fstat(fp2.fileno()).st_size:
                    return False # different sizes therefore not equal
                fp1_reader = functools.partial(fp1.read, 4096)
                fp2_reader = functools.partial(fp2.read, 4096)
                cmp_pairs = itertools.izip(iter(fp1_reader, ''), iter(fp2_reader, ''))
                inequalities = itertools.starmap(operator.ne, cmp_pairs)
                return not any(inequalities)
        except:
            return False

    def getOclRepositoryVersionExport(self, endpoint='', version='', tarfilename='', jsonfilename=''):
        '''
        Fetches an export of the specified repository version and saves to file.
        Use version="latest" to fetch the most recent released repo version.
        Note that the export must already exist before using this method.
        :param endpoint: endpoint must point to the repo endpoint only, e.g. '/orgs/myorg/sources/mysource/'
        :param version: repo version ID or "latest"
        :param tarfilename: Filename to save the compressed OCL export to
        :param jsonfilename: Filename to save the decompressed OCL-JSON export to
        :return: bool True upon success; False otherwise
        '''

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
        with open(self.attachAbsolutePath(tarfilename), 'wb') as handle:
            for block in r.iter_content(1024):
                handle.write(block)
        if self.verbosity:
            self.log('%s bytes saved to "%s"' % (r.headers['Content-Length'], tarfilename))

        # Decompress the tar and rename
        tar = tarfile.open(self.attachAbsolutePath(tarfilename))
        tar.extractall(self.__location__)
        tar.close()
        os.rename(self.attachAbsolutePath('export.json'), self.attachAbsolutePath(jsonfilename))
        if self.verbosity:
            self.log('Export decompressed to "%s"' % (jsonfilename))

        return True
