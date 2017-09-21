from __future__ import with_statement
import os
import itertools, functools, operator
import requests
import sys
import json
import tarfile
from pprint import pprint
from deepdiff import DeepDiff
from requests.auth import HTTPBasicAuth
from json_flex_import import ocl_json_flex_import
from shutil import copyfile
from datetime import datetime


class DatimSimsSync:
    ''' Class to manage DATIM SIMS Synchronization '''

    # URLs
    url_sims_filtered_endpoint = '/orgs/PEPFAR/collections/?q=SIMS&verbose=true'

    # Filenames
    old_dhis2_export_filename = 'old_sims_export.json'
    new_dhis2_export_filename = 'new_sims_export.json'
    converted_dhis2_export_filename = 'converted_sims_export.json'
    new_import_script_filename = 'sims_import_script.json'
    sims_collections_filename = 'ocl_sims_collections.json'

    # OCL Export Definitions
    ocl_export_defs = {
        'sims_source': {
            'endpoint': '/orgs/PEPFAR/sources/SIMS/',
            'tarfilename': 'sims_source_ocl_export.tar',
            'jsonfilename': 'sims_source_ocl_export.json',
            'jsoncleanfilename': 'sims_source_ocl_export_clean.json',
        }
    }

    sims_concept_fields_to_remove = ['version_created_by', 'created_on', 'updated_on',
                                     'version_created_on', 'created_by', 'updated_by', 'display_name',
                                     'display_locale', 'uuid', 'version', 'owner_url', 'source_url',
                                     'mappings', 'url', 'version_url', 'is_latest_version', 'locale']
    sims_mapping_fields_to_remove = []
    sims_name_fields_to_remove = ['uuid', 'type']

    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

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

        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

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

    def getRepositories(self, url='', key_field='id', require_external_id=True,
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

    def saveDhis2SimsAssessmentTypesToFile(self, str_active_dataset_ids='', outputfile=''):
        url_dhis2_export = self.dhis2env + 'api/dataElements.json?fields=name,code,id,valueType,lastUpdated,dataElementGroups[id,name]&order=code:asc&paging=false&filter=dataElementGroups.id:in:[' + str_active_dataset_ids + ']'
        if self.verbosity:
            self.log('DHIS2 SIMS Assessment Types Request URL:', url_dhis2_export)
        r = requests.get(url_dhis2_export, auth=HTTPBasicAuth(self.dhis2uid, self.dhis2pwd))
        r.raise_for_status()
        with open(self.attachAbsolutePath(outputfile), 'wb') as handle:
            for block in r.iter_content(1024):
                handle.write(block)
        return r.headers['Content-Length']

    def dhis2oj_sims(self, inputfile='', outputfile='', sims_collections=None):
        ''' Transform each DataElement to an OCL concept and each DataElementGroup to an OCL reference '''
        with open(self.attachAbsolutePath(inputfile), "rb") as ifile,\
                open(self.attachAbsolutePath(outputfile), 'wb') as ofile:
            new_sims = json.load(ifile)
            num_concepts = 0
            num_references = 0
            output = {}

            # Iterate through each DataElement and transform to an OCL-JSON concept
            for de in new_sims['dataElements']:
                concept_id = de['code']
                url = '/orgs/PEPFAR/sources/SIMS/concepts/' + concept_id + '/'
                c = {
                    'type':'Concept',
                    'id':concept_id,
                    'concept_class':'Assessment Type',
                    'datatype':'None',
                    'owner':'PEPFAR',
                    'owner_type':'Organization',
                    'source':'SIMS',
                    'retired':False,
                    'descriptions':None,
                    'external_id':de['id'],
                    'names':[
                        {
                            'name':de['name'],
                            'name_type':'Fully Specified',
                            'locale':'en',
                            'locale_preferred':False,
                            'external_id':None,
                        }
                    ],
                    'extras':{'Value Type':de['valueType']}
                }
                output[url] = c
                num_concepts += 1

                # Iterate through each DataElementGroup and transform to an OCL-JSON reference
                for deg in de['dataElementGroups']:
                    collection_id = sims_collections[deg['id']]['id']
                    target_url = '/orgs/PEPFAR/sources/SIMS/concepts/' + concept_id + '/'
                    url = '/orgs/PEPFAR/collections/' + collection_id + '/references/?target=' + target_url
                    r = {
                        'type':'Reference',
                        'owner':'PEPFAR',
                        'owner_type':'Organization',
                        'collection':collection_id,
                        'data':{"expressions": ['/orgs/PEPFAR/sources/SIMS/concepts/' + concept_id + '/']}
                    }
                    output[url] = r
                    num_references += 1
            ofile.write(json.dumps(output))

        if self.verbosity:
            self.log('DHIS2 export successfully transformed and saved to "%s": %s concepts + %s references = %s total' % (outputfile, num_concepts, num_references, num_concepts + num_references))
        return True

    # endpoint must point to the repo endpoint only, e.g. '/orgs/myorg/sources/mysource/'
    # Note that atleast version of the repo must be released and the export for that version already created
    # Todo: Still needs to implement OCL authentication
    def getOclLatestExport(self, endpoint='', tarfilename='', jsonfilename=''):
        '''
        Fetches the latest released export of the specified repository and saves to file
        :param endpoint: endpoint must point to the repo endpoint only, e.g. '/orgs/myorg/sources/mysource/'
        :param tarfilename: Filename to save the compressed OCL export to
        :param jsonfilename: Filename to save the decompressed OCL-JSON export to
        :return: bool True upon success; False otherwise
        '''

        # Get the latest version of the repo
        # TODO: error handling for case when no repo version is returned
        url_latest_version = self.oclenv + endpoint + 'latest/'
        if self.verbosity:
            self.log('Latest version request URL:', url_latest_version)
        r = requests.get(url_latest_version)
        r.raise_for_status()
        latest_version_attr = r.json()
        if self.verbosity:
            self.log('Latest version ID:', latest_version_attr['id'])

        # Get the export
        url_ocl_export = self.oclenv + endpoint + latest_version_attr['id'] + '/export/'
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

    def logSettings(self):
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
        """
        Runs the entire synchronization process --
        Recommend breaking this into smaller methods in the future
        """
        if self.verbosity: self.logSettings()

        # STEP 1: Fetch OCL Collections for SIMS Assessment Types
        # Collections that have 'SIMS' in the name, __datim_sync==true, and external_id not empty
        if self.verbosity:
            self.log('**** STEP 1 of 13: Fetch OCL Collections for SIMS Assessment Types')
        if not self.runoffline:
            url_sims_collections = self.oclenv + self.url_sims_filtered_endpoint
            if self.verbosity:
                self.log('Request URL:', url_sims_collections)
            sims_collections = self.getRepositories(url=url_sims_collections, key_field='external_id')
            with open(self.attachAbsolutePath(self.sims_collections_filename), 'wb') as ofile:
                ofile.write(json.dumps(sims_collections))
            if self.verbosity:
                self.log('Repositories retreived from OCL and stored in memory:', len(sims_collections))
                self.log('Repositories successfully written to "%s"' % (self.sims_collections_filename))
        else:
            if self.verbosity:
                self.log('OFFLINE: Loading repositories from "%s"' % (self.sims_collections_filename))
            with open(self.attachAbsolutePath(self.sims_collections_filename), 'rb') as handle:
                sims_collections = json.load(handle)
            if self.verbosity:
                self.log('OFFLINE: Repositories successfully loaded:', len(sims_collections))

        # STEP 2: Extract list of DHIS2 dataset IDs from collection external_id
        if self.verbosity:
            self.log('**** STEP 2 of 13: Extract list of DHIS2 dataset IDs from collection external_id')
        str_active_dataset_ids = ','.join(sims_collections.keys())
        if self.verbosity:
            self.log('SIMS Assessment Type Dataset IDs:', str_active_dataset_ids)

        # STEP 3: Fetch new export from DATIM DHIS2
        if verbosity:
            self.log('**** STEP 3 of 13: Fetch new export from DATIM DHIS2')
        if not runoffline:
            content_length = self.saveDhis2SimsAssessmentTypesToFile(
                str_active_dataset_ids=str_active_dataset_ids, outputfile=self.new_dhis2_export_filename)
            if verbosity:
                self.log('%s bytes retrieved from DHIS2 and written to file "%s"' % (
                content_length, self.new_dhis2_export_filename))
        else:
            if verbosity:
                self.log('OFFLINE: Using local file: "%s"' % (self.new_dhis2_export_filename))
            if os.path.isfile(self.attachAbsolutePath(self.new_dhis2_export_filename)):
                if verbosity:
                    self.log('OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                    self.new_dhis2_export_filename, os.path.getsize(self.attachAbsolutePath(self.new_dhis2_export_filename))))
            else:
                self.log('Could not find offline file "%s". Exiting...' % (self.new_dhis2_export_filename))
                sys.exit(1)

        # STEP 4: Quick comparison of current and previous DHIS2 exports
        # Compares new DHIS2 export to most recent previous export from a successful sync that is available
        # NOTE: This section should be skipped if doing the OCL/DHIS2 data validation check
        if self.verbosity:
            self.log('**** STEP 4 of 13: Quick comparison of current and previous DHIS2 exports')
        if not self.compare2previousexport:
            if self.verbosity:
                self.log("Skipping (due to settings)...")
        elif not self.old_dhis2_export_filename:
            if self.verbosity:
                self.log("Skipping (no previous export filename provided)...")
        else:
            if self.filecmp(self.attachAbsolutePath(self.old_dhis2_export_filename),
                            self.attachAbsolutePath(self.new_dhis2_export_filename)):
                self.log("Current and previous exports are identical, so exit without doing anything...")
                sys.exit()
            else:
                self.log("Current and previous exports are different in size and/or content, so continue...")

        # STEP 5: Transform new DHIS2 export to OCL-formatted JSON (OJ)
        if self.verbosity:
            self.log('**** STEP 5 of 13: Transform DHIS2 export to OCL formatted JSON')
        self.dhis2oj_sims(inputfile=self.new_dhis2_export_filename,
                          outputfile=self.converted_dhis2_export_filename,
                          sims_collections=sims_collections)

        # STEP 6: Fetch latest versions of relevant OCL exports
        if self.verbosity:
            self.log('**** STEP 6 of 13: Fetch latest versions of relevant OCL exports')
        for ocl_export_def_key in self.ocl_export_defs:
            if self.verbosity:
                self.log('%s:' % (ocl_export_def_key))
            export_def = self.ocl_export_defs[ocl_export_def_key]
            if not self.runoffline:
                self.getOclLatestExport(endpoint=export_def['endpoint'],
                                        tarfilename=export_def['tarfilename'],
                                        jsonfilename=export_def['jsonfilename'])
            else:
                if self.verbosity:
                    self.log('OFFLINE: Using local file "%s"...' % (export_def['jsonfilename']))
                if os.path.isfile(self.attachAbsolutePath(export_def['jsonfilename'])):
                    if self.verbosity:
                        self.log('OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                            export_def['jsonfilename'], os.path.getsize(self.attachAbsolutePath(export_def['jsonfilename']))))
                else:
                    self.log('Could not find offline file "%s". Exiting...' % (export_def['jsonfilename']))
                    sys.exit(1)

        # STEP 7: Prepare OCL exports for diff
        # Concepts/mappings in OCL exports have extra attributes that should be removed before the diff
        if self.verbosity:
            self.log('**** STEP 7 of 13: Prepare OCL exports for diff')
        for ocl_export_def_key in self.ocl_export_defs:
            if self.verbosity:
                self.log('%s:' % (ocl_export_def_key))
            export_def = self.ocl_export_defs[ocl_export_def_key]
            with open(self.attachAbsolutePath(export_def['jsonfilename']), 'rb') as ifile, open(
                    self.attachAbsolutePath(export_def['jsoncleanfilename']), 'wb') as ofile:
                ocl_sims_export = json.load(ifile)
                ocl_sims_export_clean = {}

                # Iterate through concepts, clean, then write
                for c in ocl_sims_export['concepts']:
                    url = c['url']
                    # Remove core fields
                    for f in self.sims_concept_fields_to_remove:
                        if f in c: del c[f]
                    # Remove name fields
                    if 'names' in c:
                        for i, name in enumerate(c['names']):
                            for f in self.sims_name_fields_to_remove:
                                if f in name: del name[f]
                    ocl_sims_export_clean[url] = c

                # Iterate through mappings, clean, then write -- not used for SIMS assessment types
                for m in ocl_sims_export['mappings']:
                    url = m['url']
                    core_fields_to_remove = []
                    for f in self.sims_mapping_fields_to_remove:
                        if f in m: del m[f]
                    ocl_sims_export_clean[url] = m
                ofile.write(json.dumps(ocl_sims_export_clean))
                if self.verbosity:
                    self.log('Processed OCL export saved to "%s"' % (export_def['jsoncleanfilename']))

        # STEP 8: Perform deep diff
        # Note that multiple deep diffs may be performed, each with their own input and output files
        if self.verbosity:
            self.log('**** STEP 8 of 13: Perform deep diff')
        diff = None
        with open(self.attachAbsolutePath(self.ocl_export_defs['sims_source']['jsoncleanfilename']), 'rb') as ocl_handle,\
                open(self.attachAbsolutePath(self.converted_dhis2_export_filename), 'rb') as dhis2_handle:
            a_ocl = json.load(ocl_handle)
            b_dhis2 = json.load(dhis2_handle)
            diff = DeepDiff(a_ocl, b_dhis2, ignore_order=True, verbose_level=2)
            if self.verbosity:
                str_log = 'Diff results: '
                for k in diff:
                    str_log += '%s: %s; ' % (k, len(diff[k]))
                self.log(str_log)

        # STEP 9: Determine action based on diff result
        # TODO: If data check only, then output need to return Success/Failure and then exit regardless
        if self.verbosity:
            self.log('**** STEP 9 of 13: Determine action based on diff result')
        if diff:
            self.log('Deep diff identified one or more differences between DHIS2 and OCL...')
        else:
            self.log('No diff, exiting...')
            exit()

        # STEP 10: Generate import scripts by processing the diff results
        # TODO: This currently only handles 'dictionary_item_added'
        if self.verbosity:
            self.log('**** STEP 10 of 13: Generate import scripts')
        with open(self.attachAbsolutePath(self.new_import_script_filename), 'wb') as ofile:
            if 'dictionary_item_added' in diff:
                for k in diff['dictionary_item_added']:
                    if 'type' in diff['dictionary_item_added'][k] and diff['dictionary_item_added'][k]['type'] == 'Concept':
                        ofile.write(json.dumps(diff['dictionary_item_added'][k]))
                        ofile.write('\n')
            if self.verbosity:
                self.log('New import script written to file "%s"' % (self.new_import_script_filename))

        # STEP 11: Perform the import in OCL
        if self.verbosity:
            self.log('**** STEP 11 of 13: Perform the import in OCL')
        ocl_importer = ocl_json_flex_import(
            file_path=self.attachAbsolutePath(self.new_import_script_filename),
            api_token=self.oclapitoken, api_url_root=self.oclenv, test_mode=self.import_test_mode,
            do_update_if_exists=False, verbosity=self.verbosity, limit=self.import_limit)
        import_result = ocl_importer.process()
        if self.verbosity:
            self.log('Import records processed:', import_result)

        # STEP 12: Save new DHIS2 export for the next sync attempt
        if self.verbosity:
            self.log('**** STEP 12 of 13: Save the DHIS2 export if import successful')
        if import_result and not self.import_test_mode:
            # Delete the old cache if it is there
            if os.path.isfile(self.attachAbsolutePath(self.old_dhis2_export_filename)):
                os.remove(self.attachAbsolutePath(self.old_dhis2_export_filename))
            # Copy the new dhis2 export
            copyfile(self.attachAbsolutePath(self.new_dhis2_export_filename),
                     self.attachAbsolutePath(self.old_dhis2_export_filename))
            if self.verbosity:
                self.log('DHIS2 export successfully copied to "%s"' % (self.old_dhis2_export_filename))
        else:
            if self.verbosity:
                self.log('Skipping, because import failed or import test mode enabled...')

        # STEP 13: Manage OCL repository versions
        if self.verbosity:
            self.log('**** STEP 13 of 13: Manage OCL repository versions')
        if self.import_test_mode:
            if self.verbosity:
                self.log('Skipping, because import test mode enabled...')
        elif import_result:
            # SIMS source
            dt = datetime.utcnow()
            new_source_version_data = {
                'id': 'v' + dt.strftime('%Y-%m-%dT%H%M%S.%f'),
                'description': 'Automatically generated by DATIM-Sync - %s import record(s) processed' % (import_result),
                'released': True,
            }
            new_source_version_url = oclenv + '/orgs/PEPFAR/sources/SIMS/versions/'
            if self.verbosity:
                self.log('Create new version request URL:', new_source_version_url)
                self.log(json.dumps(new_source_version_data))
            r = requests.post(new_source_version_url,
                          data=json.dumps(new_source_version_data),
                          headers=self.oclapiheaders)
            r.raise_for_status()

            # TODO: SIMS collections

        else:
            if self.verbosity:
                self.log('Skipping because no records imported...')


# Default Script Settings
verbosity = 2                   # 0=none, 1=some, 2=all
import_limit = 0                # Number of resources to import; 0=all
import_test_mode = False        # Set to True to see which import API requests would be performed on OCL
runoffline = False              # Set to true to use local copies of dhis2/ocl exports
compare2previousexport = True   # Set to False to ignore the previous export

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
    import_limit = 2
    dhis2env = 'https://dev-de.datim.org/'
    dhis2uid = 'jpayne'
    dhis2pwd = 'Johnpayne1!'
    oclenv = 'https://api.showcase.openconceptlab.org'
    oclapitoken = '2da0f46b7d29aa57970c0b3a535121e8e479f881'
    #oclapitoken = 'a61ba53ed7b8b26ece8fcfc53022b645de0ec055'
    #oclenv = 'https://ocl-stg.openmrs.org'
    compare2previousexport = False

# Create SIMS sync object and run
sims_sync = DatimSimsSync(oclenv=oclenv, oclapitoken=oclapitoken,
                          dhis2env=dhis2env, dhis2uid=dhis2uid, dhis2pwd=dhis2pwd,
                          compare2previousexport=compare2previousexport,
                          runoffline=runoffline, verbosity=verbosity,
                          import_test_mode=import_test_mode, import_limit=import_limit)
sims_sync.run()
