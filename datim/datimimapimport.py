"""
Class to import into OCL an indicator mapping CSV for a specified country (e.g. UG) and
period (e.g. FY17). CSV must follow the format of the country indicator mapping CSV template.

TODO:
- Improve validation step: New import must be for the latest or newer country period
  (e.g. can't import/update FY17 if FY18 already defined)
- Move country collection reconstruction and version creation into a separate process that this class uses
- Add "clean up" functionality to retire unused resources
- Query collections by their mappings, not ID -- names are not consistent coming from DHIS2 which is killing this
- Exclude "null-disag" from the import scripts -- this does not have any effect, its just an unnecessary step

The import script creates OCL-formatted JSON consisting of:
    Country Org (e.g. DATIM-MOH-UG) - if doesn't exist
    Country Source (e.g. DATIM-Alignment-Indicators) - if doesn't exist
    One concept for each country unique indicator/data element and unique disag
    One mapping for each unique country indicator+disag pair with an operation map type (e.g. ADD, SUBTRACT)
    One mapping for each PEPFAR indicator+disag pair represented with a "DATIM HAS OPTION" map type
    Country Collections, one per mapping to DATIM indicator+disag pair
    References for each concept and mapping added to each collection
"""
import sys
import requests
import json
import os
import time
import pprint
import datimbase
import datimimap
import datimimapexport
import datimimapreferencegenerator
import ocldev.oclfleximporter
import ocldev.oclexport


class DatimImapImport(datimbase.DatimBase):
    """
    Class to import DATIM country indicator mapping metadata from a CSV file into OCL.
    """

    def __init__(self, oclenv='', oclapitoken='', verbosity=0, run_ocl_offline=False, test_mode=False,
                 country_public_access='View'):
        datimbase.DatimBase.__init__(self)
        self.verbosity = verbosity
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.run_ocl_offline = run_ocl_offline
        self.test_mode = test_mode
        self.country_public_access = country_public_access

        # Prepare the headers
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    def import_imap(self, imap_input=None):
        """ Import the specified IMAP into OCL """

        # Get out of here if variables aren't set
        if not self.oclapitoken or not self.oclapiheaders:
            msg = 'ERROR: Authorization token must be set'
            self.vlog(1, msg)
            raise Exception(msg)

        # STEP 1 of 12: Download PEPFAR DATIM metadata export for specified period from OCL
        self.vlog(1, '**** STEP 1 of 12: Download PEPFAR/DATIM-MOH metadata export for specified period from OCL')
        datim_owner_endpoint = '/orgs/%s/' % self.datim_owner_id
        datim_source_endpoint = '%ssources/%s/' % (datim_owner_endpoint, self.datim_source_id)
        datim_source_version = self.get_latest_version_for_period(
            repo_endpoint=datim_source_endpoint, period=imap_input.period)
        if not datim_source_version:
            msg = 'ERROR: Could not find released version for period "%s" for source PEPFAR/DATIM-MOH' % (
                imap_input.period)
            self.vlog(1, msg)
            raise Exception(msg)
        self.vlog(1, 'Latest version found for period "%s" for source PEPFAR/DATIM-MOH: "%s"' % (
            imap_input.period, datim_source_version))
        datim_source_zipfilename = self.endpoint2filename_ocl_export_zip(datim_source_endpoint)
        datim_source_jsonfilename = self.endpoint2filename_ocl_export_json(datim_source_endpoint)
        if not self.run_ocl_offline:
            datim_source_export = self.get_ocl_export(
                endpoint=datim_source_endpoint, version=datim_source_version,
                zipfilename=datim_source_zipfilename, jsonfilename=datim_source_jsonfilename)
        else:
            self.vlog(1, 'OCL-OFFLINE: Using local file "%s"...' % datim_source_jsonfilename)
            if os.path.isfile(self.attach_absolute_data_path(datim_source_jsonfilename)):
                self.vlog(1, 'OCL-OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                    datim_source_jsonfilename, os.path.getsize(self.attach_absolute_data_path(
                        datim_source_jsonfilename))))
            else:
                msg = 'ERROR: Could not find offline OCL file "%s"' % datim_source_jsonfilename
                self.vlog(1, msg)
                raise Exception(msg)

        # STEP 2 of 12: Validate input country mapping CSV file
        # NOTE: This currently just verifies that the correct columns exist (order agnostic)
        self.vlog(1, '**** STEP 2 of 12: Validate input country mapping CSV file')
        if imap_input.is_valid():
            self.vlog(1, 'Required fields are defined in the provided IMAP CSV')
        else:
            msg = 'Missing required fields in the provided IMAP CSV'
            self.vlog(1, msg)
            raise Exception(msg)

        # STEP 3 of 12: Fetch latest available IMAP export from OCL for the specified country
        self.vlog(1, '**** STEP 3 of 12: Fetch latest available IMAP export from OCL for the specified country')
        try:
            imap_old = datimimap.DatimImapFactory.load_imap_from_ocl(
                oclenv=self.oclenv, oclapitoken=self.oclapitoken, run_ocl_offline=self.run_ocl_offline,
                country_code=imap_input.country_code, country_org=imap_input.country_org, verbosity=self.verbosity)
            self.vlog(1, '%s CSV rows loaded from the OCL IMAP export' % imap_old.length())
        except requests.exceptions.HTTPError as e:
            imap_old = None
            self.vlog(1, 'HTTPError: No IMAP export available for country "%s". %s' % (imap_input.country_org, str(e)))
        except datimimapexport.DatimUnknownCountryPeriodError:
            imap_old = None
            self.vlog(1, 'DatimUnknownCountryPeriodError: No IMAP export available for country "%s". Continuing...' % (
                imap_input.country_org))

        # STEP 4 of 12: Evaluate delta between input and OCL IMAPs
        self.vlog(1, '**** STEP 4 of 12: Evaluate delta between input and OCL IMAPs')
        imap_diff = None
        if imap_old:
            self.vlog(1, 'Previous OCL IMAP export is available. Evaluating delta...')
            imap_diff = imap_old.diff(imap_input, exclude_empty_maps=True)
        else:
            self.vlog(1, 'No previous OCL IMAP export available. Continuing...')

        # SHOW SOME DEBUG OUTPUT
        if self.verbosity:
            print('\n**** NEW IMAP')
            imap_input.display(exclude_empty_maps=True, auto_fix_null_disag=True)
            print('\n**** OLD IMAP')
            if imap_old:
                imap_old.display(exclude_empty_maps=True, auto_fix_null_disag=True)
            else:
                print('No old IMAP available for the specified country/period')
            print('\n**** DIFF')
            if imap_diff:
                pprint.pprint(imap_diff.get_diff())
                print('\n')
            else:
                print('No DIFF available for the specified country/period\n')

        # STEP 5 of 12: Determine actions to take
        self.vlog(1, '**** STEP 5 of 12: Determine actions to take')
        do_create_country_org = False
        do_create_country_source = False
        do_update_country_concepts = False
        do_update_country_collections = False
        do_something = False
        if not imap_old:
            do_create_country_org = True
            do_create_country_source = True
            do_something = True
            self.vlog(1, 'Country org and source do not exist. Will create...')
            # TODO: Check existence of org/source directly with OCL rather than via IMAP
        else:
            self.vlog(1, 'Country org and source exist. No action to take...')
        if imap_diff or not imap_old:
            # TODO: Actually use the "do_update..." variables
            do_update_country = True
            do_something = True
            self.vlog(1, 'Country concepts and mappings do not exist or are out of date. Will update...')
        else:
            self.vlog(1, 'Country concepts and mappings are up-to-date. No action to take...')
        if not do_something:
            self.vlog(1, 'No action to take. Exiting...')
            return

        # STEP 6 of 12: Determine next country version number
        # NOTE: The country source and collections all version together
        self.vlog(1, '**** STEP 6 of 12: Determine next country version number')
        current_country_version_id = ''
        country_owner_endpoint = '/orgs/%s/' % imap_input.country_org
        country_source_endpoint = '%ssources/%s/' % (
            country_owner_endpoint, datimbase.DatimBase.country_source_id)
        if do_create_country_source:
            next_country_version_id = '%s.v0' % imap_input.period
        else:
            current_country_version_id = self.get_latest_version_for_period(
                repo_endpoint=country_source_endpoint, period=imap_input.period)
            if not current_country_version_id:
                next_country_version_id = '%s.v0' % imap_input.period
            else:
                needle = '%s.v' % imap_input.period
                current_minor_version_number = int(current_country_version_id.replace(needle, ''))
                next_minor_version_number = current_minor_version_number + 1
                next_country_version_id = '%s.v%s' % (imap_input.period, next_minor_version_number)
        country_next_version_endpoint = '%s%s/' % (country_source_endpoint, next_country_version_id)
        country_next_version_url = self.oclenv + country_next_version_endpoint
        self.vlog(1, 'Current country version number for period "%s": "%s"' % (
            imap_input.period, current_country_version_id))
        self.vlog(1, 'Next country version number for period "%s": "%s"' % (
            imap_input.period, next_country_version_id))

        # STEP 7 of 12: Generate import script for the country org and source, if missing
        self.vlog(1, '**** STEP 7 of 12: Generate country org and source if missing')
        import_list = []
        if do_create_country_org:
            org = DatimImapImport.get_country_org_dict(country_org=imap_input.country_org,
                                                       country_code=imap_input.country_code,
                                                       country_name=imap_input.country_name,
                                                       country_public_access=self.country_public_access)
            import_list.append(org)
            self.vlog(1, 'Country org import script generated:', json.dumps(org))
        if do_create_country_source:
            source = DatimImapImport.get_country_source_dict(country_org=imap_input.country_org,
                                                             country_code=imap_input.country_code,
                                                             country_name=imap_input.country_name,
                                                             country_public_access=self.country_public_access)
            import_list.append(source)
            self.vlog(1, 'Country source import script generated:', json.dumps(source))
        if not do_create_country_org and not do_create_country_source:
            self.vlog(1, 'Skipping...')

        # STEP 8 of 12: Generate import script for the country source concepts and mappings
        self.vlog(1, '**** STEP 8 of 12: Generate import script for the country source concepts and mappings')
        if imap_diff:
            self.vlog(1, 'Creating import script based on the delta...')
            add_to_import_list = datimimap.DatimImapFactory.generate_import_script_from_diff(imap_diff)
            self.vlog(1, '%s resource(s) added to import list' % len(add_to_import_list))
            import_list += add_to_import_list
        else:
            self.vlog(1, 'Creating import script for full country CSV...')
            add_to_import_list = datimimap.DatimImapFactory.generate_import_script_from_csv(imap_input)
            self.vlog(1, '%s resource(s) added to import list' % len(add_to_import_list))
            import_list += add_to_import_list
        if self.verbosity > 1:
            pprint.pprint(import_list)

        # STEP 9 of 12: Import changes to the source into OCL
        # NOTE: Up to this point, everything above is non-destructive. Changes are committed to OCL as of this step
        # TODO: Pass test_mode to the BulkImport API so that we can get real test results from the server
        self.vlog(1, '**** STEP 9 of 12: Import changes into OCL')
        if import_list and not self.test_mode:
            self.vlog(1, 'Importing %s changes to OCL...' % len(import_list))
            # TODO: Implement better OclBulkImporter response -- a new class OclBulkImportResponse?
            bulk_import_response = ocldev.oclfleximporter.OclBulkImporter.post(
                input_list=import_list, api_token=self.oclapitoken, api_url_root=self.oclenv)
            task_id = bulk_import_response.json()['task']
            import_results = ocldev.oclfleximporter.OclBulkImporter.get_bulk_import_results(
                task_id=task_id, api_url_root=self.oclenv, api_token=self.oclapitoken,
                delay_seconds=5, max_wait_seconds=500)
            if import_results:
                if self.verbosity:
                    self.vlog(self.verbosity, import_results.display_report())
            else:
                # TODO: Need smarter way to handle long running bulk import than just quitting
                print 'Import is still processing... QUITTING'
                sys.exit(1)

            '''
            # JP 2019-04-23: Old OclFlexImporter code replaced by the bulk import code above 
            importer = ocldev.oclfleximporter.OclFlexImporter(
                input_list=import_list, api_token=self.oclapitoken, api_url_root=self.oclenv,
                test_mode=self.test_mode, verbosity=self.verbosity,
                do_update_if_exists=True, import_delay=0)
            importer.process()
            if self.verbosity:
                importer.import_results.display_report()
            '''
        elif self.test_mode:
            self.vlog(1, 'Test mode! Skipping import...')
        else:
            self.vlog(1, 'Nothing to import! Skipping...')

        # STEP 10 of 12: Create new country source version
        self.vlog(1, '**** STEP 10 of 12: Create new country source version')
        if import_list and not self.test_mode:
            datimimap.DatimImapFactory.create_repo_version(
                oclenv=self.oclenv, oclapitoken=self.oclapitoken,
                repo_endpoint=country_source_endpoint, repo_version_id=next_country_version_id)
            self.vlog(1, 'New country source version created: "%s"' % next_country_version_id)
        elif self.test_mode:
            self.vlog(1, 'SKIPPING: New country source version not created in test mode...')
        elif not import_list:
            self.vlog(1, 'SKIPPING: No resources imported into the source...')
            # TODO: Note that the source version should still be incremented if references are added to collections

        # STEP 10b of 12: Delay until the country source version is done processing
        # TODO: Incorporate delay until done processing into the create_repo_version method
        self.vlog(1, '**** STEP 10b of 12: Delay until the country source version is done processing')
        if import_list and not self.test_mode:
            is_repo_version_processing = True
            country_version_processing_url = '%s%sprocessing/' % (
                self.oclenv, country_next_version_endpoint)
            self.vlog(1, 'URL for checking source version processing status: %s' % country_version_processing_url)
            while is_repo_version_processing:
                r = requests.get(country_version_processing_url, headers=self.oclapiheaders)
                r.raise_for_status()
                self.vlog(1, 'Processing status: %s' % r.text)
                if r.text == 'False':
                    is_repo_version_processing = False
                    self.vlog(1, 'Source version processing is complete. Continuing...')
                else:
                    self.vlog(1, 'DELAY: Delaying 15 seconds while new source version is processing')
                    time.sleep(10)
        elif not import_list:
            self.vlog(1, 'SKIPPING: No resources imported so no new versions to create...')
        elif self.test_mode:
            self.vlog(1, 'SKIPPING: New source version not created in test mode...')

        # STEP 11 of 12: Generate JSON for ALL references for ALL country collections
        self.vlog(1, '**** STEP 11 of 12: Generate collection references')
        ref_import_list = None
        if import_list and not self.test_mode:
            refgen = datimimapreferencegenerator.DatimImapReferenceGenerator(
                oclenv=self.oclenv, oclapitoken=self.oclapitoken, imap_input=imap_input)
            country_source_export = ocldev.oclexport.OclExportFactory.load_export(
                repo_version_url=country_next_version_url, oclapitoken=self.oclapitoken)
            ref_import_list = refgen.process_imap(country_source_export=country_source_export)
            pprint.pprint(ref_import_list)
        elif not import_list:
            self.vlog(1, 'SKIPPING: No resources imported so no need to update collections...')
        else:
            self.vlog(1, 'SKIPPING: New version not created in test mode...')

        # STEP 12 of 12: Import new collection references
        self.vlog(1, '**** STEP 12 of 12: Import new collection references')
        if ref_import_list:

            # 12a. Get the list of unique collection IDs
            unique_collection_ids = []
            for ref_import in ref_import_list:
                if ref_import['collection'] not in unique_collection_ids:
                    unique_collection_ids.append(ref_import['collection'])

            # 12b. Delete existing references for each unique collection
            # NOTE: Bulk import currently supports Creates & Updates, not Deletes, so this will be done the old way
            self.vlog(1, 'Clearing existing collection references...')
            for collection_id in unique_collection_ids:
                collection_url = '%s/orgs/%s/collections/%s/' % (
                    self.oclenv, imap_input.country_org, collection_id)
                self.vlog(1, '  - %s' % collection_url)
                self.clear_collection_references(collection_url=collection_url)

            # 12c. Create JSON for new repo version for each unique collection
            self.vlog(1, 'Creating JSON for each new collection version...')
            for collection_id in unique_collection_ids:
                new_repo_version_json = datimimap.DatimImapFactory.get_new_repo_version_json(
                    owner_type='Organization', owner_id=imap_input.country_org, repo_type='Collection',
                    repo_id=collection_id, released=True, repo_version_id=next_country_version_id)
                self.vlog(1, '  - %s' % new_repo_version_json)
                ref_import_list.append(new_repo_version_json)

            # 12d. Bulk import new references and collection versions
            self.vlog(1, 'Importing %s batch(es) of collection references...' % len(ref_import_list))
            # TODO: Implement better OclBulkImporter response -- a new class OclBulkImportResponse?
            bulk_import_response = ocldev.oclfleximporter.OclBulkImporter.post(
                input_list=ref_import_list, api_token=self.oclapitoken, api_url_root=self.oclenv)
            ref_task_id = bulk_import_response.json()['task']
            ref_import_results = ocldev.oclfleximporter.OclBulkImporter.get_bulk_import_results(
                task_id=ref_task_id, api_url_root=self.oclenv, api_token=self.oclapitoken,
                delay_seconds=6, max_wait_seconds=500)
            if ref_import_results:
                self.vlog(1, ref_import_results.display_report())
            else:
                # TODO: Need smarter way to handle long running bulk import than just quitting
                print 'Reference import is still processing... QUITTING'
                sys.exit(1)

            '''
            # JP 2019-04-23: Old OclFlexImporter code replaced by the bulk import code above 
            importer = ocldev.oclfleximporter.OclFlexImporter(
                input_list=ref_import_list, api_token=self.oclapitoken, api_url_root=self.oclenv,
                test_mode=self.test_mode, verbosity=self.verbosity, import_delay=0)
            importer.process()
            if self.verbosity:
                importer.import_results.display_report()
            '''

            '''
            # JP 2019-04-24: Incorporated creation of new collection versions into the bulk import script above
            # 12d. Create new version for each unique collection
            # TODO: Incorporate collection version requests into the bulk import script above
            self.vlog(1, 'Creating new collection versions...')
            for collection_id in unique_collection_ids:
                collection_endpoint = '/orgs/%s/collections/%s/' % (imap_input.country_org, collection_id)
                collection_version_endpoint = '%s%s/' % (collection_endpoint, next_country_version_id)
                self.vlog(1, 'Creating collection version: %s' % collection_version_endpoint)
                datimimap.DatimImapFactory.create_repo_version(
                    oclenv=self.oclenv, oclapitoken=self.oclapitoken,
                    repo_endpoint=collection_endpoint, repo_version_id=next_country_version_id)
            '''
        else:
            self.vlog(1, 'SKIPPING: No collections updated...')

        self.vlog(1, '**** IMAP import process complete!')

    def clear_collection_references(self, collection_url='', batch_size=25):
        """ Clear all references for the specified collection """

        # Load the list of references in the collection
        collection_refs_url = '%sreferences/' % collection_url
        r = requests.get(collection_url, headers=self.oclapiheaders)
        r.raise_for_status()
        collection = r.json()

        # Exit if no references
        if 'references' not in collection or not collection['references']:
            self.vlog(1, 'Collection is already empty. Continuing...')
            return

        # Loop through collection references and delete in batches
        i = 0
        refs = []
        while i < len(collection['references']):
            refs.append(collection['references'][i]['expression'])
            if len(refs) % batch_size == 0:
                payload = {"references": refs}
                self.vlog(1, '%s: %s' % (collection_refs_url, json.dumps(payload)))
                r = requests.delete(collection_refs_url, json=payload, headers=self.oclapiheaders)
                r.raise_for_status()
                refs = []
            i += 1

        # Delete any references still in the refs list
        if refs:
            payload = {"references": refs}
            self.vlog(1, '%s: %s' % (collection_refs_url, json.dumps(payload)))
            r = requests.delete(collection_refs_url, json=payload, headers=self.oclapiheaders)
            r.raise_for_status()

    @staticmethod
    def get_country_org_dict(country_org='', country_code='', country_name='', country_public_access='View'):
        """ Get an OCL-formatted dictionary of a country IMAP organization ready to import """
        return {
            'type': 'Organization',
            'id': country_org,
            'name': 'DATIM MOH %s' % country_name,
            'location': country_name,
            'public_access': country_public_access,
        }

    @staticmethod
    def get_country_source_dict(country_org='', country_code='', country_name='', country_public_access='View'):
        """ Get an OCL-formatted dictionary of a country IMAP source ready to import """
        source_name = 'DATIM MOH %s Alignment Indicators' % country_name
        source = {
            "type": "Source",
            "id": datimbase.DatimBase.country_source_id,
            "owner_type": "Organization",
            "owner": country_org,
            "short_code": datimbase.DatimBase.country_source_id,
            "name": source_name,
            "full_name": source_name,
            "source_type": "Dictionary",
            "default_locale": "en",
            "supported_locales": "en",
            "public_access": country_public_access,
        }
        return source
