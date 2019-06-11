"""
Class to import into OCL an indicator mapping file (CSV or JSON) for a specified country (e.g. UG) and
period (e.g. FY19). CSV must follow the format of the country indicator mapping template.

The import script creates OCL-formatted JSON consisting of:
    Country Org (e.g. DATIM-MOH-UG) - if doesn't exist
    Country Source (e.g. DATIM-Alignment-Indicators) - if doesn't exist
    One concept for each country unique indicator/data element and unique disag
    One mapping for each unique country indicator+disag pair with an operation map type (e.g. ADD, SUBTRACT)
    One mapping for each PEPFAR indicator+disag pair represented with a "DATIM HAS OPTION" map type
    Country Collections, one per mapping to DATIM indicator+disag pair
    References for each concept and mapping added to each collection

TODO: Improve validation step
TODO: Move country collection reconstruction and version creation into a separate process that this class uses
TODO: Add "clean up" functionality to retire unused resources
TODO: Query collections by their mappings, not ID -- names are not consistent coming from DHIS2
TODO: Exclude "null-disag" from the import scripts -- this does not have any effect, its just an unnecessary step
"""
import sys
import requests
import json
import time
import pprint
import datimbase
import datimimap
import datimimapexport
import datimimapreferencegenerator
import ocldev.oclfleximporter
import ocldev.oclexport
import ocldev.oclconstants


class DatimImapImport(datimbase.DatimBase):
    """
    Class to import DATIM country indicator mapping metadata into OCL.
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
        """
        Import the specified IMAP into OCL
        :param imap_input:
        :return:
        """

        # Validate input variables
        if not self.oclapitoken or not self.oclapiheaders:
            msg = 'ERROR: Authorization token must be set'
            self.vlog(1, msg)
            raise Exception(msg)

        # STEP 1 of 12: Download /orgs/PEPFAR/sources/DATIM-MOH-FY##/ export for specified period from OCL
        self.vlog(1, '**** STEP 1 of 12: Download PEPFAR/DATIM-MOH-FY## metadata export from OCL')
        self.datim_moh_source_id = datimbase.DatimBase.get_datim_moh_source_id(imap_input.period)
        datim_source_endpoint = datimbase.DatimBase.get_datim_moh_source_endpoint(imap_input.period)
        datim_source_version = self.get_latest_version_for_period(
            repo_endpoint=datim_source_endpoint, period=imap_input.period)
        if not datim_source_version:
            msg = 'ERROR: Could not find released version for period "%s" for source "%s"' % (
                imap_input.period, datim_source_endpoint)
            self.vlog(1, msg)
            raise Exception(msg)
        self.vlog(1, 'Latest version found for period "%s" for source "%s": "%s"' % (
            imap_input.period, datim_source_endpoint, datim_source_version))
        datim_source_zip_filename = self.endpoint2filename_ocl_export_zip(datim_source_endpoint)
        datim_source_json_filename = self.endpoint2filename_ocl_export_json(datim_source_endpoint)
        if not self.run_ocl_offline:
            datim_source_export = self.get_ocl_export(
                endpoint=datim_source_endpoint, version=datim_source_version,
                zipfilename=datim_source_zip_filename, jsonfilename=datim_source_json_filename)
        else:
            self.does_offline_data_file_exist(datim_source_json_filename, exit_if_missing=True)

        # STEP 2 of 12: Validate input country mapping CSV file
        # NOTE: This currently just verifies that the correct columns exist (order agnostic)
        # TODO: Validate that DATIM indicator/disag IDs in the provided IMAP are valid
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
                country_code=imap_input.country_code, country_org=imap_input.country_org, verbosity=self.verbosity,
                period=imap_input.period)
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
        do_update_country = False
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
            country_owner_endpoint, datimbase.DatimBase.DATIM_MOH_COUNTRY_SOURCE_ID)
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
            self.vlog(1, 'Bulk importing %s changes to OCL...' % len(import_list))
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
                msg = 'Import is still processing... QUITTING'
                self.log(msg)
                raise Exception(msg)
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
        if ref_import_list and not self.test_mode:

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
                msg = 'Reference import is still processing... QUITTING'
                self.log(msg)
                raise Exception(msg)
        elif self.test_mode:
            self.vlog(1, 'SKIPPING: No collections to update in test mode...')
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
            'type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION,
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
            "type": ocldev.oclconstants.OclConstants.RESOURCE_TYPE_SOURCE,
            "id": datimbase.DatimBase.DATIM_MOH_COUNTRY_SOURCE_ID,
            "owner_type": "Organization",
            "owner": country_org,
            "short_code": datimbase.DatimBase.DATIM_MOH_COUNTRY_SOURCE_ID,
            "name": source_name,
            "full_name": source_name,
            "source_type": "Dictionary",
            "default_locale": "en",
            "supported_locales": "en",
            "public_access": country_public_access,
        }
        return source
