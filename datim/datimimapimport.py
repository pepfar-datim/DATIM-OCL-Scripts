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

TODO: Move country collection reconstruction and version creation into a separate process that this class uses
TODO: Add "clean up" functionality to retire unused resources
TODO: Query collections by their mappings, not ID -- names are not consistent coming from DHIS2
TODO: Exclude "null-disag" from the import scripts -- this does not have any effect, its just an unnecessary step
"""
import requests
import json
import pprint
import datimbase
import datimimap
import datimimapexport
import datimimapreferencegenerator
import ocldev.oclfleximporter
import ocldev.oclexport
import ocldev.oclconstants
import utils.timer


class DatimImapImport(datimbase.DatimBase):
    """
    Class to import DATIM country indicator mapping metadata into OCL.
    """

    DATIM_IMAP_RESULT_SUCCESS = 1
    DATIM_IMAP_RESULT_WARNING = 0
    DATIM_IMAP_RESULT_ERROR = -1

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

        # STEP 1 of 11: Download PEPFAR/DATIM-MOH-FY##/ export for specified period from OCL
        imap_timer = utils.timer.Timer()
        imap_timer.start()
        self.vlog(1, '**** STEP 1 of 11: Download PEPFAR/DATIM-MOH-FY## export for specified period from OCL')
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

        # STEP 2 of 11: Validate input country mapping CSV file
        # NOTE: This currently just verifies that the correct columns exist (order agnostic)
        # TODO: Validate that DATIM indicator/disag IDs in the provided IMAP are valid
        imap_timer.lap(label='Step 1')
        self.vlog(1, '**** STEP 2 of 11: Validate input country mapping CSV file')
        is_valid = imap_input.is_valid()
        if type(is_valid) == str:
            self.vlog(1, 'WARNING: The following warnings were found in the provided IMAP CSV:\n%s' % is_valid)
        else:
            self.vlog(1, 'The provided IMAP CSV passed validation')

        # STEP 3 of 11: Fetch latest available IMAP export from OCL for the specified country
        imap_timer.lap(label='Step 2')
        self.vlog(1, '**** STEP 3 of 11: Fetch latest available IMAP export from OCL for the specified country')
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

        # STEP 4 of 11: Evaluate delta between input and OCL IMAPs
        imap_timer.lap(label='Step 3')
        self.vlog(1, '**** STEP 4 of 11: Evaluate delta between input and OCL IMAPs')
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
                imap_diff.display()
            else:
                print('No DIFF available for the specified country/period\n')

        # STEP 5 of 11: Generate country org and source import scripts if they do not exist
        imap_timer.lap(label='Step 4')
        self.vlog(1, '**** STEP 5 of 11: Generate country org and source if they do not exist')
        import_list = []
        if not imap_old:
            org = DatimImapImport.get_country_org_dict(
                country_org=imap_input.country_org, country_code=imap_input.country_code,
                country_name=imap_input.country_name, country_public_access=self.country_public_access)
            import_list.append(org)
            self.vlog(1, 'Country org import script generated:', json.dumps(org))
            source = DatimImapImport.get_country_source_dict(
                country_org=imap_input.country_org, country_code=imap_input.country_code,
                country_name=imap_input.country_name, country_public_access=self.country_public_access)
            import_list.append(source)
            self.vlog(1, 'Country source import script generated:', json.dumps(source))
        else:
            self.vlog(1, 'SKIPPING: Country org and source already exist')

        # STEP 6 of 11: Generate import script for the country concepts and mappings
        imap_timer.lap(label='Step 5')
        self.vlog(1, '**** STEP 6 of 11: Generate import script for the country concepts and mappings')
        if imap_diff:
            self.vlog(1, 'Creating import script based on the delta...')
            add_to_import_list = datimimap.DatimImapFactory.generate_import_script_from_diff(
                imap_diff, verbose=self.verbosity)
            self.vlog(1, '%s diffs resulted in %s resource(s) added to import list' % (
                imap_diff.get_num_diffs(), len(add_to_import_list)))
        else:
            self.vlog(1, 'Creating import script using only the input IMAP CSV...')
            add_to_import_list = datimimap.DatimImapFactory.generate_import_script_from_csv(
                imap_input, verbose=self.verbosity)
            self.vlog(1, '%s resource(s) added to import list' % len(add_to_import_list))
        if add_to_import_list:
            import_list += add_to_import_list
        else:
            self.vlog(1, 'INFO: No resources to import. Exiting...')
            return True

        # STEP 7 of 11: Determine next country version number
        # NOTE: The country source and collections all version together
        imap_timer.lap(label='Step 6')
        self.vlog(1, '**** STEP 7 of 11: Determine next country version number')
        if imap_old:
            current_country_version_id = imap_old.version
            current_minor_version_number = datimimap.DatimImapFactory.get_minor_version_number_from_version_id(
                current_country_version_id)
            next_minor_version_number = int(current_minor_version_number) + 1
        else:
            current_country_version_id = ''
            next_minor_version_number = 0
        next_country_version_id = '%s.v%s' % (imap_input.period, next_minor_version_number)
        country_owner_endpoint = '/orgs/%s/' % imap_input.country_org
        country_source_endpoint = '%ssources/%s/' % (
            country_owner_endpoint, datimbase.DatimBase.DATIM_MOH_COUNTRY_SOURCE_ID)
        country_next_version_endpoint = '%s%s/' % (country_source_endpoint, next_country_version_id)
        country_next_version_url = self.oclenv + country_next_version_endpoint
        self.vlog(1, 'Current country version number for period "%s": "%s"' % (
            imap_input.period, current_country_version_id))
        self.vlog(1, 'Next country version number for period "%s": "%s"' % (
            imap_input.period, next_country_version_id))

        # STEP 8 of 11: Import changes to the source into OCL
        # NOTE: Up to this point, everything above is non-destructive. Changes are committed to OCL as of this step
        # TODO: Pass test_mode to the BulkImport API so that we can get real test results from the server
        imap_timer.lap(label='Step 7')
        self.vlog(1, '**** STEP 8 of 11: Import changes into OCL')
        import_results = None
        if import_list and not self.test_mode:
            self.vlog(1, 'Bulk importing %s changes to OCL...' % len(import_list))
            # TODO: Implement better OclBulkImporter response -- a new class OclBulkImportResponse?
            bulk_import_response = ocldev.oclfleximporter.OclBulkImporter.post(
                input_list=import_list, api_token=self.oclapitoken, api_url_root=self.oclenv)
            task_id = bulk_import_response.json()['task']
            if self.verbosity:
                self.vlog(1, 'BULK IMPORT TASK ID: %s' % task_id)
            import_results = ocldev.oclfleximporter.OclBulkImporter.get_bulk_import_results(
                task_id=task_id, api_url_root=self.oclenv, api_token=self.oclapitoken,
                delay_seconds=5, max_wait_seconds=800)
            if import_results:
                if self.verbosity:
                    self.vlog(1, import_results.display_report())
            else:
                # TODO: Need smarter way to handle long running bulk import than just quitting
                msg = 'Import taking too long to process... QUITTING'
                self.log(msg)
                raise Exception(msg)
        elif self.test_mode:
            self.vlog(1, 'TEST MODE: Skipping import...')
        else:
            self.vlog(1, 'SKIPPING: Nothing to import!')

        # STEP 9 of 11: Create new country source version
        imap_timer.lap(label='Step 8')
        self.vlog(1, '**** STEP 9 of 11: Create new country source version')
        if import_list and not self.test_mode:
            datimimap.DatimImapFactory.create_repo_version(
                oclenv=self.oclenv, oclapitoken=self.oclapitoken,
                repo_endpoint=country_source_endpoint, repo_version_id=next_country_version_id,
                delay_until_processed=True, delay_interval_seconds=10, verbose=True)
            self.vlog(1, 'New country source version created: "%s"' % next_country_version_id)
        elif self.test_mode:
            self.vlog(1, 'SKIPPING: New country source version not created in test mode...')
        elif not import_list:
            self.vlog(1, 'SKIPPING: No resources imported into the source...')
            # TODO: Note that the source version should still be incremented if references are added to collections

        # STEP 10 of 11: Generate JSON for ALL references for ALL country collections
        imap_timer.lap(label='Step 9')
        self.vlog(1, '**** STEP 10 of 11: Generate collection references')
        ref_import_list = None
        if import_list and not self.test_mode:
            refgen = datimimapreferencegenerator.DatimImapReferenceGenerator(
                oclenv=self.oclenv, oclapitoken=self.oclapitoken, imap_input=imap_input)
            country_source_export = ocldev.oclexport.OclExportFactory.load_export(
                repo_version_url=country_next_version_url, oclapitoken=self.oclapitoken)
            self.vlog(1, 'INFO: Successfully retrieved export of new source version "%s"' % country_next_version_url)
            ref_import_list = refgen.process_imap(country_source_export=country_source_export)
            pprint.pprint(ref_import_list)
        elif not import_list:
            self.vlog(1, 'SKIPPING: No resources imported so no need to update collections...')
        else:
            self.vlog(1, 'SKIPPING: New version not created in test mode...')

        # STEP 11 of 11: Import new collection references
        imap_timer.lap(label='Step 10')
        self.vlog(1, '**** STEP 11 of 11: Import new collection references')
        ref_import_results = None
        if ref_import_list and not self.test_mode:

            # STEP 11a. Get the list of unique collection IDs involved in this import
            unique_collection_ids = []
            for ref_import in ref_import_list:
                if ref_import['collection'] not in unique_collection_ids:
                    unique_collection_ids.append(ref_import['collection'])

            # STEP 11b. Delete all existing references for each collection involved in this import
            # NOTE: Bulk import currently supports Creates & Updates, not Deletes, so doing this one-by-one
            imap_timer.lap(label='Step 11a')
            self.vlog(1, 'Clearing existing collection references...')
            for collection_id in unique_collection_ids:
                collection_url = '%s/orgs/%s/collections/%s/' % (
                    self.oclenv, imap_input.country_org, collection_id)
                self.vlog(1, '  - %s' % collection_url)
                self.clear_collection_references(collection_url=collection_url)

            # STEP 11c. Create JSON for new repo version for each unique collection and add to ref_import_list
            imap_timer.lap(label='Step 11b')
            self.vlog(1, 'Creating JSON for each new collection version...')
            for collection_id in unique_collection_ids:
                new_repo_version_json = datimimap.DatimImapFactory.get_new_repo_version_json(
                    owner_type='Organization', owner_id=imap_input.country_org, repo_type='Collection',
                    repo_id=collection_id, released=True, repo_version_id=next_country_version_id)
                self.vlog(1, '  - %s' % new_repo_version_json)
                ref_import_list.append(new_repo_version_json)

            # STEP 11d. Bulk import new references and collection versions
            imap_timer.lap(label='Step 11c')
            self.vlog(1, 'Importing %s batch(es) of collection references and new collection versions...' % len(
                ref_import_list))
            # TODO: Implement better OclBulkImporter response -- a new class OclBulkImportResponse?
            bulk_import_response = ocldev.oclfleximporter.OclBulkImporter.post(
                input_list=ref_import_list, api_token=self.oclapitoken, api_url_root=self.oclenv)
            ref_task_id = bulk_import_response.json()['task']
            if self.verbosity:
                self.vlog(1, 'BULK IMPORT TASK ID: %s' % ref_task_id)
            ref_import_results = ocldev.oclfleximporter.OclBulkImporter.get_bulk_import_results(
                task_id=ref_task_id, api_url_root=self.oclenv, api_token=self.oclapitoken,
                delay_seconds=6, max_wait_seconds=800)
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

        # Stop the timer
        imap_timer.stop(label='Step 11d')

        # SHOW SOME FINAL DEBUG OUTPUT
        self.vlog(1, '**** IMAP IMPORT SUMMARY')
        has_warnings = False
        if import_results:
            self.vlog(1, 'INFO: Initial country concept and mapping import:', import_results.get_summary())
            if import_results.has_error_status_code():
                has_warnings = True
                self.vlog(1, 'WARNING: Initial country concept and mapping import completed with one or more warnings')
        if ref_import_results:
            self.vlog(1, 'INFO: Country reference import:', ref_import_results.get_summary())
            if ref_import_results.has_error_status_code():
                has_warnings = True
                self.vlog(1, 'WARNING: Country reference import completed with one or more warnings')
        self.vlog(2, '** IMAP import time breakdown:\n', imap_timer)
        if has_warnings:
            self.vlog(1, 'WARNING: IMAP import process complete with one or more warnings!')
            return DatimImapImport.DATIM_IMAP_RESULT_WARNING
        else:
            self.vlog(1, 'INFO: IMAP import process completed successfully!')
            return DatimImapImport.DATIM_IMAP_RESULT_SUCCESS

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
    def get_country_org_dict(country_org='', country_code='', country_name='', country_public_access='View', period=''):
        """ Get an OCL-formatted dictionary of a country IMAP organization ready to import """
        return {
            'type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION,
            'id': country_org,
            'name': 'DATIM MOH %s' % country_name,
            'location': country_name,
            'public_access': country_public_access,
            "extras": {"datim_moh_object": True, "datim_moh_period": period}
        }

    @staticmethod
    def get_country_source_dict(country_org='', country_code='', country_name='', country_public_access='View',
                                period=''):
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
            "extras": {"datim_moh_object": True, "datim_moh_period": period}

        }
        return source
