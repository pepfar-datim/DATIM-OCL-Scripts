"""
Class to import into OCL a country mapping CSV for a specified country (e.g. UG) and
period (e.g. FY17). CSV must follow the format of the country mapping CSV template.

Diff & Import Steps:
1.  Validate that OCL environment is ready: /PEPFAR/DATIM-MOH course/fine metadata
    and released period (e.g. FY17) available
2.  Load/validate input country mapping CSV file: verify correct columns exist (order agnostic)
3.  Pre-process input country mapping CSV, if needed (csv_fixer.py)
4.  Fetch imap CSV export from OCL for the specified country+period (imapexport.py)
5.  Evaluate delta between input and OCL CSV
6.  Generate (and dedup) import script of country org, source, collections, concepts, and
    mappings from the delta
7.  Import delta JSON into OCL, and be sure to get the mapping IDs into the import results object!
    - Import error handling?
8.  Generate released source version for the country
9.  Generate collection references (refgen.py)
10. Import the collection references
11. Create released versions for each of the collections (new_versions.py)

Source files from ocl_import/DATIM/:
    moh_csv2json.py
    new_versions.py
    refgen.py
    csv_fixer.py

Columns for the input country mapping CSV file:
    DATIM_Indicator_Category (e.g. HTS_TST)
    DATIM_Indicator_ID (e.g. HTS_TST_N_MOH or HTS_TST_N_MOH_Age_Sex_Result)
    DATIM_Disag_ID (e.g. HllvX50cXC0)
    DATIM_Disag_Name (e.g. Total)
    Operation (ADD, SUBTRACT, ADD HALF, SUBTRACT HALF)
    MOH_Indicator_ID (e.g. )
    MOH_Indicator_Name (HTS_TST_POS_U15_F, PMTCT_STAT_NEW_NEG)
    MOH_Disag_ID (e.g. HQWtIkUYJnX)
    MOH_Disag_Name (e.g. 5-9yrsF, Positive|15+|Male)

The output JSON file consists of one JSON document per line and for each country includes:
    Country Org (e.g. DATIM-MOH-UG)
    Country Source (e.g. DATIM-Alignment-Indicators)
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
import datimimapreferencegenerator
import ocldev.oclfleximporter
import ocldev.oclexport


class DatimImapImport(datimbase.DatimBase):
    """
    Class to import DATIM country mapping metadata from a CSV file into OCL.
    """

    def __init__(self, oclenv='', oclapitoken='', verbosity=0, run_ocl_offline=False, test_mode=False):
        datimbase.DatimBase.__init__(self)
        self.verbosity = verbosity
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.run_ocl_offline = run_ocl_offline
        self.test_mode = test_mode

        # Prepare the headers
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    def import_imap(self, imap_input=None):
        """ Import the specified IMAP into OCL """

        # Get out of here if variables aren't set
        if not self.oclapitoken or not self.oclapiheaders:
            self.log('ERROR: Authorization token must be set')
            sys.exit(1)

        # STEP 1 of 12: Download PEPFAR DATIM metadata export for specified period from OCL
        self.vlog(1, '**** STEP 1 of 12: Download PEPFAR/DATIM-MOH metadata export for specified period from OCL')
        datim_owner_endpoint = '/orgs/%s/' % self.datim_owner_id
        datim_source_endpoint = '%ssources/%s/' % (datim_owner_endpoint, self.datim_source_id)
        datim_source_version = self.get_latest_version_for_period(
            repo_endpoint=datim_source_endpoint, period=imap_input.period)
        if not datim_source_version:
            self.log('ERROR: Could not find released version for period "%s" for source PEPFAR/DATIM-MOH' % imap_input.period)
            sys.exit(1)
        self.vlog(1, 'Latest version found for period "%s" for source PEPFAR/DATIM-MOH: "%s"' % (imap_input.period, datim_source_version))
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
                self.log('ERROR: Could not find offline OCL file "%s". Exiting...' % datim_source_jsonfilename)
                sys.exit(1)

        # STEP 2 of 12: Validate input country mapping CSV file
        # Verify that correct columns exist (order agnostic)
        self.vlog(1, '**** STEP 2 of 12: Validate input country mapping CSV file')
        if imap_input.is_valid():
            self.vlog(1, 'Provided IMAP CSV is valid')
        else:
            self.vlog(1, 'Provided IMAP CSV is not valid. Exiting...')
            sys.exit(1)

        # STEP 3 of 12: Fetch existing IMAP export from OCL for the specified country
        self.vlog(1, '**** STEP 3 of 12: Fetch existing IMAP export from OCL for the specified country and period')
        try:
            imap_old = datimimap.DatimImapFactory.load_imap_from_ocl(
                oclenv=self.oclenv, oclapitoken=self.oclapitoken, run_ocl_offline=self.run_ocl_offline,
                country_code=imap_input.country_code, country_org=imap_input.country_org,
                period=imap_input.period, verbosity=self.verbosity)
            self.vlog(1, '%s CSV rows loaded from the OCL IMAP export' % imap_old.length())
        except requests.exceptions.HTTPError:
            imap_old = None
            self.vlog(1, 'OCL IMAP export not available for country "%s" and period "%s". Continuing...' % (
                imap_input.country_org, imap_input.period))

        # STEP 4 of 12: Evaluate delta between input and OCL IMAPs
        self.vlog(1, '**** STEP 4 of 12: Evaluate delta between input and OCL IMAPs')
        imap_diff = None
        if imap_old:
            self.vlog(1, 'Previous OCL IMAP export is available. Evaluating delta...')
            imap_diff = imap_old.diff(imap_input, exclude_empty_maps=True)
            print '\n**** OLD IMAP'
            imap_old.display(exclude_empty_maps=True)
            print '\n**** NEW IMAP'
            imap_input.display(exclude_empty_maps=True)
            print '\n**** DIFF'
            pprint.pprint(imap_diff.get_diff())
        else:
            self.vlog(1, 'No previous OCL IMAP export available. Continuing...')

        # STEP 5 of 12: Determine actions to take
        self.vlog(1, '**** STEP 5 of 12: Determine actions to take')
        do_create_country_org = False
        do_create_country_source = False
        do_update_country_concepts = False
        do_update_country_collections = False
        if not imap_old:
            do_create_country_org = True
            do_create_country_source = True
            self.vlog(1, 'Country org and source do not exist. Will create...')
        else:
            self.vlog(1, 'Country org and source exist. No action to take...')
        if imap_diff or not imap_old:
            do_update_country_concepts = True
            do_update_country_collections = True
            self.vlog(1, 'Country concepts and mappings do not exist or are out of date. Will update...')
        else:
            self.vlog(1, 'Country concepts and mappings are up-to-date. No action to take...')
        if (not do_create_country_org and not do_create_country_source and
                not do_update_country_concepts and not do_update_country_collections):
            self.vlog(1, 'No action to take. Exiting...')
            sys.exit()

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

        # STEP 7 of 12: Generate country org and source if missing
        self.vlog(1, '**** STEP 7 of 12: Generate country org and source if missing')
        import_list = []
        if do_create_country_org:
            org = DatimImapImport.get_country_org_dict(country_org=imap_input.country_org,
                                            country_code=imap_input.country_code,
                                            country_name=imap_input.country_name)
            import_list.append(org)
            self.vlog(1, 'Country org import script generated:', json.dumps(org))
        if do_create_country_source:
            source = DatimImapImport.get_country_source_dict(country_org=imap_input.country_org,
                                                  country_code=imap_input.country_code,
                                                  country_name=imap_input.country_name)
            import_list.append(source)
            self.vlog(1, 'Country source import script generated:', json.dumps(source))
        if not do_create_country_org and not do_create_country_source:
            self.vlog(1, 'Skipping...')

        # STEP 8 of 12: Generate import script for the country source concepts and mappings
        self.vlog(1, '**** STEP 8 of 12: Generate import script for the country source concepts and mappings')
        if imap_diff:
            self.vlog(1, 'Creating import script based on the delta...')
            add_to_import_list = datimimap.DatimImapFactory.generate_import_script_from_diff(imap_diff)
            self.vlog(1, '%s resources added to import list' % len(add_to_import_list))
            import_list += add_to_import_list
        else:
            self.vlog(1, 'Creating import script for full country CSV...')
            add_to_import_list = datimimap.DatimImapFactory.generate_import_script_from_csv(imap_input)
            self.vlog(1, '%s resources added to import list' % len(add_to_import_list))
            import_list += add_to_import_list
        pprint.pprint(import_list)

        # STEP 9 of 12: Import changes to the source into OCL
        # NOTE: Up to this point, everything above is non-destructive. Changes are committed to OCL as of this step
        self.vlog(1, '**** STEP 9 of 12: Import changes into OCL')
        if import_list:
            self.vlog(1, 'Importing %s changes to OCL...' % len(import_list))
            importer = ocldev.oclfleximporter.OclFlexImporter(
                input_list=import_list, api_token=self.oclapitoken, api_url_root=self.oclenv,
                test_mode=self.test_mode, verbosity=self.verbosity,
                do_update_if_exists=True, import_delay=5)
            importer.process()
            if self.verbosity:
                importer.import_results.display_report()
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
        # time.sleep(15)
        self.vlog(1, '**** STEP 10b of 12: Delay until the country source version is done processing')
        if not self.test_mode:
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
                    time.sleep(15)
        else:
            self.vlog(1, 'SKIPPING: New version not created in test mode...')

        # STEP 11 of 12: Generate all references for all country collections
        self.vlog(1, '**** STEP 11 of 12: Generate collection references')
        refgen = datimimapreferencegenerator.DatimImapReferenceGenerator(
            oclenv=self.oclenv, oclapitoken=self.oclapitoken, imap_input=imap_input)
        country_source_export = ocldev.oclexport.OclExportFactory.load_export(
            repo_version_url=country_next_version_url, oclapitoken=self.oclapitoken)
        ref_import_list = refgen.process_imap(country_source_export=country_source_export)
        pprint.pprint(ref_import_list)

        # STEP 12 of 12: Import new collection references
        self.vlog(1, '**** STEP 12 of 12: Import new collection references')

        # 12a. Get the list of unique collection IDs
        unique_collection_ids = []
        for ref_import in ref_import_list:
            if ref_import['collection'] not in unique_collection_ids:
                unique_collection_ids.append(ref_import['collection'])

        # 12b. Delete existing references for each unique collection
        self.vlog(1, 'Clearing existing collection references...')
        for collection_id in unique_collection_ids:
            collection_url = '%s/orgs/%s/collections/%s/' % (
                self.oclenv, imap_input.country_org, collection_id)
            self.vlog(1, '  - %s' % collection_url)
            self.clear_collection_references(collection_url=collection_url)

        # 12c. Import new references for the collection
        self.vlog(1, 'Importing %s batch(es) of collection references...' % len(ref_import_list))
        importer = ocldev.oclfleximporter.OclFlexImporter(
            input_list=ref_import_list, api_token=self.oclapitoken, api_url_root=self.oclenv,
            test_mode=self.test_mode, verbosity=self.verbosity, import_delay=5)
        importer.process()
        if self.verbosity:
            importer.import_results.display_report()

        # 12d. Create new version for each unique collection
            self.vlog(1, 'Creating new collection versions...')
        for collection_id in unique_collection_ids:
            collection_endpoint = '/orgs/%s/collections/%s/' % (imap_input.country_org, collection_id)
            collection_version_endpoint = '%s%s/' % (collection_endpoint, next_country_version_id)
            self.vlog(1, 'Creating collection version: %s' % collection_version_endpoint)
            datimimap.DatimImapFactory.create_repo_version(
                oclenv=self.oclenv, oclapitoken=self.oclapitoken,
                repo_endpoint=collection_endpoint, repo_version_id=next_country_version_id)

        self.vlog(1, '**** IMAP import process complete!')

    def clear_collection_references(self, collection_url=''):
        """ Clear all references for the specified collection """
        # TOOD: In the future, batch deletes for no more than 25 references at a time
        collection_refs_url = '%sreferences/' % collection_url
        r = requests.get(collection_url, headers=self.oclapiheaders)
        r.raise_for_status()
        collection = r.json()
        refs = []
        for ref in collection['references']:
            refs.append(ref['expression'])
        payload = {"references": refs}
        if refs:
            self.vlog(1, '%s: %s' % (collection_refs_url, json.dumps(payload)))
            r = requests.delete(collection_refs_url, json=payload, headers=self.oclapiheaders)
            r.raise_for_status()
        else:
            self.vlog(1, 'Empty collection. Continuing...')

    @staticmethod
    def get_country_org_dict(country_org='', country_code='', country_name=''):
        """ Get an OCL-formatted dictionary of a country IMAP organization ready to import """
        return {
            'type': 'Organization',
            'id': country_org,
            'name': 'DATIM MOH %s' % country_name,
            'location': country_name,
        }

    @staticmethod
    def get_country_source_dict(country_org='', country_code='', country_name=''):
        """ Get an OCL-formatted dictionary of a country IMAP source ready to import """
        source_name = 'DATIM MOH %s Alignment Indicators' % (country_name)
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
            "supported_locales": "en"
        }
        return source
