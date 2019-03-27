"""
Class to generate a country mapping export for a specified country (e.g. UG) and
period (e.g. FY17). Export follows the format of the country mapping CSV template.

** Fields:
DATIM_Indicator_Category
DATIM_Indicator_ID
DATIM_Disag_ID
DATIM_Disag_Name
Operation - ADD, SUBTRACT
MOH_Indicator_ID
MOH_Indicator_Name
MOH_Disag_ID
MOH_Disag_Name

** Issues:
1. Implement long-term method for populating the indicator category column (currently manually set a custom attribute)
"""
import sys
import json
import os
import requests
import datimbase
import datimimap


class DatimUnknownCountryPeriodError(Exception):
    def __init___(self, message):
        Exception.__init__(self, message)
        self.message = message


class DatimUnknownDatimPeriodError(Exception):
    def __init___(self, message):
        Exception.__init__(self, message)
        self.message = message


class DatimImapExport(datimbase.DatimBase):
    """
    Class to export PEPFAR country mapping metadata stored in OCL in various formats.
    """

    def __init__(self, oclenv='', oclapitoken='', verbosity=0, run_ocl_offline=False):
        datimbase.DatimBase.__init__(self)
        self.verbosity = verbosity
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.run_ocl_offline = run_ocl_offline

        # Prepare the headers
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    def log_settings(self):
        """ Write settings to console """
        self.log(
            '**** I-Map Export Script Settings:',
            'verbosity:', self.verbosity,
            ', oclenv:', self.oclenv,
            ', oclapitoken: <hidden>')
        if self.run_ocl_offline:
            self.log('**** RUNNING OCL IN OFFLINE MODE ****')

    @staticmethod
    def get_format_from_string(format_string, default_fmt='CSV'):
        for fmt in DatimImapExport.DATIM_IMAP_FORMATS:
            if format_string.lower() == fmt.lower():
                return fmt
        return default_fmt

    def get_imap(self, period='', version='', country_org='', country_code=''):
        """
        Fetch JSON exports from OCL and build the IMAP export
        If version is not specified, then the latest released version for the given period will be used.
        For example, if period is 'FY17', and 'FY17.v0' and 'FY17.v1' versions are released in OCL,
        then 'FY17.v1' would be returned.
        If period is not specified, version is ignored and the latest released version of the repository
        is returned regardless of period.
        """

        # Initial validation
        if not country_org:
            msg = 'ERROR: Country organization ID (e.g. "DATIM-MOH-UG") is required, none provided'
            self.vlog(1, msg)
            raise Exception(msg)

        # STEP 1 of 8: Determine the country period, minor version, and repo version ID (e.g. FY18.v0)
        self.vlog(1, '**** STEP 1 of 8: Determine the country period, minor version, and repo version ID')
        country_owner_endpoint = '/orgs/%s/' % country_org
        country_source_endpoint = '%ssources/%s/' % (country_owner_endpoint, self.country_source_id)
        country_source_url = '%s%s' % (self.oclenv, country_source_endpoint)
        if period and version:
            country_version_id = '%s.%s' % (period, version)
        else:
            country_version_id = ''
            country_version = datimimap.DatimImapFactory.get_repo_latest_period_version(
                repo_url=country_source_url, period=period, oclapitoken=self.oclapitoken)
            if not country_version:
                msg = 'ERROR: No valid released version found for country "%s" for period "%s"' % (
                    country_org, period)
                self.vlog(1, msg)
                raise DatimUnknownCountryPeriodError(msg)
            country_version_id = country_version['id']
            period = datimimap.DatimImapFactory.get_period_from_version_id(country_version_id)
        if not period or not country_version_id:
            msg = 'ERROR: No valid and released version found for the specified country'
            self.vlog(1, msg)
            raise DatimUnknownCountryPeriodError(msg)
        self.vlog(1, 'Using version "%s" for country "%s"' % (country_version_id, country_org))

        # STEP 2 of 8: Download PEPFAR/DATIM-MOH source
        self.vlog(1, '**** STEP 2 of 8: Download PEPFAR/DATIM-MOH source for specified period')
        datim_owner_endpoint = '/orgs/%s/' % (self.datim_owner_id)
        datim_source_endpoint = '%ssources/%s/' % (datim_owner_endpoint, self.datim_source_id)
        datim_source_url = '%s%s' % (self.oclenv, datim_source_endpoint)
        datim_version = datimimap.DatimImapFactory.get_repo_latest_period_version(
            repo_url=datim_source_url, period=period, oclapitoken=self.oclapitoken)
        if not datim_version:
            msg = 'ERROR: PEPFAR/DATIM-MOH metadata not defined for period "%s"' % period
            self.vlog(1, msg)
            raise DatimUnknownDatimPeriodError(msg)
        datim_version_id = datim_version['id']
        datim_source_zipfilename = self.endpoint2filename_ocl_export_zip(datim_source_endpoint)
        datim_source_jsonfilename = self.endpoint2filename_ocl_export_json(datim_source_endpoint)
        if not self.run_ocl_offline:
            self.get_ocl_export(
                endpoint=datim_source_endpoint, version=datim_version_id,
                zipfilename=datim_source_zipfilename, jsonfilename=datim_source_jsonfilename)
        else:
            self.vlog(1, 'OCL-OFFLINE: Using local file "%s"...' % datim_source_jsonfilename)
            if os.path.isfile(self.attach_absolute_data_path(datim_source_jsonfilename)):
                self.vlog(1, 'OCL-OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                    datim_source_jsonfilename, os.path.getsize(self.attach_absolute_data_path(datim_source_jsonfilename))))
            else:
                msg = 'ERROR: Could not find offline OCL file "%s"' % datim_source_jsonfilename
                self.vlog(1, msg)
                raise Exception(msg)

        # STEP 3 of 8: Prepare output with the DATIM-MOH indicator+disag structure
        self.vlog(1, '**** STEP 3 of 8: Prepare output with the DATIM-MOH indicator+disag structure')
        indicators = {}
        disaggregates = {}
        with open(self.attach_absolute_data_path(datim_source_jsonfilename), 'rb') as handle_datim_source:
            datim_source = json.load(handle_datim_source)

            # Split up the indicator and disaggregate concepts
            for concept in datim_source['concepts']:
                if concept['concept_class'] == self.concept_class_disaggregate:
                    disaggregates[concept['url']] = concept.copy()
                elif concept['concept_class'] == self.concept_class_indicator:
                    indicators[concept['url']] = concept.copy()
                    indicators[concept['url']]['mappings'] = []

            # Now iterate through the mappings
            for mapping in datim_source['mappings']:
                if mapping['map_type'] == self.map_type_datim_has_option:
                    if mapping['from_concept_url'] not in indicators:
                        msg = 'ERROR: Missing indicator from_concept: %s' % (mapping['from_concept_url'])
                        self.vlog(1, msg)
                        raise Exception(msg)
                    indicators[mapping['from_concept_url']]['mappings'].append(mapping.copy())
                else:
                    self.vlog(1, 'SKIPPING: Unrecognized map type "%s" for mapping: %s' % (mapping['map_type'], str(mapping)))

        # STEP 4 of 8: Download and process country source
        self.vlog(1, '**** STEP 4 of 8: Download and process country source')
        country_source_zipfilename = self.endpoint2filename_ocl_export_zip(country_source_endpoint)
        country_source_jsonfilename = self.endpoint2filename_ocl_export_json(country_source_endpoint)
        if not self.run_ocl_offline:
            self.get_ocl_export(
                endpoint=country_source_endpoint, version=country_version_id,
                zipfilename=country_source_zipfilename, jsonfilename=country_source_jsonfilename)
        else:
            self.vlog(1, 'OCL-OFFLINE: Using local file "%s"...' % country_source_jsonfilename)
            if os.path.isfile(self.attach_absolute_data_path(country_source_jsonfilename)):
                self.vlog(1, 'OCL-OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                    country_source_jsonfilename, os.path.getsize(self.attach_absolute_data_path(country_source_jsonfilename))))
            else:
                msg = 'ERROR: Could not find offline OCL file "%s"' % country_source_jsonfilename
                self.vlog(1, msg)
                raise Exception(msg)
        country_indicators = {}
        country_disaggregates = {}
        with open(self.attach_absolute_data_path(country_source_jsonfilename), 'rb') as handle_country_source:
            country_source = json.load(handle_country_source)
            for concept in country_source['concepts']:
                if concept['concept_class'] == self.concept_class_disaggregate:
                    country_disaggregates[concept['url']] = concept.copy()
                elif concept['concept_class'] == self.concept_class_indicator:
                    country_indicators[concept['url']] = concept.copy()

        # STEP 5 of 8: Download list of country indicator mappings (i.e. collections)
        # TODO: Make this one work offline
        self.vlog(1, '**** STEP 5 of 8: Download list of country indicator mappings (i.e. collections)')
        country_collections_endpoint = '%scollections/' % country_owner_endpoint
        if self.run_ocl_offline:
            self.vlog(1, 'WARNING: Offline not supported here yet. Taking this ship online!')
        country_collections = self.get_ocl_repositories(
            endpoint=country_collections_endpoint, require_external_id=False, active_attr_name=None)

        # STEP 6 of 8: Process one country collection at a time
        self.vlog(1, '**** STEP 6 of 8: Process one country collection at a time')
        for collection_id, collection in country_collections.items():
            collection_zipfilename = self.endpoint2filename_ocl_export_zip(collection['url'])
            collection_jsonfilename = self.endpoint2filename_ocl_export_json(collection['url'])
            if not self.run_ocl_offline:
                try:
                    self.get_ocl_export(
                        endpoint=collection['url'], version=country_version_id,
                        zipfilename=collection_zipfilename, jsonfilename=collection_jsonfilename)
                except requests.exceptions.HTTPError:
                    # collection or collection version does not exist, so we can safely throw it out
                    continue
            else:
                self.vlog(1, 'OCL-OFFLINE: Using local file "%s"...' % collection_jsonfilename)
                if os.path.isfile(self.attach_absolute_data_path(collection_jsonfilename)):
                    self.vlog(1, 'OCL-OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                        collection_jsonfilename, os.path.getsize(self.attach_absolute_data_path(collection_jsonfilename))))
                else:
                    msg = 'ERROR: Could not find offline OCL file "%s"' % collection_jsonfilename
                    self.vlog(1, msg)
                    raise Exception(msg)
            operations = []
            datim_pair_mapping = None
            datim_indicator_url = None
            datim_disaggregate_url = None
            with open(self.attach_absolute_data_path(collection_jsonfilename), 'rb') as handle_country_collection:
                country_collection = json.load(handle_country_collection)

                # Organize the mappings between operations and the datim indicator+disag pair
                for mapping in country_collection['mappings']:
                    if mapping['map_type'] == self.map_type_country_has_option:
                        if mapping['from_concept_url'] in indicators and mapping['to_concept_url'] in disaggregates:
                            # we're good - the from and to concepts are part of the PEPFAR/DATIM_MOH source
                            datim_pair_mapping = mapping.copy()
                            datim_indicator_url = mapping['from_concept_url']
                            datim_disaggregate_url = mapping['to_concept_url']
                        else:
                            # we're not good. not good at all
                            msg = 'ERROR: The from_concept or to_concept of the "%s" mapping in collection "%s" are not part of "%s" version "%s": %s ' % (
                                self.map_type_country_has_option, collection_id, self.datim_source_id, period, str(mapping))
                            self.vlog(1, msg)
                            raise Exception(msg)
                    elif mapping['map_type'] in self.DATIM_IMAP_OPERATIONS:
                        if (mapping['from_concept_url'] in country_indicators and
                                (mapping['to_concept_url'] in country_disaggregates or
                                 mapping['to_concept_url'] == datimbase.DatimBase.NULL_DISAG_ENDPOINT)):
                            # we're good - we have a valid mapping operation
                            operations.append(mapping)
                        else:
                            # also not good - we are missing the country indicator or disag concepts
                            msg = 'ERROR: From or to concept not found in country source for operation mapping: %s' % (
                                str(mapping))
                            self.vlog(1, msg)
                            raise Exception(msg)
                    else:
                        # also not good - we don't know what to do with this map type
                        msg = 'ERROR: Invalid map_type "%s" in collection "%s".' % (mapping['map_type'], collection_id)
                        self.vlog(1, msg)
                        raise Exception(msg)

            # Save the set of operations in the relevant datim indicator mapping
            for datim_indicator_mapping in indicators[datim_indicator_url]['mappings']:
                if datim_indicator_mapping['from_concept_url'] == datim_indicator_url and datim_indicator_mapping['to_concept_url'] == datim_disaggregate_url:
                    datim_indicator_mapping['operations'] = operations

        # STEP 7 of 8: Cache the results
        self.vlog(1, '**** STEP 7 of 8: SKIPPING -- Cache the results')

        # STEP 8 of 8: Convert to tabular format
        self.vlog(1, '**** STEP 8 of 8: Convert to tabular format')
        rows = []
        for indicator_id, indicator in indicators.items():
            for mapping in indicator['mappings']:
                if 'operations' in mapping and mapping['operations']:
                    # Country has mapped content to this datim indicator+disag pair
                    for operation in mapping['operations']:
                        row = {}
                        row['DATIM_Indicator_Category'] = ''
                        if 'extras' in indicator and type(indicator['extras']) is dict and 'indicator_category_code' in indicator['extras']:
                            row['DATIM_Indicator_Category'] = indicator['extras']['indicator_category_code']
                        row['DATIM_Indicator_ID'] = indicator['id']
                        row['DATIM_Disag_ID'] = mapping['to_concept_code']
                        row['DATIM_Disag_Name'] = mapping['to_concept_name']
                        row['Operation'] = self.map_type_to_operator(operation['map_type'])
                        row['MOH_Indicator_ID'] = operation['from_concept_code']
                        row['MOH_Indicator_Name'] = operation['from_concept_name']
                        row['MOH_Disag_ID'] = operation['to_concept_code']
                        row['MOH_Disag_Name'] = operation['to_concept_name']
                        rows.append(row)
                else:
                    # Country has not defined any mappings for this datim indicator+disag pair
                    row = {}
                    row['DATIM_Indicator_Category'] = ''
                    if 'extras' in indicator and type(indicator['extras']) is dict and 'indicator_category_code' in indicator['extras']:
                        row['DATIM_Indicator_Category'] = indicator['extras']['indicator_category_code']
                    row['DATIM_Indicator_ID'] = indicator['id']
                    row['DATIM_Disag_ID'] = mapping['to_concept_code']
                    row['DATIM_Disag_Name'] = mapping['to_concept_name']
                    row['Operation'] = ''
                    row['MOH_Indicator_ID'] = ''
                    row['MOH_Indicator_Name'] = ''
                    row['MOH_Disag_ID'] = ''
                    row['MOH_Disag_Name'] = ''
                    rows.append(row)

        # Generate and return the IMAP object
        return datimimap.DatimImap(imap_data=rows, country_code=country_code, country_org=country_org, period=period)

    def map_type_to_operator(self, map_type):
        return map_type.replace(' OPERATION', '')
