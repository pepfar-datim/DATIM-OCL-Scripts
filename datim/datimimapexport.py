"""
Class to generate a country mapping export for a specified country (e.g. UG) and
period (e.g. FY17). Export follows the format of the country mapping CSV template.

** Example IMAP values:
    DATIM_Indicator_Category - HTS_TST
    DATIM_Indicator_ID - HTS_TST_N_MOH_Age_Agg_Sex_Result
    DATIM_Disag_ID - FSmIqIsgheB
    DATIM_Disag_Name - <15, Female, Negative
    Operation - ADD or SUBTRACT
    MOH_Indicator_ID - INDHTC-108c
    MOH_Indicator_Name - HIV negative Children (0-14years)
    MOH_Disag_ID - Females
    MOH_Disag_Name - Adults (14+) initiated ART
    Classification - course

** Issues:
1. Implement long-term method for populating the indicator category column (currently manually set a custom attribute)
"""
import json
from . import datimbase
from . import datimimap
from . import datimimapimport
from . import datimsyncmohhelper
import utils.timer
import ocldev.oclfleximporter


class DatimUnknownCountryPeriodError(Exception):
    """ DatimUnknownCountryPeriodError """
    def __init___(self, message):
        Exception.__init__(self, message)
        self.message = message


class DatimUnknownDatimPeriodError(Exception):
    """ DatimUnknownDatimPeriodError """
    def __init___(self, message):
        Exception.__init__(self, message)
        self.message = message


class DatimImapExport(datimbase.DatimBase):
    """
    Class to export PEPFAR country mapping metadata stored in OCL in various formats.
    """

    def __init__(self, oclenv='', oclapitoken='', verbosity=0, run_ocl_offline=False):
        """
        Initialize an DatimImapExport object
        :param oclenv: Base URL for the OCL environment with hanging slash omitted,
            e.g. https://api.openconceptlab.org
        :param oclapitoken: API token of the OCL user account making the export request
        :param verbosity: Verbosity level (0=none, 1=some, 2=tons)
        :param run_ocl_offline:
        """
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
        """
        Get one of the DatimImapExport.DATIM_IMAP_FORMAT constants from a string
        :param format_string:
        :param default_fmt:
        :return:
        """
        for fmt in DatimImapExport.DATIM_IMAP_FORMATS:
            if format_string.lower() == fmt.lower():
                return fmt
        return default_fmt

    def get_imap(self, period='', version='', country_org='', country_code='', ocl_api_version='v2'):
        """
        Fetch JSON exports from OCL and build the IMAP export
        If version is not specified, then the latest released version for the given period will be used.
        For example, if period is 'FY17', and 'FY17.v0' and 'FY17.v1' versions are released in OCL,
        then 'FY17.v1' would be returned.
        If period is not specified, version is ignored and the latest released version of the repository
        is returned regardless of period.
        :param period: FY18, FY19
        :param version: (Optional) Specify country minor version number (e.g. v3, v4). Alternatively set to "latest"
            (or simply leave blank) to automatically retrieve the latest version for the specified period.
        :param country_org: DATIM-MOH-UA-FY19
        :param country_code: UA
        :param ocl_api_version: v1 or v2
        :return:
        """

        # Initial validation
        if not country_org:
            msg = 'ERROR: Country organization ID (e.g. "DATIM-MOH-UG-FY18") is required, none provided'
            self.vlog(1, msg)
            raise Exception(msg)
        if not period:
            msg = 'ERROR: Period (e.g. "FY18") is required, none provided'
            self.vlog(1, msg)
            raise Exception(msg)

        # STEP 1 of 8: Make sure an import for same country+period is not underway
        imap_timer = utils.timer.Timer()
        imap_timer.start()
        self.vlog(1, '**** STEP 1 of 8: Make sure an import for same country+period is not underway')
        status_filter = ['PENDING', 'STARTED']
        queued_imports = ocldev.oclfleximporter.OclBulkImporter.get_queued_imports(
            api_url_root=self.oclenv, api_token=self.oclapitoken, queue=country_org,
            status_filter=status_filter)
        if queued_imports:
            err_msg = 'Cannot export IMAP because an import is being processed for this country and period: %s' % (
                country_org)
            raise datimimapimport.ImapCountryLockedForPeriodError(err_msg)
        imap_timer.lap(label='STEP 1: Make sure an import for same country+period is not underway')

        # STEP 2 of 8: Determine the country period, minor version, & repo version ID (eg FY18.v0)
        self.vlog(1, '**** STEP 2 of 8: Determine country period, minor version, & repo version ID')
        country_owner_endpoint = '/orgs/%s/' % country_org  # e.g. /orgs/DATIM-MOH-RW-FY18/
        country_source_endpoint = '%ssources/%s/' % (
            country_owner_endpoint, self.DATIM_MOH_COUNTRY_SOURCE_ID)
        country_source_url = '%s%s' % (self.oclenv, country_source_endpoint)
        if period and version:
            country_version_id = '%s.%s' % (period, version)
            country_minor_version = version
        else:
            country_version = datimimap.DatimImapFactory.get_repo_latest_period_version(
                repo_url=country_source_url, period=period, oclapitoken=self.oclapitoken)
            if not country_version:
                msg = 'ERROR: No valid released version found for country "%s" for period "%s"' % (
                    country_org, period)
                self.vlog(1, msg)
                raise DatimUnknownCountryPeriodError(msg)
            country_version_id = country_version['id']
            period = datimimap.DatimImapFactory.get_period_from_version_id(country_version_id)
            country_minor_version = datimimap.DatimImapFactory.get_minor_version_from_version_id(
                country_version_id)
        if not period or not country_version_id:
            msg = 'ERROR: No valid and released version found for country org "%s"' % country_org
            self.vlog(1, msg)
            raise DatimUnknownCountryPeriodError(msg)
        self.vlog(1, 'Using version "%s" for country "%s"' % (country_version_id, country_org))
        imap_timer.lap(label='STEP 2: Parse IMAP export parameters')

        # STEP 3 of 8: Download DATIM-MOH-xx source for specified period (e.g. DATIM-MOH-FY18)
        self.vlog(1, '**** STEP 3 of 8: Download DATIM-MOH source for specified period (e.g. DATIM-MOH-FY18)')
        datim_moh_source_id = datimbase.DatimBase.get_datim_moh_source_id(period)
        datim_source_endpoint = datimbase.DatimBase.get_datim_moh_source_endpoint(period)
        datim_source_url = '%s%s' % (self.oclenv, datim_source_endpoint)
        datim_version = datimimap.DatimImapFactory.get_repo_latest_period_version(
            repo_url=datim_source_url, period=period, oclapitoken=self.oclapitoken)
        if not datim_version:
            msg = 'ERROR: %s does not exist or no valid repository version defined for period (e.g. FY19.v1)' % (
                datim_source_endpoint)
            self.vlog(1, msg)
            raise DatimUnknownDatimPeriodError(msg)
        datim_version_id = datim_version['id']
        datim_source_zip_filename = self.endpoint2filename_ocl_export_zip(datim_source_endpoint)
        datim_source_json_filename = self.endpoint2filename_ocl_export_json(datim_source_endpoint)
        if not self.run_ocl_offline:
            self.get_ocl_export(
                endpoint=datim_source_endpoint, version=datim_version_id,
                zipfilename=datim_source_zip_filename, jsonfilename=datim_source_json_filename)
        else:
            self.does_offline_data_file_exist(datim_source_json_filename, exit_if_missing=True)
        imap_timer.lap(label='STEP 3: Download DATIM-MOH-xx source')

        # STEP 4 of 8: Pre-process DATIM-MOH indicator+disag structure
        self.vlog(1, '**** STEP 4 of 8: Pre-process DATIM-MOH indicator+disag structure')
        indicators = {}
        disaggregates = {}
        with open(self.attach_absolute_data_path(datim_source_json_filename), 'rb') as handle_datim_source:
            datim_source = json.load(handle_datim_source)

            # Split up the indicator and disaggregate concepts
            for concept in datim_source['concepts']:
                if concept['concept_class'] == self.DATIM_MOH_CONCEPT_CLASS_DISAGGREGATE:
                    disaggregates[concept['url']] = concept.copy()
                elif concept['concept_class'] == self.DATIM_MOH_CONCEPT_CLASS_DE:
                    indicators[concept['url']] = concept.copy()
                    indicators[concept['url']]['mappings'] = []

            # Now iterate through the mappings
            for mapping in datim_source['mappings']:
                if mapping['map_type'] == self.DATIM_MOH_MAP_TYPE_HAS_OPTION:
                    if mapping['from_concept_url'] not in indicators:
                        msg = 'ERROR: Missing indicator from_concept: %s' % (
                            mapping['from_concept_url'])
                        self.vlog(1, msg)
                        raise Exception(msg)
                    indicators[mapping['from_concept_url']]['mappings'].append(mapping.copy())
                else:
                    self.vlog(1, 'SKIPPING: Unrecognized map type "%s" for mapping: %s' % (
                        mapping['map_type'], str(mapping)))
        imap_timer.lap(label='STEP 4: Pre-process DATIM-MOH indicator+disag structure')

        # STEP 5 of 8: Download and process country source
        # NOTE: This returns the individual country concepts and mappings
        self.vlog(1, '**** STEP 5 of 8: Download and process country source')
        country_source_zip_filename = self.endpoint2filename_ocl_export_zip(country_source_endpoint)
        country_source_json_filename = self.endpoint2filename_ocl_export_json(
            country_source_endpoint)
        if not self.run_ocl_offline:
            self.get_ocl_export(
                endpoint=country_source_endpoint, version=country_version_id,
                zipfilename=country_source_zip_filename, jsonfilename=country_source_json_filename)
        else:
            self.does_offline_data_file_exist(country_source_json_filename, exit_if_missing=True)
        country_indicators = {}
        country_disaggregates = {}
        with open(self.attach_absolute_data_path(country_source_json_filename), 'rb') as handle_country_source:
            country_source = json.load(handle_country_source)
            for concept in country_source['concepts']:
                if concept['concept_class'] == self.DATIM_MOH_CONCEPT_CLASS_DISAGGREGATE:
                    country_disaggregates[concept['url']] = concept.copy()
                elif concept['concept_class'] == self.DATIM_MOH_CONCEPT_CLASS_DE:
                    country_indicators[concept['url']] = concept.copy()
        imap_timer.lap(label='STEP 5: Download and process country source')

        # STEP 6 of 8: Async download of country indicator+disag collections
        # NOTE: This returns the collections that define how individual concepts/mappings from the
        # country source combine to map country indicator+disag pairs to DATIM indicator+disag pairs
        self.vlog(1, '**** STEP 6 of 8: Async download of country indicator+disag mappings')
        country_collections_endpoint = '%scollections/' % country_owner_endpoint
        if self.run_ocl_offline:
            self.vlog(1, 'WARNING: Offline not supported here yet. Taking this ship online!')
        country_collections = self.get_ocl_exports_async(
            endpoint=country_collections_endpoint, period=period, version=country_minor_version)
        imap_timer.lap(label='STEP 6: Async download of country indicator+disag mappings')

        # STEP 7 of 8: Process one country collection at a time
        self.vlog(1, '**** STEP 7 of 8: Process one country collection at a time')
        datim_moh_null_disag_endpoint = datimbase.DatimBase.get_datim_moh_null_disag_endpoint(period)
        for collection_version_export_url, collection_version in list(country_collections.items()):
            collection_id = collection_version['collection']['id']
            operations = []
            datim_indicator_url = None
            datim_disaggregate_url = None

            # Organize the mappings between operations and the datim indicator+disag pair
            for mapping in collection_version['mappings']:
                if mapping['map_type'] == self.DATIM_MOH_MAP_TYPE_COUNTRY_OPTION:
                    if mapping['from_concept_url'] in indicators and mapping['to_concept_url'] in disaggregates:
                        # we're good - the from and to concepts are part of the PEPFAR/DATIM_MOH source
                        # JP 2019-08-22 not currently using: datim_pair_mapping = mapping.copy()
                        datim_indicator_url = mapping['from_concept_url']
                        datim_disaggregate_url = mapping['to_concept_url']
                    else:
                        # uhoh this is no good -- indicator or disag not defined in the PEPFAR source version
                        if mapping['from_concept_url'] not in indicators:
                            msg = 'ERROR: from_concept "%s" of the "%s" mapping in collection "%s" is not part of "%s" version "%s": %s' % (
                                mapping['from_concept_url'], self.DATIM_MOH_MAP_TYPE_COUNTRY_OPTION, collection_id,
                                datim_moh_source_id, period, str(mapping))
                        elif mapping['to_concept_url'] not in disaggregates:
                            msg = 'ERROR: to_concept "%s" of the "%s" mapping in collection "%s" is not part of "%s" version "%s": %s' % (
                                mapping['to_concept_url'], self.DATIM_MOH_MAP_TYPE_COUNTRY_OPTION, collection_id,
                                datim_moh_source_id, period, str(mapping))
                        self.vlog(1, msg)
                        raise Exception(msg)
                elif mapping['map_type'] in self.DATIM_IMAP_OPERATIONS:
                    if (mapping['from_concept_url'] in country_indicators and
                            (mapping['to_concept_url'] in country_disaggregates or
                             mapping['to_concept_url'] == datim_moh_null_disag_endpoint)):
                        # we're good - we have a valid mapping operation
                        operations.append(mapping)
                    else:
                        # uhoh. this is no good - we are missing the country indicator or disag concept
                        if mapping['from_concept_url'] not in country_indicators:
                            msg = 'ERROR: from_concept "%s" not found in country source for operation mapping: %s' % (
                                mapping['from_concept_url'], str(mapping))
                        elif (mapping['to_concept_url'] not in country_disaggregates and
                              mapping['to_concept_url'] != datim_moh_null_disag_endpoint):
                            msg = 'ERROR: to_concept "%s" not found in country source for operation mapping: %s' % (
                                mapping['to_concept_url'], str(mapping))
                        self.vlog(1, msg)
                        raise Exception(msg)
                else:
                    # also not good - we don't know what to do with this map type
                    msg = 'ERROR: Invalid map_type "%s" in collection "%s".' % (mapping['map_type'], collection_id)
                    self.vlog(1, msg)
                    raise Exception(msg)

            # Save set of operations in relevant datim indicator mapping, or skip if indicator has no mappings
            if datim_indicator_url in indicators:
                for datim_indicator_mapping in indicators[datim_indicator_url]['mappings']:
                    if (datim_indicator_mapping['from_concept_url'] == datim_indicator_url and
                            datim_indicator_mapping['to_concept_url'] == datim_disaggregate_url):
                        datim_indicator_mapping['operations'] = operations
        imap_timer.lap(label='STEP 7: Process one country collection at a time')

        # STEP 8 of 8: Convert to tabular format
        self.vlog(1, '**** STEP 8 of 8: Convert to tabular format')
        if ocl_api_version == 'v1':
            from_concept_name_field = 'from_concept_name'
            to_concept_name_field = 'to_concept_name'
        else:
            from_concept_name_field = 'from_concept_name_resolved'
            to_concept_name_field = 'to_concept_name_resolved'
        rows = []
        for indicator_id, indicator in list(indicators.items()):
            for mapping in indicator['mappings']:
                row_base = {
                    datimimap.DatimImap.IMAP_FIELD_DATIM_INDICATOR_CATEGORY: '',
                    datimimap.DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID: indicator['id'],
                    datimimap.DatimImap.IMAP_FIELD_DATIM_DISAG_ID: mapping['to_concept_code'],
                    datimimap.DatimImap.IMAP_FIELD_DATIM_DISAG_NAME: mapping[to_concept_name_field],
                    datimimap.DatimImap.IMAP_FIELD_OPERATION: '',
                    datimimap.DatimImap.IMAP_FIELD_MOH_INDICATOR_ID: '',
                    datimimap.DatimImap.IMAP_FIELD_MOH_INDICATOR_NAME: '',
                    datimimap.DatimImap.IMAP_FIELD_MOH_DISAG_ID: '',
                    datimimap.DatimImap.IMAP_FIELD_MOH_DISAG_NAME: '',
                    datimimap.DatimImap.IMAP_FIELD_MOH_CLASSIFICATION: '',
                }
                if 'extras' in indicator and type(indicator['extras']) is dict:
                    if datimimap.DatimImap.IMAP_INDICATOR_CATEGORY_CUSTOM_ATTRIBUTE in indicator['extras']:
                        row_base[datimimap.DatimImap.IMAP_FIELD_DATIM_INDICATOR_CATEGORY] = indicator['extras'][
                            datimimap.DatimImap.IMAP_INDICATOR_CATEGORY_CUSTOM_ATTRIBUTE]

                # Set the classification attribute for this data element+disag pair
                if mapping['to_concept_url'] in disaggregates and 'extras' in disaggregates[mapping['to_concept_url']]:
                    # Classification for FY21 and forward is set by a disag custom attribute
                    classification =  disaggregates[mapping['to_concept_url']]['extras'].get('classification')
                if not classification:
                    # If we can't get classification from a disag custom attribute, try to evaluate
                    # it from the data element code. This method was needed for FY18-FY20.
                    classification = datimsyncmohhelper.DatimSyncMohHelper.get_disag_classification(
                        period=period, de_code=indicator['id'], de_uid=indicator['external_id'],
                        coc_name=mapping[to_concept_name_field])
                row_base[datimimap.DatimImap.IMAP_FIELD_MOH_CLASSIFICATION] = classification

                moh_disag = mapping['from_concept_url']
                if 'operations' in mapping and mapping['operations']:
                    # Country has mapped content to this datim indicator+disag pair
                    for operation in mapping['operations']:
                        row = row_base.copy()
                        if 'operations' in mapping and mapping['operations']:
                            row[datimimap.DatimImap.IMAP_FIELD_OPERATION] = DatimImapExport.map_type_to_operator(
                                operation['map_type'])
                            row[datimimap.DatimImap.IMAP_FIELD_MOH_INDICATOR_ID] = DatimImapExport.get_clean_indicator_id(
                                operation['from_concept_code'])
                            row[datimimap.DatimImap.IMAP_FIELD_MOH_INDICATOR_NAME] = operation[from_concept_name_field]
                            row[datimimap.DatimImap.IMAP_FIELD_MOH_DISAG_ID] = DatimImapExport.get_clean_disag_id(
                                operation['to_concept_code'])
                            row[datimimap.DatimImap.IMAP_FIELD_MOH_DISAG_NAME] = operation[to_concept_name_field]
                        rows.append(row)
                else:
                    # Country has not mapped to this indicator+disag pair, so just add the blank row
                    rows.append(row_base.copy())

        # Stop the timer
        imap_timer.stop(label='STEP 8')

        # Display debug information
        self.vlog(2, '**** IMAP EXPORT SUMMARY')
        self.vlog(2, '** IMAP export time breakdown:\n', imap_timer)

        # Generate and return the IMAP object
        return datimimap.DatimImap(imap_data=rows, country_code=country_code, country_org=country_org,
                                   period=period, version=country_version_id)

    @staticmethod
    def get_clean_disag_id(disag_id):
        """ Cleans a disag ID by removing the "disag-" prefix """
        return DatimImapExport.remove_prefix_if_exists(
            disag_id, datimimap.DatimImap.IMAP_MOH_DISAG_ID_PREFIX)

    @staticmethod
    def get_clean_indicator_id(disag_id):
        """ Cleans an indicator ID by removing the "de-" prefix """
        return DatimImapExport.remove_prefix_if_exists(
            disag_id, datimimap.DatimImap.IMAP_MOH_DATA_ELEMENT_ID_PREFIX)

    @staticmethod
    def remove_prefix_if_exists(original_string, prefix):
        """ Removes a prefix from a string if present """
        if original_string[:len(prefix)] == prefix:
            return original_string[len(prefix):]
        return original_string

    @staticmethod
    def map_type_to_operator(map_type):
        """
        Convert OCL Map Type to an IMAP operator
        If map_type == "ADD OPERATOR" then operator = 'ADD'
        """
        return map_type.replace(datimimap.DatimImap.IMAP_MOH_MAP_TYPE_OPERATION_POSTFIX, '')
