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
import requests
import json
import os
import csv
from pprint import pprint
from datimbase import DatimBase


class DatimImapExport(DatimBase):
    """
    Class to export PEPFAR country mapping metadata stored in OCL in various formats.
    """

    DATIM_IMAP_OPERATION_ADD = 'ADD OPERATION'
    DATIM_IMAP_OPERATION_ADD_HALF = 'ADD HALF OPERATION'
    DATIM_IMAP_OPERATION_SUBTRACT = 'SUBTRACT OPERATION'
    DATIM_IMAP_OPERATION_SUBTRACT_HALF = 'SUBTRACT HALF OPERATION'
    DATIM_IMAP_OPERATIONS = [
        DATIM_IMAP_OPERATION_ADD,
        DATIM_IMAP_OPERATION_ADD_HALF,
        DATIM_IMAP_OPERATION_SUBTRACT,
        DATIM_IMAP_OPERATION_SUBTRACT_HALF
    ]

    concept_class_indicator = 'Indicator'
    concept_class_disaggregate = 'Disaggregate'
    map_type_datim_has_option = 'Has Option'
    map_type_country_has_option = 'DATIM HAS OPTION'

    DATIM_IMAP_FORMAT_CSV = 'CSV'
    DATIM_IMAP_FORMAT_JSON = 'JSON'
    DATIM_IMAP_FORMATS = [
            DATIM_IMAP_FORMAT_CSV,
            DATIM_IMAP_FORMAT_JSON,
        ]

    datim_owner_id = 'PEPFAR'
    datim_owner_type = 'Organization'
    datim_source_id = 'DATIM-MOH'
    country_owner = 'DATIM-MOH-xx'
    country_owner_type = 'Organization'
    country_source_id = 'DATIM-Alignment-Indicators'

    null_disag_url = '/orgs/PEPFAR/sources/DATIM-MOH/concepts/null_disag/'
    null_disag_name = 'Null Disaggregation'

    imap_fields = [
        'DATIM_Indicator_Category',
        'DATIM_Indicator_ID',
        'DATIM_Disag_ID',
        'DATIM_Disag_Name',
        'Operation',
        'MOH_Indicator_ID',
        'MOH_Indicator_Name',
        'MOH_Disag_ID',
        'MOH_Disag_Name',
    ]

    def __init__(self, oclenv='', oclapitoken='', verbosity=0, run_ocl_offline=False):
        DatimBase.__init__(self)
        self.verbosity = verbosity
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.run_ocl_offline = run_ocl_offline

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

    def get(self, format='JSON', period='FY17', country_org=''):
        """ Fetch exports from OCL and build the export """

        # Initial validation
        if format not in self.DATIM_IMAP_FORMATS:
            self.log('ERROR: Unrecognized format "%s"' % (format))
            exit(1)
        if not period:
            self.log('ERROR: Period identifier (e.g. "FY17") is required, none provided')
            exit(1)
        if not country_org:
            self.log('ERROR: Country organization ID (e.g. "DATIM-MOH-UG") is required, none provided')
            exit(1)

        # STEP 1 of 8: Download DATIM-MOH source
        self.vlog(1, '**** STEP 1 of 8: Download DATIM-MOH source')
        datim_owner_endpoint = '/orgs/%s/' % (self.datim_owner_id)
        datim_source_endpoint = '%ssources/%s/' % (datim_owner_endpoint, self.datim_source_id)
        datim_source_zipfilename = self.endpoint2filename_ocl_export_zip(datim_source_endpoint)
        datim_source_jsonfilename = self.endpoint2filename_ocl_export_json(datim_source_endpoint)
        if not self.run_ocl_offline:
            datim_source_export = self.get_ocl_export(
                    endpoint=datim_source_endpoint, version=period,
                    zipfilename=datim_source_zipfilename, jsonfilename=datim_source_jsonfilename)
        else:
            self.vlog(1, 'OCL-OFFLINE: Using local file "%s"...' % datim_source_jsonfilename)
            if os.path.isfile(self.attach_absolute_data_path(datim_source_jsonfilename)):
                self.vlog(1, 'OCL-OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                    datim_source_jsonfilename, os.path.getsize(self.attach_absolute_data_path(datim_source_jsonfilename))))
            else:
                self.log('ERROR: Could not find offline OCL file "%s". Exiting...' % datim_source_jsonfilename)
                sys.exit(1)

        # STEP 2 of 8: Prepare output with the DATIM-MOH indicator+disag structure
        self.vlog(1, '**** STEP 2 of 8: Prepare output with the DATIM-MOH indicator+disag structure')
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
                        self.log('ERROR: Missing indicator from_concept: %s' % (mapping['from_concept_url']))
                        exit(1)
                    indicators[mapping['from_concept_url']]['mappings'].append(mapping.copy())
                else:
                    self.log('SKIPPING: Unrecognized map type "%s" for mapping: %s' % (mapping['map_type'], str(mapping)))

        # STEP 3 of 8: Download and process country source
        self.vlog(1, '**** STEP 3 of 8: Download and process country source')
        country_owner_endpoint = '/orgs/%s/' % (country_org)
        country_source_endpoint = '%ssources/%s/' % (country_owner_endpoint, self.country_source_id)
        country_source_zipfilename = self.endpoint2filename_ocl_export_zip(country_source_endpoint)
        country_source_jsonfilename = self.endpoint2filename_ocl_export_json(country_source_endpoint)
        if not self.run_ocl_offline:
            country_source_export = self.get_ocl_export(
                endpoint=country_source_endpoint, version=period,
                zipfilename=country_source_zipfilename, jsonfilename=country_source_jsonfilename)
        else:
            self.vlog(1, 'OCL-OFFLINE: Using local file "%s"...' % country_source_jsonfilename)
            if os.path.isfile(self.attach_absolute_data_path(country_source_jsonfilename)):
                self.vlog(1, 'OCL-OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                    country_source_jsonfilename, os.path.getsize(self.attach_absolute_data_path(country_source_jsonfilename))))
            else:
                self.log('ERROR: Could not find offline OCL file "%s". Exiting...' % country_source_jsonfilename)
                sys.exit(1)
        country_indicators = {}
        country_disaggregates = {}
        with open(self.attach_absolute_data_path(country_source_jsonfilename), 'rb') as handle_country_source:
            country_source = json.load(handle_country_source)
            for concept in country_source['concepts']:
                if concept['concept_class'] == self.concept_class_disaggregate:
                    country_disaggregates[concept['url']] = concept.copy()
                elif concept['concept_class'] == self.concept_class_indicator:
                    country_indicators[concept['url']] = concept.copy()

        # STEP 4 of 8: Download list of country indicator mappings (i.e. collections)
        # TODO: Make this one work offline
        self.vlog(1, '**** STEP 4 of 8: Download list of country indicator mappings (i.e. collections)')
        country_collections_endpoint = '%scollections/' % (country_owner_endpoint)
        if self.run_ocl_offline:
            self.vlog('WARNING: Offline not supported here yet...')
        country_collections = self.get_ocl_repositories(endpoint=country_collections_endpoint,
                                                        require_external_id=False,
                                                        active_attr_name=None)

        # STEP 5 of 8: Process one country collection at a time
        self.vlog(1, '**** STEP 5 of 8: Process one country collection at a time')
        for collection_id, collection in country_collections.items():
            collection_zipfilename = self.endpoint2filename_ocl_export_zip(collection['url'])
            collection_jsonfilename = self.endpoint2filename_ocl_export_json(collection['url'])
            if not self.run_ocl_offline:
                collection_export = self.get_ocl_export(
                    endpoint=collection['url'], version=period,
                    zipfilename=collection_zipfilename, jsonfilename=collection_jsonfilename)
            else:
                self.vlog(1, 'OCL-OFFLINE: Using local file "%s"...' % collection_jsonfilename)
                if os.path.isfile(self.attach_absolute_data_path(collection_jsonfilename)):
                    self.vlog(1, 'OCL-OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                        collection_jsonfilename, os.path.getsize(self.attach_absolute_data_path(collection_jsonfilename))))
                else:
                    self.log('ERROR: Could not find offline OCL file "%s". Exiting...' % collection_jsonfilename)
                    sys.exit(1)
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
                            self.log('ERROR: The from_concept or to_concept of the "%s" mapping in collection "%s" are not part of "%s" version "%s": %s ' % (self.map_type_country_has_option, collection_id, datim_source_id, period, str(mapping)))
                            exit(1)
                    elif mapping['map_type'] in self.DATIM_IMAP_OPERATIONS:
                        if mapping['from_concept_url'] in country_indicators and (mapping['to_concept_url'] in country_disaggregates or mapping['to_concept_url'] == self.null_disag_url):
                            # we're good - we have a valid mapping operation
                            operations.append(mapping)
                        else:
                            # also not good - we are missing the country indicator or disag concepts
                            self.log('ERROR: From or to concept not found in country source for operation mapping: %s' % (str(mapping)))
                            exit(1)
                    else:
                        # also not good - we don't know what to do with this map type
                        self.log('ERROR: Invalid map_type "%s" in collection "%s".' % (mapping['map_type'], collection_id))
                        exit(1)

            # Save the set of operations in the relevant datim indicator mapping
            for datim_indicator_mapping in indicators[datim_indicator_url]['mappings']:
                if datim_indicator_mapping['from_concept_url'] == datim_indicator_url and datim_indicator_mapping['to_concept_url'] == datim_disaggregate_url:
                    datim_indicator_mapping['operations'] = operations

        # STEP 6 of 8: Cache the results
        self.vlog(1, '**** STEP 6 of 8: Cache the results')

        # STEP 7 of 8: Convert to tabular format
        self.vlog(1, '**** STEP 7 of 8: Convert to tabular format')
        rows = []
        for indicator_id, indicator in indicators.items():
            for mapping in indicator['mappings']:
                if 'operations' in mapping and mapping['operations']:
                    # Country has mapped content to this datim indicator+disag pair
                    for operation in mapping['operations']:
                        row = {}
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

        # STEP 8 of 8: Output in requested format
        self.vlog(1, '**** STEP 8 of 8: Output in requested format')
        if format == self.DATIM_IMAP_FORMAT_CSV:
            writer = csv.DictWriter(sys.stdout, fieldnames=self.imap_fields)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        elif format == self.DATIM_IMAP_FORMAT_JSON:
            pprint(rows)

    def map_type_to_operator(self, map_type):
        return map_type.replace(' OPERATION', '')
