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
import csv
import pprint
import settings
import datimbase
import datimimap
import ocldev.oclcsvtojsonconverter

class DatimImapImport(datimbase.DatimBase):
    """
    Class to import PEPFAR country mapping metadata from a CSV file into OCL.
    """

    def __init__(self, oclenv='', oclapitoken='', verbosity=0, run_ocl_offline=False):
        datimbase.DatimBase.__init__(self)
        self.verbosity = verbosity
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.run_ocl_offline = run_ocl_offline

    def import_imap(self, imap_input=None):
        # STEP 1 of 11: Validate that PEPFAR metadata for specified period defined in OCL
        #/PEPFAR/DATIM-MOH course/fine metadata and released period (e.g. FY17) available
        self.vlog(1, '**** STEP 1 of 11: Validate that PEPFAR metadata for specified period defined in OCL')
        if datimimap.DatimImapFactory.is_valid_imap_period(imap_input.period):
            self.vlog(1, 'PEPFAR metadata for period "%s" defined in OCL environement "%s"' % (imap_input.period, self.oclenv))
        else:
            print('uh oh')
            sys.exit(1)

        # STEP 2 of 11: Validate input country mapping CSV file
        # verify correct columns exist (order agnostic)
        self.vlog(1, '**** STEP 2 of 11: Validate input country mapping CSV file')
        if imap_input.is_valid():
            self.vlog(1, 'Provided IMAP is valid')
        else:
            self.vlog(1, 'Provided IMAP is not valid')
            sys.exit(1)

        # STEP 3 of 11: Preprocess input country mapping CSV
        # Determine if this is needed (csv_fixer.py)
        self.vlog(1, '**** STEP 3 of 11: Preprocess input country mapping CSV')

        # STEP 4 of 11: Fetch existing IMAP export from OCL for the specified country+period
        # Refer to imapexport.py
        self.vlog(1, '**** STEP 4 of 11: Fetch existing IMAP export from OCL for the specified country and period')
        imap_old = datimimap.DatimImapFactory.load_imap_from_ocl(
            oclenv=self.oclenv, oclapitoken=self.oclapitoken, run_ocl_offline=self.run_ocl_offline,
            country_org=imap_input.country_org, period=imap_input.period)

        # STEP 5 of 11: Evaluate delta between input and OCL IMAPs
        self.vlog(1, '**** STEP 5 of 11: Evaluate delta between input and OCL IMAPs')
        imap_diff = imap_old.diff(imap_input)
        # TODO: Post-processing of diff results
        pprint.pprint(imap_diff.get_diff())
        # What I really want to extract from the diff file -- which CSV rows were added, updated and deleted
        # What I really want to extract from the answers above -- changes to the source,
        # collection versions that have been deleted, 



        # STEP 6 of 11: Generate import script
        # Generate from the delta or just go with the raw CSV if no prior version exists
        # country org, source, collections, concepts, and mappings...and remember the dedup
        self.vlog(1, '**** STEP 6 of 11: Generate import script')
        import_script = datimimap.DatimImapFactory.generate_import_script_from_diff(imap_diff)

        # STEP 7 of 11: Import changes into OCL
        # Be sure to get the mapping IDs into the import results object! -- and what about import error handling?
        self.vlog(1, '**** STEP 7 of 11: Import changes into OCL')

        # STEP 8 of 11: Create released source version for the country
        self.vlog(1, '**** STEP 8 of 11: Create released source version for the country')

        # STEP 9 of 11: Generate collection references
        # use refgen.py
        self.vlog(1, '**** STEP 9 of 11: Generate collection references')

        # STEP 10 of 11: Import the collection references
        self.vlog(1, '**** STEP 10 of 11: Import the collection references')

        # STEP 11 of 11: Create released versions for each of the collections
        # Refer to new_versions.py
        self.vlog(1, '**** STEP 11 of 11: Create released versions for each of the collections')

    def get_country_org_dict(self, country_org='', country_code='', country_name=''):
        return {
            'type': 'Organization',
            'id': country_org,
            'name': 'DATIM MOH %s' % country_name,
            'location': country_name,
        }

    def get_country_source_dict(self, country_org='', country_code='', country_name=''):
        source_name = 'DATIM MOH %s Alignment Indicators' % (country_name)
        source = {
            'type': 'Source',
            'id': 'DATIM-Alignment-Indicators',
            'short_code': 'DATIM-Alignment-Indicators',
            'owner': country_org,
            'owner_type': 'Organization',
            'name': source_name,
            'full_name': source_name,
            'source_type': 'Dictionary',
            'default_locale': 'en',
            'supported_locales': 'en',
        }
        return source


class DatimMohCsvToJsonConverter(ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter):
    ''' Extend to add a custom CSV pre-processor '''

    def get_owner_type_url_part(self, owner_type):
        if owner_type == 'Organization':
            return 'orgs'
        elif owner_type == 'User':
            return 'users'
        return ''

    def preprocess_csv_row(self, row, attr=None):
        ''' Create all of the additional columns '''
        if row['MOH_Indicator_ID'] and row['MOH_Disag_ID']:
            row['DATIM Owner Type'] = attr['datim_owner_type']
            row['DATIM Owner ID'] = attr['datim_owner']
            row['DATIM Source ID'] = attr['datim_source']
            datim_owner_type_url_part = self.get_owner_type_url_part(row['DATIM Owner Type'])
            row['Country Data Element Owner Type'] = attr['country_owner_type']
            row['Country Data Element Owner ID'] = attr['country_owner']
            row['Country Data Element Source ID'] = attr['country_source']
            country_data_element_owner_type_url_part = self.get_owner_type_url_part(row['Country Data Element Owner Type'])
            if row['MOH_Disag_ID'] == 'null_disag':
                row['Country Disaggregate Owner Type'] = attr['null_disag_owner_type']
                row['Country Disaggregate Owner ID'] = attr['null_disag_owner']
                row['Country Disaggregate Source ID'] = attr['null_disag_source']
            else:
                row['Country Disaggregate Owner Type'] = attr['country_owner_type']
                row['Country Disaggregate Owner ID'] = attr['country_owner']
                row['Country Disaggregate Source ID'] = attr['country_source']
            country_disaggregate_owner_type_url_part = self.get_owner_type_url_part(row['Country Disaggregate Owner Type'])
            row['DATIM_Disag_Name_Clean'] = '_'.join(row['DATIM_Disag_Name'].replace('>', ' gt ').replace('<', ' lt ').replace('|',' ').replace('+', ' plus ').split())
            row['Country Collection Name'] = row['DATIM_Indicator_ID'] + ': ' + row['DATIM_Disag_Name']
            row['Country Collection ID'] = (row['DATIM_Indicator_ID'] + '_' + row['DATIM_Disag_Name_Clean']).replace('_', '-')
            row['DATIM From Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (datim_owner_type_url_part, row['DATIM Owner ID'], row['DATIM Source ID'], row['DATIM_Indicator_ID'])
            row['DATIM To Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (datim_owner_type_url_part, row['DATIM Owner ID'], row['DATIM Source ID'], row['DATIM_Disag_ID'])
            row['Country Map Type'] = row['Operation'] + ' OPERATION'
            # Data Element
            row['Country From Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (country_data_element_owner_type_url_part, row['Country Data Element Owner ID'], row['Country Data Element Source ID'], row['MOH_Indicator_ID'])
            # Disaggregate
            row['Country To Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (country_disaggregate_owner_type_url_part, row['Country Disaggregate Owner ID'], row['Country Disaggregate Source ID'], row['MOH_Disag_ID'])
        else:
            row['DATIM_Disag_Name_Clean'] = ''
            row['Country Collection Name'] = ''
            row['Country Collection ID'] = ''
            row['DATIM From Concept URI'] = ''
            row['DATIM To Concept URI'] = ''
            row['Country Map Type'] = ''
            row['Country From Concept URI'] = ''
            row['Country To Concept URI'] = ''
        return row

    @staticmethod
    def get_country_csv_resource_definitions(attr):
        csv_resource_definitions = [
            {
                'definition_name':'MOH-Indicator',
                'is_active': True,
                'resource_type':'Concept',
                'id_column':'MOH_Indicator_ID',
                'skip_if_empty_column':'MOH_Indicator_ID',
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'concept_class', 'value':'Indicator'},
                    {'resource_field':'datatype', 'value':'Numeric'},
                    {'resource_field':'owner', 'column':'Country Data Element Owner ID'},
                    {'resource_field':'owner_type', 'column':'Country Data Element Owner Type'},
                    {'resource_field':'source', 'column':'Country Data Element Source ID'},
                ],
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_SUB_RESOURCES:{
                    'names':[
                        [
                            {'resource_field':'name', 'column':'MOH_Indicator_Name'},
                            {'resource_field':'locale', 'value':'en'},
                            {'resource_field':'locale_preferred', 'value':'True'},
                            {'resource_field':'name_type', 'value':'Fully Specified'},
                        ],
                    ],
                    'descriptions':[]
                },
            },
            {
                'definition_name':'MOH-Disaggregate',
                'is_active': True,
                'resource_type':'Concept',
                'id_column':'MOH_Disag_ID',
                'skip_if_empty_column':'MOH_Disag_ID',
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'concept_class', 'value':'Disaggregate'},
                    {'resource_field':'datatype', 'value':'None'},
                    {'resource_field':'owner', 'column':'Country Disaggregate Owner ID'},
                    {'resource_field':'owner_type', 'column':'Country Disaggregate Owner Type'},
                    {'resource_field':'source', 'column':'Country Disaggregate Source ID'},
                ],
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_SUB_RESOURCES:{
                    'names':[
                        [
                            {'resource_field':'name', 'column':'MOH_Disag_Name'},
                            {'resource_field':'locale', 'value':'en'},
                            {'resource_field':'locale_preferred', 'value':'True'},
                            {'resource_field':'name_type', 'value':'Fully Specified'},
                        ],
                    ],
                    'descriptions':[]
                },
            },
            {
                'definition_name':'Mapping-Datim-Has-Option',
                'is_active': True,
                'resource_type':'Mapping',
                'id_column':None,
                'skip_if_empty_column':'MOH_Disag_ID',
                'internal_external': {'value':ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.INTERNAL_MAPPING_ID},
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'from_concept_url', 'column':'DATIM From Concept URI'},
                    {'resource_field':'map_type', 'value':attr['datim_map_type']},
                    {'resource_field':'to_concept_url', 'column':'DATIM To Concept URI'},
                    {'resource_field':'owner', 'value':attr['country_owner']},
                    {'resource_field':'owner_type', 'value':attr['country_owner_type']},
                    {'resource_field':'source', 'value':attr['country_source']},
                ]
            },
            {
                'definition_name':'Mapping-Operation',
                'is_active': True,
                'resource_type':'Mapping',
                'id_column':None,
                'skip_if_empty_column':'Operation',
                'internal_external': {'value':ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.INTERNAL_MAPPING_ID},
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'from_concept_url', 'column':'Country From Concept URI'},
                    {'resource_field':'map_type', 'column':'Country Map Type'},
                    {'resource_field':'to_concept_url', 'column':'Country To Concept URI'},
                    {'resource_field':'owner', 'value':attr['country_owner']},
                    {'resource_field':'owner_type', 'value':attr['country_owner_type']},
                    {'resource_field':'source', 'value':attr['country_source']},
                ]
            },
            {
                'definition_name':'Collection',
                'is_active': True,
                'resource_type':'Collection',
                'id_column':'Country Collection ID',
                'skip_if_empty_column':'Country Collection ID',
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'full_name', 'column':'Country Collection Name'},
                    {'resource_field':'name', 'column':'Country Collection Name'},
                    {'resource_field':'short_code', 'column':'Country Collection ID'},
                    {'resource_field':'collection_type', 'value':'Subset'},
                    {'resource_field':'supported_locales', 'value':'en'},
                    {'resource_field':'public_access', 'value':'View'},
                    {'resource_field':'default_locale', 'value':'en'},
                    {'resource_field':'description', 'value':''},
                    {'resource_field':'owner', 'value':attr['country_owner']},
                    {'resource_field':'owner_type', 'value':attr['country_owner_type']},
                ]
            }
        ]
        return csv_resource_definitions

