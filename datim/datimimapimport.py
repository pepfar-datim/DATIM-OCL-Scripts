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
from pprint import pprint
from datimbase import DatimBase


class DatimImapFactory(Object):
	@staticmethod
	def load_imap_from_csv(filename):
		with open(filename) as input_file:
            imap_csv = json.loads(input_file.read())
            return DatimImap(imap_data=imap_csv)

	@staticmethod
    def load_imap_from_ocl(org_id):
    	pass

	@staticmethod
    def get_csv(datim_imap):
    	pass

    @staticmethod
    def get_ocl_import_script(datim_imap):
    	pass

    @staticmethod
    def compare(datim_imap_1, datim_imap_2):
    	pass

    @staticmethod
    def get_ocl_import_script_from_diff(imap_diff):
    	pass


class DatimImap(Object):
	"""
	Object representing a set of country indicator mappings
	"""

	def __init__(self, country_code='', period='', imap_data=None):
		self.country_code = country_code
		self.period = period
		self.set_imap_data(imap_data)

	def set_imap_data(imap_data):
		pass

	def compare()
		pass

	def get_csv():
		pass

	def get_ocl_collections():
		pass


class DatimImapImport(DatimBase):
    """
    Class to import PEPFAR country mapping metadata from a CSV file into OCL.
    """

    def __init__(self, oclenv='', oclapitoken='', verbosity=0, run_ocl_offline=False):
        DatimBase.__init__(self)
        self.verbosity = verbosity
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.run_ocl_offline = run_ocl_offline

    def generate_imap_import_script(imap_csv):
    	pass
