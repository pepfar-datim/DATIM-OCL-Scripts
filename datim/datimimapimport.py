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
import settings
from pprint import pprint
from datimbase import DatimBase
from datimimap import DatimImap, DatimImapFactory
from datimimapexport import DatimImapExport


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


    def import_imap(self, imap_input=None):
        # STEP 1 of 11: Validate that PEPFAR metadata for specified period defined in OCL
        #/PEPFAR/DATIM-MOH course/fine metadata and released period (e.g. FY17) available
        self.vlog(1, '**** STEP 1 of 11: Validate that PEPFAR metadata for specified period defined in OCL')
        if DatimImapFactory.is_valid_imap_period(imap_input.period):
            self.vlog(1, 'PEPFAR metadata for period "%s" defined in OCL environement "%s"' % (imap_input.period, self.oclenv))
        else:
            print('uh oh')
            sys.exit(1)

        # STEP 2 of 11: Validate input country mapping CSV file
        # verify correct columns exist (order agnostic)
        self.vlog(1, '**** STEP 2 of 11: Validate that PEPFAR metadata for specified period defined in OCL')
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
        imap_old = DatimImapFactory.load_imap_from_ocl(country_org=imap_input.country_org, period=imap_input.period)

        # STEP 5 of 11: Evaluate delta between input and OCL IMAPs
        self.vlog(1, '**** STEP 5 of 11: Evaluate delta between input and OCL IMAPs')
        imap_diff = imap_old.diff(imap_input)

        # STEP 6 of 11: Generate import script from the delta
        # country org, source, collections, concepts, and mappings...and remember the dedup
        self.vlog(1, '**** STEP 6 of 11: Generate import script from the delta')
        import_script = DatimImapFactory.generate_import_script_from_diff(imap_diff)

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
