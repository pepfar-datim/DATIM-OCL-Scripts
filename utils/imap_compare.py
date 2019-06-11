"""
Script to quickly compare 2 IMAP CSV files
"""

import datim.datimimap
import settings
import pprint

# IMAP A & B settings -- which files are we comparing?
imap_a_country_code = 'TZ'
imap_a_country_name = 'Tanzania'
imap_a_period = 'FY18'
imap_a_filename = 'csv/TZ-FY18-20190610.csv'
imap_b_country_code = 'TZ'
imap_b_country_name = 'Tanzania'
imap_b_period = 'FY18'
imap_b_filename = 'csv/TZ-FY18-20190610-export3.csv'

# Shared IMAP export settings


# OCL Settings - JetStream Staging user=datim-admin
oclenv = settings.ocl_api_url_staging
oclapitoken = settings.api_token_staging_datim_admin

imap_a_country_org = 'DATIM-MOH-%s-%s' % (imap_a_country_code, imap_a_period)
imap_b_country_org = 'DATIM-MOH-%s-%s' % (imap_b_country_code, imap_b_period)

# Load the IMAP files
imap_a = datim.datimimap.DatimImapFactory.load_imap_from_csv(
    csv_filename=imap_a_filename, period=imap_a_period, country_org=imap_a_country_org,
    country_name=imap_a_country_name, country_code=imap_a_country_code)
imap_b = datim.datimimap.DatimImapFactory.load_imap_from_csv(
    csv_filename=imap_b_filename, period=imap_b_period, country_org=imap_b_country_org,
    country_name=imap_b_country_name, country_code=imap_b_country_code)

# Compare
imap_diff = imap_a.diff(imap_b)
pprint.pprint(imap_diff.get_diff())
