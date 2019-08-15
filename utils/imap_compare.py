"""
Script to quickly compare 2 IMAP CSV files
"""
import datim.datimimap
import settings
import pprint

IMAP_TYPE_LOAD_FROM_FILE = 'file'
IMAP_TYPE_LOAD_FROM_OCL = 'ocl'

# Shared settings
export_format = datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV
exclude_empty_maps = True
include_extra_info = False

# IMAP A
imap_a_type = IMAP_TYPE_LOAD_FROM_FILE
imap_a_country_code = 'CM'  # 'BW'
imap_a_country_name = 'Cameroon'  # 'Botswana'
imap_a_period = 'FY19'
imap_a_filename = 'csv/CM_indicatorMapping_FY19.csv'  # 'csv/BW_indicatorMapping_FY19.csv'
imap_a_ocl_env = ''  # settings.ocl_api_url_staging
imap_a_ocl_api_token = ''  # settings.api_token_staging_datim_admin
imap_a_country_org = 'DATIM-MOH-%s-%s' % (imap_a_country_code, imap_a_period)

# IMAP B
imap_b_type = IMAP_TYPE_LOAD_FROM_OCL
imap_b_country_code = 'CM'
imap_b_country_name = 'Cameroon'
imap_b_period = 'FY19'
imap_b_filename = ''  # 'csv/TZ-FY18-20190610-export3.csv'
imap_b_ocl_env = settings.oclenv
imap_b_ocl_api_token = settings.oclapitoken
imap_b_country_org = 'DATIM-MOH-%s-%s' % (imap_b_country_code, imap_b_period)

# Load IMAP A
print '**** STEP 1 OF 3: Loading IMAP A...'
if imap_a_type == IMAP_TYPE_LOAD_FROM_FILE:
    print 'Loading IMAP A from file: %s' % imap_a_filename
    imap_a = datim.datimimap.DatimImapFactory.load_imap_from_csv(
        csv_filename=imap_a_filename, period=imap_a_period, country_org=imap_a_country_org,
        country_name=imap_a_country_name, country_code=imap_a_country_code)
elif imap_a_type == IMAP_TYPE_LOAD_FROM_OCL:
    print 'Loading IMAP A from OCL: %s' % imap_a_country_org
    imap_a = datim.datimimap.DatimImapFactory.load_imap_from_ocl(
        oclenv=imap_a_ocl_env, oclapitoken=imap_a_ocl_api_token, run_ocl_offline=False,
        country_code=imap_a_country_code, country_org=imap_a_country_org, period=imap_a_period, verbosity=2)
else:
    raise('ERROR: Unrecognized imap_a_type "%s".' % imap_a_type)
imap_a.display(fmt=export_format, sort=True, exclude_empty_maps=exclude_empty_maps,
               include_extra_info=include_extra_info)
imap_a_is_valid = imap_a.is_valid(throw_exception_on_error=False)
if imap_a_is_valid != True:
    print imap_a_is_valid

# Load IMAP B
print '**** STEP 2 OF 3: Loading IMAP B...'
if imap_b_type == IMAP_TYPE_LOAD_FROM_FILE:
    print 'Loading IMAP B from file: %s' % imap_b_filename
    imap_b = datim.datimimap.DatimImapFactory.load_imap_from_csv(
        csv_filename=imap_b_filename, period=imap_b_period, country_org=imap_b_country_org,
        country_name=imap_b_country_name, country_code=imap_b_country_code)
elif imap_b_type == IMAP_TYPE_LOAD_FROM_OCL:
    print 'Loading IMAP B from OCL: %s' % imap_b_country_org
    imap_b = datim.datimimap.DatimImapFactory.load_imap_from_ocl(
        oclenv=imap_b_ocl_env, oclapitoken=imap_b_ocl_api_token, run_ocl_offline=False,
        country_code=imap_b_country_code, country_org=imap_b_country_org, period=imap_b_period, verbosity=2)
else:
    raise('ERROR: Unrecognized imap_b_type "%s".' % imap_b_type)
imap_b.display(fmt=export_format, sort=True, exclude_empty_maps=exclude_empty_maps,
               include_extra_info=include_extra_info)
imap_b_is_valid = imap_b.is_valid(throw_exception_on_error=False)
if imap_b_is_valid != True:
    print imap_b_is_valid

# Compare
print '**** STEP 3 OF 3: Compare IMAP A and B...'
imap_diff = imap_a.diff(imap_b)
pprint.pprint(imap_diff.get_diff())
