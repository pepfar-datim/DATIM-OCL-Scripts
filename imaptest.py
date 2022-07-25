""" Script to run a set of tests on IMAP resources """
import requests

import settings
from datim import datimimap, datimimaptests, datimimapimport

# Settings
PERIOD = 'FY20'
OCL_API_ENV = settings.ocl_api_url_staging
OCL_API_TOKEN = settings.api_token_staging_datim_admin
IMAP_TEST_V0_IMPORT_FILENAME = 'csv/MW-FY19-v0.csv'
COUNTRY_TEST_CODE = 'BENCHMARK'
COUNTRY_TEST_NAME = COUNTRY_TEST_CODE
COUNTRY_TEST_ORG = f'DATIM-MOH-{COUNTRY_TEST_CODE}-{PERIOD}'

OCL_API_ROOT_TOKEN = settings.api_token_staging_root
IMAP_TEST_V1_IMPORT_FILENAME = 'csv/MW-FY19-v1.csv'
DO_DROP_WHEN_COMPLETE = True


# Test batch definition: COMPARE FILE WITH EXPORT
test_imap_diff = [
    {
        "test_id": f"export: imap-{COUNTRY_TEST_CODE}",
        "is_active": True,
        "test_description": f"Export '{COUNTRY_TEST_CODE}' from {OCL_API_ENV}",
        "test_type": datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": OCL_API_ENV,
        "ocl_api_token": OCL_API_TOKEN,
        "export_format": datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": COUNTRY_TEST_CODE,
        "country_name": COUNTRY_TEST_NAME,
        "period": PERIOD,
        "assert_result_type": datimimap.DatimImap,
    },
    {
        "test_id": f"compare: imap-{COUNTRY_TEST_CODE} || imap-{IMAP_TEST_V0_IMPORT_FILENAME}",
        "is_active": True,
        "test_description": (f"Confirm that imported '{COUNTRY_TEST_CODE}' "
                             f"matches '{IMAP_TEST_V0_IMPORT_FILENAME}'"),
        "test_type": datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_RESULT,
        "imap_a_result_id": f"export: imap-{COUNTRY_TEST_CODE}",
        "imap_b_type": datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": IMAP_TEST_V0_IMPORT_FILENAME,
        "imap_b_period": PERIOD,
        "imap_b_country_org": COUNTRY_TEST_ORG,
        "imap_b_country_name": COUNTRY_TEST_NAME,
        "imap_b_country_code":  COUNTRY_TEST_CODE,
        "assert_result_type": datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
]

# Test batch definition
imap_test_batch = [
    {
        "test_id": f"drop:{COUNTRY_TEST_CODE}-initial",
        "is_active": True,
        "test_description": f"Clear out country org '{COUNTRY_TEST_ORG}' in case it already exists",
        "test_type": datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_DROP_ORG,
        "country_org": COUNTRY_TEST_ORG,
        "ocl_api_env": OCL_API_ENV,
        "ocl_api_token": OCL_API_ROOT_TOKEN,
        "assert_result_type": bool,
        "assert_result_value": False,
    },
    {
        "test_id": f"export:{COUNTRY_TEST_CODE}-initial",
        "is_active": True,
        "test_description": f"Verify that country org '{COUNTRY_TEST_ORG}' does not exist",
        "test_type": datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": OCL_API_ENV,
        "ocl_api_token": OCL_API_TOKEN,
        "export_format": datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": COUNTRY_TEST_CODE,
        "country_name": COUNTRY_TEST_NAME,
        "period": PERIOD,
        "assert_result_type": requests.exceptions.HTTPError,
        "assert_http_response_code": 404,
    },
    {
        "test_id": f"import:{COUNTRY_TEST_CODE}-v0",
        "is_active": True,
        "test_description": (f"Initial v0 import of '{IMAP_TEST_V0_IMPORT_FILENAME}' "
                             f"into '{COUNTRY_TEST_ORG}'"),
        "test_type": datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_IMPORT,
        "ocl_api_env": OCL_API_ENV,
        "ocl_api_token": OCL_API_TOKEN,
        "country_code": COUNTRY_TEST_CODE,
        "country_name": COUNTRY_TEST_NAME,
        "period": PERIOD,
        "imap_import_filename": IMAP_TEST_V0_IMPORT_FILENAME,
        "assert_result_type": int,
        "assert_result_value": datimimapimport.DatimImapImport.DATIM_IMAP_RESULT_SUCCESS,
    },
    {
        "test_id": f"export:{COUNTRY_TEST_CODE}-latest-after-v0",
        "is_active": True,
        "test_description": f"Export '{COUNTRY_TEST_CODE}' latest after v0 import",
        "test_type": datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": OCL_API_ENV,
        "ocl_api_token": OCL_API_TOKEN,
        "export_format": datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": COUNTRY_TEST_CODE,
        "country_name": COUNTRY_TEST_NAME,
        "period": PERIOD,
        "assert_result_type": datimimap.DatimImap,
    },
    {
        "test_id": f"import:{COUNTRY_TEST_CODE}-v1",
        "is_active": True,
        "test_description": (f"v1 import of '{IMAP_TEST_V1_IMPORT_FILENAME}' "
                             f"into '{COUNTRY_TEST_ORG}'"),
        "test_type": datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_IMPORT,
        "ocl_api_env": OCL_API_ENV,
        "ocl_api_token": OCL_API_TOKEN,
        "country_code": COUNTRY_TEST_CODE,
        "country_name": COUNTRY_TEST_NAME,
        "period": PERIOD,
        "imap_import_filename": IMAP_TEST_V1_IMPORT_FILENAME,
        "assert_result_type": int,
        "assert_result_value": datimimapimport.DatimImapImport.DATIM_IMAP_RESULT_SUCCESS,
    },
    {
        "test_id": f"export:{COUNTRY_TEST_CODE}-latest-after-v1",
        "is_active": True,
        "test_description": f"Export '{COUNTRY_TEST_CODE}' latest after v1 import",
        "test_type": datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": OCL_API_ENV,
        "ocl_api_token": OCL_API_TOKEN,
        "export_format": datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": COUNTRY_TEST_CODE,
        "country_name": COUNTRY_TEST_NAME,
        "period": PERIOD,
        "assert_result_type": datimimap.DatimImap,
    },
    {
        "test_id": f"compare:{COUNTRY_TEST_CODE}-latest-after-v0 || {IMAP_TEST_V0_IMPORT_FILENAME}",
        "is_active": True,
        "test_description": (f"Confirm that imported '{COUNTRY_TEST_CODE}' v0 latest "
                             f"matches '{IMAP_TEST_V0_IMPORT_FILENAME}'"),
        "test_type": datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_RESULT,
        "imap_a_result_id": f"export:{COUNTRY_TEST_CODE}-latest-after-v0",
        "imap_b_type": datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": IMAP_TEST_V0_IMPORT_FILENAME,
        "imap_b_period": PERIOD,
        "imap_b_country_org": COUNTRY_TEST_ORG,
        "imap_b_country_name": COUNTRY_TEST_NAME,
        "imap_b_country_code":  COUNTRY_TEST_CODE,
        "assert_result_type": datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
    {
        "test_id": f"compare:{COUNTRY_TEST_CODE}-latest-after-v1 || {IMAP_TEST_V1_IMPORT_FILENAME}",
        "is_active": True,
        "test_description": (f"Confirm that imported '{COUNTRY_TEST_CODE}' v1 latest "
                             f"matches '{IMAP_TEST_V1_IMPORT_FILENAME}'"),
        "test_type": datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_RESULT,
        "imap_a_result_id": f"export:{COUNTRY_TEST_CODE}-latest-after-v1",
        "imap_b_type": datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": IMAP_TEST_V1_IMPORT_FILENAME,
        "imap_b_period": PERIOD,
        "imap_b_country_org": COUNTRY_TEST_ORG,
        "imap_b_country_name": COUNTRY_TEST_NAME,
        "imap_b_country_code": COUNTRY_TEST_CODE,
        "assert_result_type": datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
    {
        "test_id": f"drop:{COUNTRY_TEST_CODE}-end",
        "is_active": bool(DO_DROP_WHEN_COMPLETE),
        "test_description": f"Delete country org '{COUNTRY_TEST_ORG}' to clean up environment",
        "test_type": datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_DROP_ORG,
        "country_org": COUNTRY_TEST_ORG,
        "ocl_api_env": OCL_API_ENV,
        "ocl_api_token": OCL_API_ROOT_TOKEN,
        "assert_result_type": bool,
        "assert_result_value": True,
    },
]

# Run the tests and display the results
datimimaptests.DatimImapTests.display_test_summary(test_imap_diff)
imap_tester = datimimaptests.DatimImapTests()
imap_tester.run_tests(test_imap_diff)
imap_tester.display_test_results()
