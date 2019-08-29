import requests
import settings
import datim.datimimap
import datim.datimimaptests
import datim.datimimapimport


# Shared settings -- these are used for all tests
period = 'FY19'
ocl_api_env = settings.oclenv
ocl_api_token = settings.oclapitoken
ocl_api_root_token = settings.ocl_root_api_token
# ocl_api_env = settings.ocl_api_url_staging
# ocl_api_token = settings.api_token_staging_datim_admin
# ocl_api_root_token = settings.api_token_staging_root
# ocl_api_env = settings.ocl_api_url_production
# ocl_api_token = settings.api_token_production_datim_admin
# ocl_api_root_token = settings.api_token_production_root

# 2-country import and compare settings
imap_a_filename = 'csv/UA-FY19-v0.csv'
imap_b_filename = 'csv/UA-FY19-v1.csv'
imap_demo_filename = 'csv/DEMO-FY19.csv'
country_a_code = 'Test-CA'
country_a_name = 'Test Country A'
country_a_org = 'DATIM-MOH-%s-%s' % (country_a_code, period)
country_b_code = 'Test-CB'
country_b_name = 'Test Country B'
country_b_org = 'DATIM-MOH-%s-%s' % (country_b_code, period)

# 1-country import test
imap_test_import_filename = 'csv/MW-FY19-v0.csv'
country_test_code = 'MW-TEST'
country_test_name = country_test_code
country_test_org = 'DATIM-MOH-%s-%s' % (country_test_code, period)
imap_test_import_2_filename = 'csv/MW-FY19-v1.csv'


imap_tests_ua_compare = [
    {
        "test_id": "drop:CA-initial",
        "is_active": False,
        "test_description": "Clear out country org A in case it already exists",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_DROP_ORG,
        "country_org": country_a_org,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_root_token,
        "assert_result_type": bool,
        "assert_result_value": True,
    },
    {
        "test_id": "drop:CB-initial",
        "is_active": False,
        "test_description": "Clear out country org B in case it already exists",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_DROP_ORG,
        "country_org": country_b_org,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_root_token,
        "assert_result_type": bool,
        "assert_result_value": True,
    },
    {
        "test_id": "export:CA-latest-before-v0",
        "is_active": False,
        "test_description": "Verify that country org A does not exist",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "export_format": datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": country_a_code,
        "country_name": country_a_name,
        "period": period,
        "assert_result_type": requests.exceptions.HTTPError,
        "assert_http_response_code": 404,
    },
    {
        "test_id": "export:CB-latest-before-v0",
        "is_active": False,
        "test_description": "Verify that country org B does not exist",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "export_format": datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": country_b_code,
        "country_name": country_b_name,
        "period": period,
        "assert_result_type": requests.exceptions.HTTPError,
        "assert_http_response_code": 404,
    },
    {
        "test_id": "import:CA-v0",
        "is_active": False,
        "test_description": "Import IMAP-A into Country A",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_IMPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "country_code": country_a_code,
        "country_name": country_a_name,
        "period": period,
        "imap_import_filename": imap_a_filename,
        "assert_result_type": int,
        "assert_result_value": datim.datimimapimport.DatimImapImport.DATIM_IMAP_RESULT_SUCCESS,
    },
    {
        "test_id": "export:CA-latest-after-v0",
        "is_active": False,
        "test_description": "Export CA latest after v0 import",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "export_format": datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": country_a_code,
        "country_name": country_a_name,
        "period": period,
        "assert_result_type": datim.datimimap.DatimImap,
    },
    {
        "test_id": "compare:CA-latest-after-v0--imap_a.csv",
        "is_active": True,
        "test_description": "Confirm that CA latest matches IMAP-A",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_RESULT,
        "imap_a_result_id": "export:CA-latest-after-v0",
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": imap_a_filename,
        "imap_b_period": period,
        "imap_b_country_org": country_a_org,
        "imap_b_country_name": country_a_name,
        "imap_b_country_code": country_a_code,
        "assert_result_type": datim.datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
    {
        "test_id": "drop:CA-end",
        "is_active": False,
        "test_description": "Delete country org A to clean up environment",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_DROP_ORG,
        "country_org": country_a_org,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_root_token,
        "assert_result_type": bool,
        "assert_result_value": True,
    },
    {
        "test_id": "drop:CB-end",
        "is_active": False,
        "test_description": "Delete country org B to clean up environment",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_DROP_ORG,
        "country_org": country_b_org,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_root_token,
        "assert_result_type": bool,
        "assert_result_value": True,
    },
    {
        "test_id": "CA-latest-after-drop",
        "is_active": False,
        "test_description": "Verify Country A deleted after drop",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "export_format": datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": country_a_code,
        "country_name": country_a_name,
        "period": period,
        "assert_result_type": requests.exceptions.HTTPError,
        "assert_http_response_code": 404,
    },
    {
        "test_id": "CB-latest-after-drop",
        "is_active": False,
        "test_description": "Verify Country B deleted after drop",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "export_format": datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": country_b_code,
        "country_name": country_b_name,
        "period": period,
        "assert_result_type": requests.exceptions.HTTPError,
        "assert_http_response_code": 404,
    },
]

# Test to compare 2 IMAPs
imap_tests_demo_compare = [
    {
        "test_id": "demo-export-from-ocl",
        "is_active": True,
        "test_description": "Fetch DEMO export from OCL",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "export_format": datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_org": 'DATIM-MOH-DEMO-FY19',
        "country_name": 'DEMO-FY19',
        "country_code": 'DEMO',
        "period": period,
    },
    {
        "test_id": "compare-01",
        "is_active": True,
        "test_description": "Compare: imap_a.csv --> imap_b.csv",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_a_filename": imap_a_filename,
        "imap_a_period": period,
        "imap_a_country_org": country_a_org,
        "imap_a_country_name": country_a_name,
        "imap_a_country_code": country_a_code,
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": imap_b_filename,
        "imap_b_period": period,
        "imap_b_country_org": country_b_org,
        "imap_b_country_name": country_b_name,
        "imap_b_country_code": country_b_code,
        "assert_result_type": datim.datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
    {
        "test_id": "compare-02",
        "is_active": True,
        "test_description": "Compare: imap_a.csv --> ocl:demo",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_a_filename": imap_a_filename,
        "imap_a_period": period,
        "imap_a_country_org": country_a_org,
        "imap_a_country_name": country_a_name,
        "imap_a_country_code": country_a_code,
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_OCL,
        "imap_b_ocl_api_env": ocl_api_env,
        "imap_b_ocl_api_token": ocl_api_token,
        "imap_b_period": period,
        "imap_b_country_org": 'DATIM-MOH-DEMO-FY19',
        "imap_b_country_name": 'DEMO-FY19',
        "imap_b_country_code": 'DEMO',
        "assert_result_type": datim.datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
    {
        "test_id": "compare-03",
        "is_active": True,
        "test_description": "Compare: imap_a.csv --> result:demo-export-from-ocl",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_a_filename": imap_a_filename,
        "imap_a_period": period,
        "imap_a_country_org": country_a_org,
        "imap_a_country_name": country_a_name,
        "imap_a_country_code": country_a_code,
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_RESULT,
        "imap_b_result_id": "demo-export-from-ocl",
        "assert_result_type": datim.datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
    {
        "test_id": "compare-04",
        "is_active": True,
        "test_description": "Compare: demo.csv --> result:demo-export-from-ocl",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_a_filename": imap_demo_filename,
        "imap_a_period": period,
        "imap_a_country_org": 'DATIM-MOH-DEMO-FY19',
        "imap_a_country_name": 'DEMO',
        "imap_a_country_code": 'DEMO',
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_RESULT,
        "imap_b_result_id": "demo-export-from-ocl",
        "assert_result_type": datim.datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
    {
        "test_id": "compare-05",
        "is_active": True,
        "test_description": "Compare: demo.csv --> demo.csv",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_a_filename": imap_demo_filename,
        "imap_a_period": period,
        "imap_a_country_org": 'DATIM-MOH-DEMO-FY19',
        "imap_a_country_name": 'DEMO',
        "imap_a_country_code": 'DEMO',
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": imap_demo_filename,
        "imap_b_period": period,
        "imap_b_country_org": 'DATIM-MOH-DEMO-FY19',
        "imap_b_country_name": 'DEMO',
        "imap_b_country_code": 'DEMO',
        "assert_result_type": datim.datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
]


# Script to test 1 import into a single country
imap_test_one_import = [
    {
        "test_id": "drop:%s-initial" % country_test_code,
        "is_active": True,
        "test_description": "Clear out country org '%s' in case it already exists" % country_test_org,
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_DROP_ORG,
        "country_org": country_test_org,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_root_token,
        "assert_result_type": bool,
        "assert_result_value": False,
    },
    {
        "test_id": "export:%s-initial" % country_test_code,
        "is_active": True,
        "test_description": "Verify that country org '%s' does not exist" % country_test_org,
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "export_format": datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": country_test_code,
        "country_name": country_test_name,
        "period": period,
        "assert_result_type": requests.exceptions.HTTPError,
        "assert_http_response_code": 404,
    },
    {
        "test_id": "import:%s-v0" % country_test_code,
        "is_active": True,
        "test_description": "Initial v0 import of '%s' into '%s'" % (imap_test_import_filename, country_test_org),
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_IMPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "country_code": country_test_code,
        "country_name": country_test_name,
        "period": period,
        "imap_import_filename": imap_test_import_filename,
        "assert_result_type": int,
        "assert_result_value": datim.datimimapimport.DatimImapImport.DATIM_IMAP_RESULT_SUCCESS,
    },
    {
        "test_id": "export:%s-after-v0" % country_test_code,
        "is_active": True,
        "test_description": "Export '%s' latest after v0 import" % country_test_code,
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "export_format": datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": country_test_code,
        "country_name": country_test_name,
        "period": period,
        "assert_result_type": datim.datimimap.DatimImap,
    },
    {
        "test_id": "compare:%s-latest-after-v0 || %s" % (country_test_code, imap_test_import_filename),
        "is_active": True,
        "test_description": "Confirm that imported '%s' latest matches '%s'" % (
            country_test_code, imap_test_import_filename),
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_RESULT,
        "imap_a_result_id": "export:%s-after-v0" % country_test_code,
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": imap_test_import_filename,
        "imap_b_period": period,
        "imap_b_country_org": country_test_org,
        "imap_b_country_name": country_test_name,
        "imap_b_country_code":  country_test_code,
        "assert_result_type": datim.datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
    {
        "test_id": "drop:%s-end" % country_test_code,
        "is_active": True,
        "test_description": "Delete country org '%s' to clean up environment" % country_test_org,
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_DROP_ORG,
        "country_org": country_test_org,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_root_token,
        "assert_result_type": bool,
        "assert_result_value": True,
    },
]


# Script to test 2 imports to a single country
imap_test_two_imports = [
    {
        "test_id": "drop:%s-initial" % country_test_code,
        "is_active": True,
        "test_description": "Clear out country org '%s' in case it already exists" % country_test_org,
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_DROP_ORG,
        "country_org": country_test_org,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_root_token,
        "assert_result_type": bool,
        "assert_result_value": False,
    },
    {
        "test_id": "export:%s-initial" % country_test_code,
        "is_active": True,
        "test_description": "Verify that country org '%s' does not exist" % country_test_org,
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "export_format": datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": country_test_code,
        "country_name": country_test_name,
        "period": period,
        "assert_result_type": requests.exceptions.HTTPError,
        "assert_http_response_code": 404,
    },
    {
        "test_id": "import:%s-v0" % country_test_code,
        "is_active": True,
        "test_description": "Initial v0 import of '%s' into '%s'" % (imap_test_import_filename, country_test_org),
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_IMPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "country_code": country_test_code,
        "country_name": country_test_name,
        "period": period,
        "imap_import_filename": imap_test_import_filename,
        "assert_result_type": int,
        "assert_result_value": datim.datimimapimport.DatimImapImport.DATIM_IMAP_RESULT_SUCCESS,
    },
    {
        "test_id": "export:%s-latest-after-v0" % country_test_code,
        "is_active": True,
        "test_description": "Export '%s' latest after v0 import" % country_test_code,
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "export_format": datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": country_test_code,
        "country_name": country_test_name,
        "period": period,
        "assert_result_type": datim.datimimap.DatimImap,
    },
    {
        "test_id": "import:%s-v1" % country_test_code,
        "is_active": True,
        "test_description": "v1 import of '%s' into '%s'" % (imap_test_import_2_filename, country_test_org),
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_IMPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "country_code": country_test_code,
        "country_name": country_test_name,
        "period": period,
        "imap_import_filename": imap_test_import_2_filename,
        "assert_result_type": int,
        "assert_result_value": datim.datimimapimport.DatimImapImport.DATIM_IMAP_RESULT_SUCCESS,
    },
    {
        "test_id": "export:%s-latest-after-v1" % country_test_code,
        "is_active": True,
        "test_description": "Export '%s' latest after v1 import" % country_test_code,
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "export_format": datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_CSV,
        "country_code": country_test_code,
        "country_name": country_test_name,
        "period": period,
        "assert_result_type": datim.datimimap.DatimImap,
    },
    {
        "test_id": "compare:%s-latest-after-v0 || %s" % (country_test_code, imap_test_import_filename),
        "is_active": True,
        "test_description": "Confirm that imported '%s' v0 latest matches '%s'" % (
            country_test_code, imap_test_import_filename),
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_RESULT,
        "imap_a_result_id": "export:%s-latest-after-v0" % country_test_code,
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": imap_test_import_filename,
        "imap_b_period": period,
        "imap_b_country_org": country_test_org,
        "imap_b_country_name": country_test_name,
        "imap_b_country_code":  country_test_code,
        "assert_result_type": datim.datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
    {
        "test_id": "compare:%s-latest-after-v1 || %s" % (country_test_code, imap_test_import_2_filename),
        "is_active": True,
        "test_description": "Confirm that imported '%s' v1 latest matches '%s'" % (
            country_test_code, imap_test_import_2_filename),
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_RESULT,
        "imap_a_result_id": "export:%s-latest-after-v1" % country_test_code,
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": imap_test_import_2_filename,
        "imap_b_period": period,
        "imap_b_country_org": country_test_org,
        "imap_b_country_name": country_test_name,
        "imap_b_country_code": country_test_code,
        "assert_result_type": datim.datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
    {
        "test_id": "drop:%s-end" % country_test_code,
        "is_active": True,
        "test_description": "Delete country org '%s' to clean up environment" % country_test_org,
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_DROP_ORG,
        "country_org": country_test_org,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_root_token,
        "assert_result_type": bool,
        "assert_result_value": True,
    },
]


imap_tests = imap_test_two_imports

# Debug output
print '*' * 100
print 'SCRIPT SETTINGS:'
print '  ocl_api_env = %s' % ocl_api_env
print '*' * 100
datim.datimimaptests.DatimImapTests.display_test_summary(imap_tests)

# Run the tests and display the results
imap_tester = datim.datimimaptests.DatimImapTests()
imap_tester.run_tests(imap_tests)
imap_tester.display_test_results()

