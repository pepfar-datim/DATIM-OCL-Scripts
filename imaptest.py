""" Script to run a set of tests """
import requests
import settings
import datim.datimimap
import datim.datimimaptests
import datim.datimimapimport

# Settings
period = 'FY20'
ocl_api_env = settings.ocl_api_url_staging
ocl_api_token = settings.api_token_staging_datim_admin
imap_test_v0_import_filename = 'csv/MW-FY19-v0.csv'
country_test_code = 'BENCHMARK'
country_test_name = country_test_code
country_test_org = 'DATIM-MOH-%s-%s' % (country_test_code, period)

ocl_api_root_token = settings.api_token_staging_root
imap_test_v1_import_filename = 'csv/MW-FY19-v1.csv'
do_drop_when_complete = True


# Test batch definition: COMPARE FILE WITH EXPORT
test_imap_diff = [
    {
        "test_id": "export: imap-%s" % country_test_code,
        "is_active": True,
        "test_description": "Export '%s' from %s" % (country_test_code, ocl_api_env),
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
        "test_id": "compare: imap-%s || imap-%s" % (country_test_code, imap_test_v0_import_filename),
        "is_active": True,
        "test_description": "Confirm that imported '%s' matches '%s'" % (
            country_test_code, imap_test_v0_import_filename),
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_RESULT,
        "imap_a_result_id": "export: imap-%s" % country_test_code,
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": imap_test_v0_import_filename,
        "imap_b_period": period,
        "imap_b_country_org": country_test_org,
        "imap_b_country_name": country_test_name,
        "imap_b_country_code":  country_test_code,
        "assert_result_type": datim.datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
]

# Test batch definition
imap_test_batch = [
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
        "test_description": "Initial v0 import of '%s' into '%s'" % (
            imap_test_v0_import_filename, country_test_org),
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_IMPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "country_code": country_test_code,
        "country_name": country_test_name,
        "period": period,
        "imap_import_filename": imap_test_v0_import_filename,
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
        "test_description": "v1 import of '%s' into '%s'" % (
            imap_test_v1_import_filename, country_test_org),
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_IMPORT,
        "ocl_api_env": ocl_api_env,
        "ocl_api_token": ocl_api_token,
        "country_code": country_test_code,
        "country_name": country_test_name,
        "period": period,
        "imap_import_filename": imap_test_v1_import_filename,
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
        "test_id": "compare:%s-latest-after-v0 || %s" % (country_test_code, imap_test_v0_import_filename),
        "is_active": True,
        "test_description": "Confirm that imported '%s' v0 latest matches '%s'" % (
            country_test_code, imap_test_v0_import_filename),
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_RESULT,
        "imap_a_result_id": "export:%s-latest-after-v0" % country_test_code,
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": imap_test_v0_import_filename,
        "imap_b_period": period,
        "imap_b_country_org": country_test_org,
        "imap_b_country_name": country_test_name,
        "imap_b_country_code":  country_test_code,
        "assert_result_type": datim.datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    },
    {
        "test_id": "compare:%s-latest-after-v1 || %s" % (country_test_code, imap_test_v1_import_filename),
        "is_active": True,
        "test_description": "Confirm that imported '%s' v1 latest matches '%s'" % (
            country_test_code, imap_test_v1_import_filename),
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_RESULT,
        "imap_a_result_id": "export:%s-latest-after-v1" % country_test_code,
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": imap_test_v1_import_filename,
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

# Run the tests and display the results
datim.datimimaptests.DatimImapTests.display_test_summary(test_imap_diff)
imap_tester = datim.datimimaptests.DatimImapTests()
imap_tester.run_tests(test_imap_diff)
imap_tester.display_test_results()
