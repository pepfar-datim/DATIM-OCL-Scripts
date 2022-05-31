import settings
from datim import datimimap, datimimaptests

# Settings
period = 'FY19'
ocl_api_env = settings.ocl_api_url_production
ocl_api_token = settings.api_token_production_datim_admin
ocl_api_root_token = settings.api_token_production_root
imap_compare_csv_filename = 'csv/HT-FY19-v2.csv'
imap_compare_country_code = 'TEST-HT2'
imap_compare_country_name = 'TEST-HT2'
imap_compare_country_org = 'DATIM-MOH-%s-%s' % (imap_compare_country_code, period)

# Test batch definition
imap_test_batch = [
    {
        "test_id": "imap-compare-csv2ocl",
        "is_active": True,
        "test_description": "Compare IMAP in OCL with an IMAP CSV",
        "test_type": datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_OCL,
        "imap_a_ocl_api_env": settings.ocl_api_url_production,
        "imap_a_ocl_api_token": settings.api_token_production_datim_admin,
        "imap_a_period": period,
        "imap_a_country_org": imap_compare_country_org,
        "imap_a_country_name": imap_compare_country_name,
        "imap_a_country_code": imap_compare_country_code,
        "imap_b_type": datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": imap_compare_csv_filename,
        "imap_b_period": period,
        "imap_b_country_org": imap_compare_country_org,
        "imap_b_country_name": imap_compare_country_name,
        "imap_b_country_code": imap_compare_country_code,
        "assert_result_type": datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    }
]

# Run the tests and display the results
datimimaptests.DatimImapTests.display_test_summary(imap_test_batch)
imap_tester = datimimaptests.DatimImapTests()
imap_tester.run_tests(imap_test_batch)
imap_tester.display_test_results()
