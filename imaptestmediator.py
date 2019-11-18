import settings
import datim.datimimap
import datim.datimimaptests
import datim.datimimapimport

# Settings
ocl_api_env = settings.ocl_api_url_staging
ocl_api_token = settings.api_token_staging_datim_admin
ocl_api_root_token = settings.api_token_staging_root
imap_mediator_env = settings.imap_mediator_url_test
# imap_mediator_env = settings.imap_mediator_url_production
period = 'FY19'
imap_test_v0_import_filename = 'csv/ZM-FY19-v0.csv'
imap_test_v1_import_filename = 'csv/ZM-FY19-v1.csv'
country_test_code = 'ZM-TEST'
country_test_name = country_test_code
country_test_org = 'DATIM-MOH-%s-%s' % (country_test_code, period)
do_drop_when_complete = True

# Test batch definition
imap_test_batch = [
    {
        "test_id": "mediator-export:%s" % country_test_code,
        "is_active": True,
        "test_description": "Export '%s'" % country_test_code,
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_MEDIATOR_EXPORT,
        "imap_mediator_env": imap_mediator_env,
        "export_format": datim.datimimap.DatimImap.DATIM_IMAP_FORMAT_JSON,
        "country_code": country_test_code,
        "country_name": country_test_name,
        "period": period,
        "assert_result_type": datim.datimimap.DatimImap,
        "assert_http_response_code": 200,
    }
]

# Run the tests and display the results
datim.datimimaptests.DatimImapTests.display_test_summary(imap_test_batch)
imap_tester = datim.datimimaptests.DatimImapTests()
imap_tester.run_tests(imap_test_batch)
imap_tester.display_test_results()
