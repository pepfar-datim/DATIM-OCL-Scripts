"""
Perform diff on an IMAP stored in OCL and one stored in a JSON or CSV file

EXAMPLE:
python imapdiff.py --env=staging -t=token -c=BI -pFY19 imap-samples/BI-FY19-baseline.csv
"""
import argparse
from . import datim.datimimap
from . import datim.datimimaptests
from . import common


# Script argument parser
parser = argparse.ArgumentParser("imap-diff", description="Diff 2 IMAPs")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=common.ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('-t', '--token', help='OCL API token', required=True)
parser.add_argument('-c', '--country_code', help='Country code, eg "BI" or "UG"', required=True)
parser.add_argument('-p', '--period', help='Period, eg "FY19" or "FY20"', required=True)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + common.APP_VERSION)
parser.add_argument(
    'file', type=argparse.FileType('r'), help='IMAP file (JSON or CSV), eg "BI-FY20.csv"')
args = parser.parse_args()

# Pre-process input parameters
ocl_env_url = args.env if args.env else args.envurl
imap_filename = args.file.name
country_org = 'DATIM-MOH-%s-%s' % (args.country_code, args.period)

# Test batch definition
imap_test_batch = [
    {
        "test_id": "imap-diff",
        "is_active": True,
        "test_description": "Compare IMAP in OCL with an IMAP file",
        "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
        "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_OCL,
        "imap_a_ocl_api_env": ocl_env_url,
        "imap_a_ocl_api_token": args.token,
        "imap_a_period": args.period,
        "imap_a_country_org": country_org,
        "imap_a_country_name": args.country_code,
        "imap_a_country_code": args.country_code,
        "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_FILE,
        "imap_b_filename": imap_filename,
        "imap_b_period": args.period,
        "imap_b_country_org": country_org,
        "imap_b_country_name": args.country_code,
        "imap_b_country_code": args.country_code,
        "assert_result_type": datim.datimimap.DatimImapDiff,
        "assert_num_diff": 0,
    }
]

# Run the tests and display the results
datim.datimimaptests.DatimImapTests.display_test_summary(imap_test_batch)
imap_tester = datim.datimimaptests.DatimImapTests()
imap_tester.run_tests(imap_test_batch)
imap_tester.display_test_results()
