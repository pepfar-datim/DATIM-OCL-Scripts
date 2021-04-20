"""
Perform diff on an IMAP stored in OCL and one stored in a JSON or CSV file

EXAMPLE:
python imapdiffbackup.py --env=production-aws -t=2925899a86b7601de02b7b0f22cafda494ad2a5e -v1 imap-samples/production-v1-imap-backup-20210405.json
"""
import json
import argparse
import datim.datimimap
import datim.datimimaptests
import common


# Script argument parser
parser = argparse.ArgumentParser(
    "imap-diff-backup", description="Diff IMAP backup with OCL environment")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=common.ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('-t', '--token', help='OCL API token', required=True)
parser.add_argument(
    '-c', '--country_code', help='Filter backup by country code, eg "BI" or "UG"', default='')
parser.add_argument(
    '-p', '--period', help='Filter backup by period, eg "FY19" or "FY20"', default='')
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + common.APP_VERSION)
parser.add_argument(
    'file', type=argparse.FileType('r'), help='IMAP backup file #1')
args = parser.parse_args()

# Prepare filters
ocl_env_url = args.env if args.env else args.envurl
period_filter = ''
if args.period:
    period_filter = [x.strip() for x in args.period.split(',')]
country_code_filter = ''
if args.country_code:
    country_code_filter = [x.strip() for x in args.country_code.split(',')]

# Filter IMAPs
all_imap_backups = json.load(args.file)
imap_backups = []
for imap_backup in all_imap_backups:
    if period_filter and imap_backup['period'] not in period_filter:
        continue
    elif country_code_filter and imap_backup['country_code'] not in country_code_filter:
        continue
    imap_backups.append(imap_backup)

# Display debug info
if args.verbosity:
    print args
    print 'ocl_env_url=%s' % ocl_env_url
    print 'country_codes=%s' % country_code_filter
    print 'periods=%s' % period_filter
    print '%s IMAPs (after filter) of %s IMAP backups loaded' % (
        len(imap_backups), len(all_imap_backups))

# Loop through each and import
current_num = 0
for imap_backup in imap_backups:
    current_num += 1

    # Display debug info for the current IMAP org
    if args.verbosity:
        if args.verbosity > 1:
            print '\n\n' + '*' * 100
        print '** [IMAP %s of %s] Org: %s, Country Code: %s, Country Name: %s, Period: %s' % (
            current_num, len(imap_backups), imap_backup['country_org'],
            imap_backup['country_code'], imap_backup['country_name'], imap_backup['period'])
        if args.verbosity > 1:
            print '*' * 100

    # Load into IMAP object
    if imap_backup['status'] != 'Success':
        print 'WARNING: Invalid IMAP backup:', json.dumps(imap_backup)
        continue
    imap_input = datim.datimimap.DatimImap(
        imap_data=imap_backup['imap'],
        period=imap_backup['period'],
        country_org=imap_backup['country_org'],
        country_name=imap_backup['country_name'],
        country_code=imap_backup['country_code'])
    if args.verbosity > 1 and imap_input:
        imap_input.display(sort=True, exclude_empty_maps=True)
        # print 'INFO: IMAP import file "%s" loaded successfully' % args.file.name
    elif not imap_input:
        print 'ERROR: Unable to load IMAP import file "%s"' % args.file.name
        exit(1)

    # Run the diff
    imap_test_batch = [
        {
            "test_id": "imap-diff",
            "is_active": True,
            "test_description": "Compare IMAP from backup to an IMAP in OCL",
            "test_type": datim.datimimaptests.DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE,
            "imap_a_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_VARIABLE,
            "imap_a_object": imap_input,
            "imap_b_type": datim.datimimaptests.DatimImapTests.IMAP_COMPARE_TYPE_OCL,
            "imap_b_ocl_api_env": ocl_env_url,
            "imap_b_ocl_api_token": args.token,
            "imap_b_period": imap_backup['period'],
            "imap_b_country_org": imap_backup['country_org'],
            "imap_b_country_name": imap_backup['country_name'],
            "imap_b_country_code": imap_backup['country_code'],
            "assert_result_type": datim.datimimap.DatimImapDiff,
            "assert_num_diff": 0,
        }
    ]

    # Run the diff and display the results
    datim.datimimaptests.DatimImapTests.display_test_summary(imap_test_batch)
    imap_tester = datim.datimimaptests.DatimImapTests()
    imap_tester.run_tests(imap_test_batch)
    imap_tester.display_test_results()
