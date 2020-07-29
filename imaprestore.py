"""
Script to restore a backup of IMAPs into an OCL environment

This command will import an IMAP backup into OCL staging:

    python imaprestore.py --env=staging -t=[my-token] imap_backup_filename.json

"""
import json
import argparse
import datim.datimimap
import datim.datimimapimport
import common


# Script argument parser
parser = argparse.ArgumentParser(
    "imap-restore", description="Restore IMAP backup file to OCL environment")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    '--env', help='Name of the OCL API environment: production, staging, demo, qa',
    type=common.ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('-t', '--token', help='OCL API token', required=True)
parser.add_argument('--test_mode', action="store_true", help='Enable test mode', default=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--public_access', help="Level of public access: View, None", default='View')
parser.add_argument('--version', action='version', version='%(prog)s v' + common.APP_VERSION)
parser.add_argument(
    'file', type=argparse.FileType('r'), help='IMAP file (JSON or CSV), eg "BI-FY20.csv"')
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.envurl
imap_backups = json.load(args.file)
if args.verbosity > 1:
    print args
    print 'ocl_env_url=%s' % ocl_env_url
    print '%s IMAP backups loaded' % len(imap_backups)

# Loop through each and import
for imap_backup in imap_backups:
    # Display debug info for the current IMAP org
    if args.verbosity:
        print '\n\n' + '*' * 100
        print '** [EXPORT] Org: %s, Country Code: %s, Country Name: %s, Period: %s' % (
            imap_backup['country_org'], imap_backup['country_code'], imap_backup['country_name'],
            imap_backup['period'])
        print '*' * 100

    # Load IMAP
    if imap_backup['status'] != 'Success':
        print 'WARNING: Invalid IMAP backup:', json.dumps(imap_backup)
        continue
    imap_input = datim.datimimap.DatimImap(
        imap_data=imap_backup['imap'],
        period=imap_backup['period'],
        country_org=imap_backup['country_org'],
        country_name=imap_backup['country_name'],
        country_code=imap_backup['country_code'])
    if args.verbosity and imap_input:
        imap_input.display(sort=True, exclude_empty_maps=True)
        # print 'INFO: IMAP import file "%s" loaded successfully' % args.file.name
    elif not imap_input:
        print 'ERROR: Unable to load IMAP import file "%s"' % args.file.name
        exit(1)

    # Import IMAP
    output_json = {
        "country_org": imap_backup['country_org'],
        "country_code": imap_backup['country_code'],
        "country_name": imap_backup['country_name'],
        "period": imap_backup['period']
    }
    try:
        imap_import = datim.datimimapimport.DatimImapImport(
            oclenv=ocl_env_url, oclapitoken=args.token, verbosity=args.verbosity,
            run_ocl_offline=False, test_mode=args.test_mode,
            country_public_access=args.public_access)
        bulk_import_task_id = imap_import.import_imap(imap_input=imap_input)
    except Exception as err:
        output_json["status"] = "Error"
        output_json["message"] = str(err)
    else:
        if args.test_mode:
            output_json["status"] = "Test"
        elif bulk_import_task_id:
            output_json["status"] = "Success"
            output_json["message"] = ("IMAP successfully queued for bulk import into OCL. "
                                      "Request IMAP export after bulk import is processed "
                                      "or request import status.")
            output_json["ocl_bulk_import_task_id"] = bulk_import_task_id
            output_json["ocl_bulk_import_status_url"] = "%s/manage/bulkimport?task=%s" % (
                ocl_env_url, bulk_import_task_id)
    if output_json:
        print json.dumps(output_json)
