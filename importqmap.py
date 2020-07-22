"""
Script to import a QMAP into OCL. A QMAP is a JSON representation of a mapping between
a point of service system (POS) and a FHIR Questionnaire.

Example Usage:
    python importqmap.py -d="mAtches4" --env=staging -t="your-token-here" -v2
        --fhir-url="https://test.ohie.datim.org/hapi-fhir-jpaserver/fhir/"
        --qmap-api-root="https://testlohie.datimlorg:5000/plm-qmap/"
        qmap-questionnaires/HIV_example_map_with_constants.json

Example Output:
{
    "status": "Success",
    "message": "QMAP successfully queued for bulk import into OCL. Request QMAP export after bulk import is processed or request import status.",
    "fhir_server_url": "https://test.ohie.datim.org/hapi-fhir-jpaserver/fhir/",
    "qmap_export_url": "https://test.ohie.datim.org:5000/plm-qmap/mAtches4/9ffjifkd/",
    "qmap_import_status_url": "https://test.ohie.datim.org:5000/plm-qmap/:domain/qmaps?task=efijfkfijdkdifjeijfjekifjej",
    "ocl_bulk_import_status_url": "https://api.staging.openconceptlab.org/manage/bulkimport/?task=efijfkfijdkdifjeijfjekifjej",
    "ocl_bulk_import_task_id": "efijfkfijdkdifjeijfjekifjej"
}
"""
import json
import argparse
import urllib3
import common
import datim.qmap


# Suppress urllib error due to invalid SSL certificate
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Script argument parser
parser = argparse.ArgumentParser("qmap", description="Import a QMAP into OCL")
parser.add_argument('-d', '--domain', help='ID of the QMAP domain', required=True)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=common.ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('-t', '--token', help='OCL API token', required=True)
parser.add_argument(
    '--admin-token', help='OCL API administrative token required to delete existing repositories')
parser.add_argument('--testmode', action="store_true", help='Enable test mode', default=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + common.APP_VERSION)
parser.add_argument(
    '--qmap-api-root', help="API root for PLM QMAP mediators, eg https://test.ohie.datim.org:5000/")
parser.add_argument(
    '--fhir-url', required=True,
    help="URL for FHIR server API root, eg https://test.ohie.datim.org/hapi-fhir-jpaserver/fhir/")
parser.add_argument('file', type=argparse.FileType('r'), help='QMAP JSON document')
args = parser.parse_args()
with args.file as qmap_file:
    qmap_json = json.load(qmap_file)
    qmap = datim.qmap.Qmap(qmap_json=qmap_json)
ocl_env_url = args.env if args.env else args.envurl

# Display debug output
if args.verbosity > 1:
    import pprint
    print args
    pprint.pprint(json.loads(str(qmap)))

# Process the qmap import
output_json = None
try:
    bulk_import_task_id = qmap.import_qmap(
        domain=args.domain, ocl_env_url=ocl_env_url, ocl_api_token=args.token,
        test_mode=args.testmode, verbosity=args.verbosity, ocl_api_admin_token=args.admin_token,
        fhir_server_url=args.fhir_url)
except Exception as e:
    output_json = {
        "status": "Error",
        "message": str(e)
    }
else:
    if bulk_import_task_id:
        output_json = {
            "status": "Success",
            "message": ("QMAP successfully queued for bulk import into OCL. Request QMAP export "
                        "after bulk import is processed or request import status."),
            "ocl_bulk_import_task_id": bulk_import_task_id,
            "ocl_bulk_import_status_url": "%s/manage/bulkimport/?task=%s" % (
                ocl_env_url, bulk_import_task_id),
            "fhir_server_url": args.fhir_url,
        }
        if args.qmap_api_root:
            output_json["qmap_export_url"] = '%s%s/qmaps/%s/' % (
                args.qmap_api_root, args.domain, qmap.uid)
            output_json["qmap_import_status_url"] = '%s%s/qmaps/?importId=%s' % (
                args.qmap_api_root, args.domain, bulk_import_task_id)

if output_json:
    print json.dumps(output_json)
