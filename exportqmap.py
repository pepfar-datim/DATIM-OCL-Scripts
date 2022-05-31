"""
Script to export a QMAP from OCL. A QMAP is a JSON representation of a mapping between
a point of service system (POS) and a FHIR Questionnaire.

Example Usage:
    python exportqmap.py -d="mAtches3" --qmapid=qmapid --env=staging

Example mediator request:
    https://test.ohie.datim.org:5000/plm-qmap/mAtches3f/qmaps/4CmFkf/

Example Output:
{
    "uid": "4CmFkf",
    "name": "HIV_example_map",
    "questionnaireuid": "http://datim.org/fhir/Questionnaire/PLM-HIV-Questionnaire",
    "complete": true,
    "created": "2020-04-27T12:57:37.889",
    "updated": "2020-04-27T12:57:37.889",
    "map": {
        "headers": {
            "DateObserved": {
                "path": [
                    {"linkid": "/Observation", "text": "Practitioner"},
                    {"linkid": "/Observation/date", "text": "Observation Date"}
                ],
                "valueType": "dateTime"
            },
            "DOB": {
                "path": [
                    {"linkid": "/Patient", "text": "Patient ID"},
                    {"linkid": "/Patient/birthDate", "text": "Birth Date"}
                ],
                "valueType": "date"
            },
            "VLC": {
                "path": [
                    {"linkid": "/Observation", "text": "Practitioner"},
                    {"linkid": "/Observation/viralLoad", "text": "Viral Load Count"}
                ],
                "valueType": "integer"
            },
            "PractitionerCode": {
                "path": [
                    {"linkid": "/Observation", "text": "Practitioner"},
                    {"linkid": "/Observation/performer", "text": "Practitioner ID"}
                ],
                "valueType": "string"
            },
            "ART_Start": {
                "path": [
                    {"linkid": "/MedicationStatement", "text": "MedicationStatement"},
                    {"linkid": "/MedicationStatement/startDate", "text": "ART Start Date"}
                ],
                "valueType": "dateTime"
            },
            "LocationCode": {
                "path": [
                    {"linkid": "/Encounter", "text": "Encounter"},
                    {"linkid": "/Encounter/location", "text": "Location ID"}
                ],
                "valueType": "string"
            }
        },
        "constants": {
            "/Patient/gender": {
                "path": [
                    {"linkid": "/Patient", "text": "Patient ID"},
                    {"linkid": "/Patient/gender", "text": "Gender"}
                ],
                "valueType": "choice",
                "code": "female",
                "display": "Female"
            },
            "/Patient/id": {
                "path": [
                    {"linkid": "/Patient", "text": "Patient ID"},
                    {"linkid": "/Patient/id", "text": "Patient ID"}
                ],
                "valueType": "string",
                "code": "123",
                "display": "123"
            }
        }
    }
}
"""
import argparse
import json

import ocldev.oclexport

import common
from datim import qmap

# Script argument parser
parser = argparse.ArgumentParser("qmap", description="Export a QMAP into OCL")
parser.add_argument('-d', '--domain', help='ID of the QMAP domain', required=True)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--env', help='Name of the OCL API environment', type=common.ocl_environment)
group.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('--qmapid', help='QMAP ID', required=True)
parser.add_argument('-t', '--token', help='OCL API token', required=False)
# parser.add_argument('--testmode', action="store_true", help='Enable test mode', default=False)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
parser.add_argument('--version', action='version', version='%(prog)s v' + common.APP_VERSION)
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.env_url

# Display debug output
if args.verbosity > 1:
    print(args)

# Process the qmap export
try:
    qmap = qmap.Qmap.export_qmap(
        domain=args.domain, qmap_id=args.qmapid,
        ocl_env_url=ocl_env_url, ocl_api_token=args.token,
        verbosity=args.verbosity)
    # Export successful -- Return 200 status code and print (as JSON)
    print(qmap)
except ocldev.oclexport.OclExportNotAvailableError as export_error:
    # Export not yet ready -- Return 204 status code
    print(json.dumps({
        "status": "Not Available",
        "message": "QMAP export not available. Try requesting again later."
    }, indent=4))
except Exception as e:
    print(json.dumps({
        "status": "Error",
        "message": str(e)
    }, indent=4))
