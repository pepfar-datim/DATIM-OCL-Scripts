"""
Script to present MSP Metadata in the DATIM codelist format

OpenHIM Endpoint Request Format: /show-msp?collection=___&format=___&de=___
Supported Formats: html, xml, csv, json
Supported Collections: Refer to DatimConstants.MER_OCL_EXPORT_DEFS (there are more than 60 options)

This script fetches an export from OCL for the latest released version of the specified collection.
If it seems like you're looking at old data, check the collection version first.

Example value for repo: MER-R-COMMUNITY-BASED
Example data element IDs: [
    'MQYi9hco9WG',  # DATIM TB_PREV_N_DSD_Age_Therapy_NewExArt_HIV_TARGET
    'kqhj1NfxHvi',  # DATIM VMMC_CIRC_NAT_N_NAT_Age_Sex
    'P8B3VcejEjn',  # PDH HTS_TST_POS (N, DSD, VCT/AgeLessThanTen/Result)
    'DhtVap9UN63',  # PDH HTS_TST_POS (N, DSD, VMMC/Age Aggregated/Sex/Result)
    'kGCGnEJE7hy',  # PDH HTS_INDEX_FAC (N, TA) TARGET: Number of index cases tested
]

Example Usage:
- Export "MER-R-COMMUNITY-BASED" Codelist as HTML
    python showmsp.py --env=staging --repo="MER-R-COMMUNITY-BASED" --msp-org=PEPFAR-Test6
        --msp-source=MER-Test6 -t="your-token-here" -fHTML
- Export data elements list as CSV
    python showmsp.py --env=staging --dataelements="ljoZhkWOvx1,cObJTp3DWdY"
        --msp-org=PEPFAR-Test6 --msp-source=MER-Test6 -t="your-token-here" -fcsv

"""
import argparse
import datim.datimshow
import datim.datimshowmsp


# Script constants
SCRIPT_VERSION = '0.1.0'
OCL_ENVIRONMENTS = {
    'qa': 'https://api.qa.openconceptlab.org',
    'staging': 'https://api.staging.openconceptlab.org',
    'production': 'https://api.openconceptlab.org',
    'demo': 'https://api.demo.openconceptlab.org',
}


# Argument parser validation functions
def arg_parser_ocl_environment(string):
    """ Arg parser function for processing OCL enviroment setting """
    if string not in OCL_ENVIRONMENTS:
        raise argparse.ArgumentTypeError(
            'Argument "env" must be %s' % ', '.join(OCL_ENVIRONMENTS.keys()))
    return OCL_ENVIRONMENTS[string]


def arg_parser_data_element_ids(string):
    """ Arg parser function for processing data element IDs """
    dirty_data_element_ids = string.split(',')
    output_data_element_ids = []
    for dirty_data_element_id in dirty_data_element_ids:
        output_data_element_ids.append(dirty_data_element_id.strip())
    return output_data_element_ids


def arg_parser_export_format(format_string):
    """ Arg parser function for determining export format """
    return datim.datimshow.DatimShow.get_format_from_string(format_string, default_fmt='csv')


# Script argument parser
parser = argparse.ArgumentParser(
    "MSP Data Element Exporter",
    description="Export MSP Data Elements from OCL")
group_env = parser.add_mutually_exclusive_group(required=True)
group_env.add_argument('--env', help='Name of the OCL API environment',
                       type=arg_parser_ocl_environment)
group_env.add_argument('--envurl', help='URL of the OCL API environment')
parser.add_argument('--msp-org', help='MSP Organization ID in OCL (default=PEPFAR)', default='PEPFAR')
parser.add_argument('--msp-source', help='MSP Organization ID in OCL (default=MER)', default='MER')
parser.add_argument('-t', '--token', help='OCL API token')
parser.add_argument('-f', '--format', help='Export format: CSV, HTML, JSON, XML', default="CSV",
                    type=arg_parser_export_format)
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
group_data_elements = parser.add_mutually_exclusive_group(required=True)
group_data_elements.add_argument('--dataelements', help='IDs of the data elements to export',
                                 type=arg_parser_data_element_ids)
group_data_elements.add_argument('--repo', help='ID of the repository to export')
args = parser.parse_args()
ocl_env_url = args.env if args.env else args.envurl

# Display debug output
if args.verbosity > 1:
    print args, ocl_env_url

# Create DatimMspShow object and run
datim_show = datim.datimshowmsp.DatimShowMsp(
    oclenv=ocl_env_url, oclapitoken=args.token,
    run_ocl_offline=False, verbosity=args.verbosity)
datim_show.get(
    data_element_ids=args.dataelements, repo_id=args.repo, export_format=args.format,
    owner=args.msp_org, source=args.msp_source)
