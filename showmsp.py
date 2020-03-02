"""
Script to present MSP Metadata in the DATIM codelist format

OpenHIM Endpoint Request Format: /show-msp?collection=___&format=___&de=___
Supported Formats: html, xml, csv, json
Supported Collections: Refer to DatimConstants.MER_OCL_EXPORT_DEFS (there are more than 60 options)

This script fetches an export from OCL for the latest released version of the specified collection.
If it seems like you're looking at old data, check the collection version first.
"""
import sys
import settings
import datim.datimshow
import datim.datimshowmsp


# Default Script Settings
verbosity = 0  # 0=none, 1=some, 2=all
run_ocl_offline = False  # Set to true to use local copies of ocl exports
export_format = datim.datimshow.DatimShow.DATIM_FORMAT_CSV
# repo_id = 'MER-R-COMMUNITY-BASED'  # e.g. MER-R-Operating-Unit-Level-IM-FY17Q2
repo_id = ''
data_element_ids = [
    'MQYi9hco9WG',  # DATIM TB_PREV_N_DSD_Age_Therapy_NewExArt_HIV_TARGET
    'kqhj1NfxHvi',  # DATIM VMMC_CIRC_NAT_N_NAT_Age_Sex
    'P8B3VcejEjn',  # PDH HTS_TST_POS (N, DSD, VCT/AgeLessThanTen/Result)
    'DhtVap9UN63',  # PDH HTS_TST_POS (N, DSD, VMMC/Age Aggregated/Sex/Result)
    'kGCGnEJE7hy',  # PDH HTS_INDEX_FAC (N, TA) TARGET: Number of index cases tested
]

# OCL Settings - JetStream Staging user=datim-admin
oclenv = settings.oclenv
oclapitoken = settings.oclapitoken

# Optionally set arguments from the command line
if sys.argv and len(sys.argv) > 2:
    export_format = datim.datimshow.DatimShow.get_format_from_string(sys.argv[1])
    repo_id = sys.argv[2]
    data_element_ids = sys.argv[3]

# Create Show object and run
datim_show = datim.datimshowmsp.DatimShowMsp(
    oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline, verbosity=verbosity)
datim_show.get(
    data_element_ids=data_element_ids, repo_id=repo_id, export_format=export_format,
    owner='PEPFAR-Test6', source='MER-Test6')
