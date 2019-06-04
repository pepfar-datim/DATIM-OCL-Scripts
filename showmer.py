"""
Script to present DATIM MER metadata

Supported Formats: html, xml, csv, json
Supported Collections: Refer to DatimConstants.MER_OCL_EXPORT_DEFS (there are more than 60 options)
OpenHIM Endpoint Request Format: /datim-mer?collection=____&format=____

This script fetches an export from OCL for the latest released version of the specified collection.
If it seems like you're looking at old data, check the collection version first.
"""
import sys
import settings
import datim.datimshow
import datim.datimshowmer


# Default Script Settings
verbosity = 0  # 0=none, 1=some, 2=all
run_ocl_offline = False  # Set to true to use local copies of ocl exports
export_format = datim.datimshow.DatimShow.DATIM_FORMAT_CSV
repo_id = 'MER-R-MOH-Facility-FY18'  # e.g. MER-R-Operating-Unit-Level-IM-FY17Q2

# OCL Settings - JetStream Staging user=datim-admin
oclenv = settings.ocl_api_url_staging
oclapitoken = settings.api_token_staging_datim_admin

# Optionally set arguments from the command line
if sys.argv and len(sys.argv) > 2:
    export_format = datim.datimshow.DatimShow.get_format_from_string(sys.argv[1])
    repo_id = sys.argv[2]

# Create Show object and run
datim_show = datim.datimshowmer.DatimShowMer(
    oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline, verbosity=verbosity)
datim_show.get(repo_id=repo_id, export_format=export_format)
