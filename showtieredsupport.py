"""
Script to present DATIM Tiered Support metadata

Supported Formats: html, xml, csv, json
Supported Collections: datalements, options
OpenHIM Endpoint Request Format: /datim-tiered-support?collection=____&format=____
"""
import sys

import settings
from datim import datimshow, datimshowtieredsupport

# Default Script Settings
verbosity = 0  # 0=none, 1=some, 2=all
run_ocl_offline = False  # Set to true to use local copies of ocl exports
export_format = datimshow.DatimShow.DATIM_FORMAT_JSON
repo_id = 'options'

# OCL Settings - JetStream Staging user=datim-admin
oclenv = settings.oclenv
oclapitoken = settings.oclapitoken

# Optionally set arguments from the command line
if sys.argv and len(sys.argv) > 1:
    export_format = datimshow.DatimShow.get_format_from_string(sys.argv[1])
    repo_id = sys.argv[2]

# Create Show object and run
datim_show = datimshowtieredsupport.DatimShowTieredSupport(
    oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline, verbosity=verbosity)
datim_show.get(repo_id=repo_id, export_format=export_format)
