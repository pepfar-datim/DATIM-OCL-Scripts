"""
Script to present DATIM Mechanisms metadata

Supported Formats: html, xml, csv, json
OpenHIM Endpoint Request Format: /datim-mechanisms?format=____
"""
import sys

import settings
from datim import datimshow, datimshowmechanisms

# Default Script Settings
verbosity = 0  # 0=none, 1=some, 2=all
run_ocl_offline = False  # Set to true to use local copies of dhis2/ocl exports
export_format = datimshow.DatimShow.DATIM_FORMAT_JSON
repo_id = 'Mechanisms'  # This one is hard-coded

# JetStream Staging user=datim-admin
oclenv = settings.oclenv
oclapitoken = settings.oclapitoken

# Optionally set arguments from the command line
if sys.argv and len(sys.argv) > 1:
    export_format = datimshow.DatimShow.get_format_from_string(sys.argv[1])

# Create Show object and run
datim_show = datimshowmechanisms.DatimShowMechanisms(
    oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline, verbosity=verbosity)
datim_show.get(export_format=export_format, repo_id=repo_id)
