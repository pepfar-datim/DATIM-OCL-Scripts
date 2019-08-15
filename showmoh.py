"""
Script to present DATIM MOH metadata -
NOTE This only shows the global PEPFAR DATIM MOH Alignment indicators, without mapping to country indicators.
To view country mappings, use the IMAP export script and mediator.

Supported Formats: html, xml, csv, json
OpenHIM Mediator Request Format: /datim-moh?period=____&format=____

This script fetches an export from OCL for the latest released version of the specified collection.
If it seems like you're looking at old data, check the collection version first.
"""
import sys
import settings
import datim.datimshow
import datim.datimshowmoh


# Default Script Settings
verbosity = 0  # 0=none, 1=some, 2=all
run_ocl_offline = False  # Set to true to use local copies of ocl exports
export_format = datim.datimshow.DatimShow.DATIM_FORMAT_CSV
period = ''  # e.g. FY18, FY19

# OCL Settings - JetStream Staging user=datim-admin
oclenv = settings.oclenv
oclapitoken = settings.oclapitoken

# Optionally set arguments from the command line
if sys.argv and len(sys.argv) > 2:
    export_format = datim.datimshow.DatimShow.get_format_from_string(sys.argv[1])
    period = sys.argv[2]

# Create Show object and run
datim_show = datim.datimshowmoh.DatimShowMoh(
    oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline, verbosity=verbosity)
datim_show.get(period=period, export_format=export_format)
