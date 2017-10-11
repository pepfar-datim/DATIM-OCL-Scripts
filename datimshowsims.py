"""
Script to present DATIM SIMS metadata

Request Format: /datim-sims?collection=____&format=____
Supported Formats: html, xml, csv, json
Supported Collections:
    sims3_facility, sims3_community, sims3_above_site
    sims2_facility, sims2_community, sims2_above_site
    sims_option_sets
"""
from __future__ import with_statement
import sys
from datimshow import DatimShow
from datimconstants import DatimConstants


class DatimShowSims(DatimShow):
    """ Class to manage DATIM SIMS Presentation """

    # OCL Export Definitions
    OCL_EXPORT_DEFS = DatimConstants.SIMS_OCL_EXPORT_DEFS

    # Default endpoint to use if unspecified OCL export
    DEFAULT_REPO_LIST_ENDPOINT = '/orgs/PEPFAR/collections/'

    # Output headers
    headers = {
        'sims': [
            {"name": "name", "column": "name", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "code", "column": "code", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "uid", "column": "uid", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "valuetype", "column": "valuetype", "type": "java.lang.String", "hidden": False, "meta": False},
        ],
        'option_sets': [
            {"name": "option_set", "column": "option_set", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "option_description", "column": "option_description", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "option_code", "column": "option_code", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "option_code_description", "column": "option_code_description", "type": "java.lang.String", "hidden": False, "meta": False}
        ]
    }

    def __init__(self, oclenv='', oclapitoken='', run_ocl_offline=False, verbosity=0, cache_intermediate=True):
        DatimShow.__init__(self)
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.run_ocl_offline = run_ocl_offline
        self.verbosity = verbosity
        self.cache_intermediate = cache_intermediate
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    def build_sims_row(self, concept, headers=None, direct_mappings=None, repo_title='', repo_subtitle=''):
        return {
            'name': concept['names'][0]['name'],
            'code': concept['id'],
            'uid': concept['external_id'],
            'valuetype': concept['extras']['Value Type'],
        }

    def build_sims_option_sets_row(self, concept, headers=None, direct_mappings=None, repo_title='', repo_subtitle=''):
        return {
            'option_set': concept['extras']['Option Set Name'],
            'option_description': concept['extras']['Option Set Name'],
            'option_code': concept['extras']['Option Code'],
            'option_code_description': concept['names'][0]['name'],
        }

# Default Script Settings
verbosity = 0  # 0=none, 1=some, 2=all
run_ocl_offline = False  # Set to true to use local copies of ocl exports
export_format = DatimShow.DATIM_FORMAT_HTML
repo_id = 'SIMS3-Above-Site'

# OCL Settings - JetStream Staging user=datim-admin
oclenv = 'https://api.staging.openconceptlab.org'
oclapitoken = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'

# Set arguments from the command line
if sys.argv and len(sys.argv) > 1:
    export_format = DatimShow.get_format_from_string(sys.argv[1])
    repo_id = sys.argv[2]

# Create Show object and run
datim_show = DatimShowSims(
    oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline, verbosity=verbosity)
datim_show.get(repo_id=repo_id, export_format=export_format)
