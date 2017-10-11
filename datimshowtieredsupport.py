"""
Script to present DATIM Tiered Support metadata

Request Format: /datim-tiered-support?collection=____&format=____
Supported Formats: html, xml, csv, json
Supported Collections: datalements, options
"""
from __future__ import with_statement
import sys
from datimshow import DatimShow
from datimconstants import DatimConstants


class DatimShowTieredSupport(DatimShow):
    """ Class to manage DATIM Tiered Support Presentation """

    # OCL Export Definitions
    OCL_EXPORT_DEFS = DatimConstants.TIERED_SUPPORT_OCL_EXPORT_DEFS

    # Default endpoint to use if unspecified OCL export
    DEFAULT_REPO_LIST_ENDPOINT = '/orgs/PEPFAR/collections/'

    # Output headers
    headers = {
        'tiered_support_data_elements': [
            {"name": "name", "column": "name", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "code", "column": "code", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "uid", "column": "uid", "type": "java.lang.String", "hidden": False, "meta": False}
        ],
        'tiered_support_options': [
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

    def build_tiered_support_data_element_row(self, c, headers=None, direct_mappings=None, repo_title='', repo_subtitle=''):
        if c['concept_class'] == 'Misc':
            return {
                'name': c['names'][0]['name'],
                'code': c['id'],
                'uid': c['external_id'],
            }
        return None

    def build_tiered_support_option_row(self, c, headers=None, direct_mappings=None, repo_title='', repo_subtitle=''):
        if c['concept_class'] == 'Option':
            return {
                'option_set': c['extras']['option_set'],
                'option_description': c['extras']['option_set'],
                'option_code': c['extras']['option_code'],
                'option_code_description': c['names'][0]['name'],
            }
        return None


# Default Script Settings
verbosity = 0  # 0=none, 1=some, 2=all
run_ocl_offline = False  # Set to true to use local copies of ocl exports
export_format = DatimShow.DATIM_FORMAT_HTML
repo_id = 'options'

# OCL Settings - JetStream Staging user=datim-admin
oclenv = 'https://api.staging.openconceptlab.org'
oclapitoken = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'

# Set arguments from the command line
if sys.argv and len(sys.argv) > 1:
    export_format = DatimShow.get_format_from_string(sys.argv[1])
    repo_id = sys.argv[2]

# Create Show object and run
datim_show = DatimShowTieredSupport(
    oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline, verbosity=verbosity)
datim_show.get(repo_id=repo_id, export_format=export_format)
