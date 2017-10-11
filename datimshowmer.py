"""
Script to present DATIM MER metadata

Request Format: /datim-mer?collection=____&format=____
Supported Formats: html, xml, csv, json
Supported Collections: Refer to DatimConstants.MER_OCL_EXPORT_DEFS (there are more than 60 options)
"""
from __future__ import with_statement
import sys
from datimshow import DatimShow
from datimconstants import DatimConstants


class DatimShowMer(DatimShow):
    """ Class to manage DATIM MER Presentation """

    # OCL Export Definitions
    OCL_EXPORT_DEFS = DatimConstants.MER_OCL_EXPORT_DEFS

    # Default endpoint to use if unspecified OCL export
    DEFAULT_REPO_LIST_ENDPOINT = '/orgs/PEPFAR/collections/'

    # Output headers
    headers = {
        'mer': [
            {"name": "dataset", "column": "dataset", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "dataelement", "column": "dataelement", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "shortname", "column": "shortname", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "code", "column": "code", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "dataelementuid", "column": "dataelementuid", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "dataelementdesc", "column": "dataelementdesc","type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "categoryoptioncombo", "column": "categoryoptioncombo", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "categoryoptioncombocode", "column": "categoryoptioncombocode", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "categoryoptioncombouid", "column": "categoryoptioncombouid", "type": "java.lang.String", "hidden": False, "meta": False}
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

    def build_mer_indicator_output(self, c, headers=None, direct_mappings=None, repo_title='', repo_subtitle=''):
        if c['concept_class'] != 'Indicator':
            return None

        # Build the indicator
        concept_description = ''
        if c['descriptions']:
            concept_description = c['descriptions'][0]['description']
        output_concept = {
            'dataset': repo_title,
            'dataelement': c['names'][0]['name'],
            'shortname': c['names'][1]['name'],
            'code': c['id'],
            'dataelementuid': c['external_id'],
            'dataelementdesc': concept_description,
            'categoryoptioncombo': '',
            'categoryoptioncombocode': '',
            'categoryoptioncombouid': '',
        }

        # Find all the relevant mappings
        if direct_mappings:
            output_rows = []
            for m in direct_mappings:
                output_concept['categoryoptioncombo'] = m['to_concept_name']
                output_concept['categoryoptioncombocode'] = m['to_concept_code']
                output_concept['categoryoptioncombouid'] = m['to_concept_code']
                output_rows.append(output_concept.copy())
            return output_rows
        else:
            return output_concept


# Default Script Settings
verbosity = 0  # 0=none, 1=some, 2=all
run_ocl_offline = False  # Set to true to use local copies of ocl exports
export_format = DatimShow.DATIM_FORMAT_HTML
repo_id = 'MER-R-Operating-Unit-Level-IM-FY17Q2'

# OCL Settings - JetStream Staging user=datim-admin
oclenv = 'https://api.staging.openconceptlab.org'
oclapitoken = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'

# Set arguments from the command line
if sys.argv and len(sys.argv) > 2:
    export_format = DatimShow.get_format_from_string(sys.argv[1])
    repo_id = sys.argv[2]

# Create Show object and run
datim_show = DatimShowMer(
    oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline, verbosity=verbosity)
datim_show.get(repo_id=repo_id, export_format=export_format)
