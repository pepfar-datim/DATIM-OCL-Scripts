"""
Script to present DATIM MER metadata

Request Format: /datim-mer?collection=____&format=____
Supported Formats: html, xml, csv, json
Supported Collections: Refer to DatimConstants.MER_OCL_EXPORT_DEFS (there are more than 60 options)
"""

from . import datimshow
from . import datimconstants


class DatimShowMer(datimshow.DatimShow):
    """ Class to manage DATIM MER Presentation """

    # OCL Export Definitions
    OCL_EXPORT_DEFS = datimconstants.DatimConstants.MER_OCL_EXPORT_DEFS

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
        datimshow.DatimShow.__init__(self)
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
