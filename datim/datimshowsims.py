"""
Class to present DATIM SIMS metadata based on the DatimShow class

Supported Formats: html, xml, csv, json
Supported Collections:
    sims3_facility, sims3_community, sims3_above_site
    sims2_facility, sims2_community, sims2_above_site
    sims_option_sets
"""

from . import datimconstants, datimshow


class DatimShowSims(datimshow.DatimShow):
    """ Class to manage DATIM SIMS Presentation """

    # OCL Export Definitions
    OCL_EXPORT_DEFS = datimconstants.DatimConstants.SIMS_OCL_EXPORT_DEFS

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
