"""
Script to present DATIM Mechanisms metadata

Request Format: /datim-mechanisms?format=____
Supported Formats: html, xml, csv, json
"""
from __future__ import with_statement
import datimshow
import datimconstants


class DatimShowMechanisms(datimshow.DatimShow):
    """ Class to manage DATIM Mechanisms Presentation """

    # OCL Export Definitions
    OCL_EXPORT_DEFS = datimconstants.DatimConstants.MECHANISMS_OCL_EXPORT_DEFS

    # Default endpoint to use if unspecified OCL export
    DEFAULT_REPO_LIST_ENDPOINT = '/orgs/PEPFAR/collections/'

    # Output headers
    headers = {
        'mechanisms': [
            {"name": "mechanism", "column": "mechanism", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "code", "column": "code", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "uid", "column": "uid", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "partner", "column": "partner", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "primeid", "column": "primeid", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "agency", "column": "agency", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "ou", "column": "ou", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "startdate", "column": "startdate", "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "enddate", "column": "enddate", "type": "java.lang.String", "hidden": False, "meta": False}
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

    def build_mechanism_row(self, c, headers=None, direct_mappings=None, repo_title='', repo_subtitle=''):
        if c['concept_class'] == 'Funding Mechanism':
            return {
                'mechanism': c['names'][0]['name'],
                'code': c['id'],
                'uid': c['external_id'],
                'partner': c['extras']['Partner'],
                'primeid': c['extras']['Prime Id'],
                'agency': c['extras']['Agency'],
                'ou': c['extras']['Organizational Unit'],
                'startdate': c['extras']['Start Date'],
                'enddate': c['extras']['End Date'],
            }
        return None
